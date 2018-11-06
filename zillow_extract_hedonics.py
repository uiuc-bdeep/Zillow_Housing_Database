import psycopg2
import sys
import os

## Run in format: python3 zillow_extract_hedonics.py ST_num (delete)

# Declare gloabal variables, see zillow_txt_to_database.py for details
path = '/home/schadri/share/projects/Zillow_Housing/stores/Zillow_2017_Nov/'
dbname = 'zillow_2017_nov'
st_num = sys.argv[1]
completionfilename = './store_records_new/Data_Stored_%s.txt' % st_num
zasmschema = 'newzasmt' + st_num
ztransschema = 'newztrans' + st_num
# delete option, indicating whether to delete the zasmt & ztrans schemas
delete = False

# Check input format
if len(sys.argv) == 1 or len(sys.argv) > 3:
	print("Please enter a state number.")
	sys.exit(-1)

# Check state number exists
if not os.path.exists(path + st_num):
	print("Please enter a valid state number (i.e. two digits).")
	sys.exit(-2)

# Check delete option
if len(sys.argv) == 3 and sys.argv[2] == 'delete':
	delete = True

# Connect to database
conn_string = "host='localhost' dbname='{}' user='postgres' password='bdeep'".format(dbname)
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()
print("Connected to database: host = localhost dbname = {} user = postgres".format(dbname))

# Start constructing hedonics
print("Start Constructing hedonics...")
# ZAsmt hedonics
cursor.execute(""" SELECT RowID, ImportParcelID, LoadID,
                          FIPS, State, County,
                          PropertyFullStreetAddress,
                          PropertyHouseNumber, PropertyHouseNumberExt,
						  PropertyStreetPreDirectional, PropertyStreetName,
						  PropertyStreetSuffix, PropertyStreetPostDirectional,
                          PropertyCity, PropertyState, PropertyZip,
                          PropertyBuildingNumber, PropertyAddressUnitDesignator,
						  PropertyAddressUnitNumber, PropertyAddressLatitude,
						  PropertyAddressLongitude,
						  PropertyAddressCensusTractAndBlock,
                          NoOfBuildings,
                          LotSizeAcres, LotSizeSquareFeet,
                          TaxAmount, TaxYear
  				  INTO %s.BASE FROM %s.utmain
			   """ % (zasmschema, zasmschema))

# clear duplicates, keep the only importparcelid with the largest loadid
cursor.execute(""" WITH DATA AS (
						SELECT ImportParcelID, MAX(LoadID) AS LoadID, COUNT(*)
						FROM %s.BASE
						GROUP BY ImportParcelID
						HAVING COUNT(*) > 1
					)
				   DELETE FROM %s.BASE AS BA
				   USING DATA
				   WHERE DATA.ImportParcelID = BA.ImportParcelID
				   AND DATA.LoadID > BA.LoadID
			   """ % (zasmschema, zasmschema))

cursor.execute(""" SELECT RowID, NoOfUnits, BuildingOrImprovementNumber,
                       	  YearBuilt, EffectiveYearBuilt, YearRemodeled,
                       	  NoOfStories, StoryTypeStndCode, TotalRooms,
						  TotalBedrooms, FullBath, ThreeQuarterBath, HalfBath,
						  QuarterBath, HeatingTypeorSystemStndCode,
                          PropertyLandUseStndCode, WaterStndCode
					INTO %s.BLDG
					FROM %s.utbuilding
					WHERE PropertyLandUseStndCode
					IN (	'RR101',  /* SFR */
                        	'RR999',  /* Inferred SFR */
                        	'RR104',  /* Townhouse */
                        	'RR105',  /* Cluster Home */
	                        'RR106',  /* Condominium */
	                        'RR107',  /* Cooperative */
	                        'RR108',  /* Row House */
	                        'RR109',  /* Planned Unit Development */
	                        'RR113',  /* Bungalow */
	                        'RR116',  /* Patio Home */
	                        'RR119',  /* Garden Home */
	                        'RR120')
			   """ % (zasmschema, zasmschema))

# collect zasmt hedonics
cursor.execute(""" WITH ATTR AS (
						SELECT *
					    FROM %s.BASE
						INNER JOIN %s.BLDG
					    USING (RowID)
					),
					SQFT AS (
						SELECT RowID,
							   BuildingOrImprovementNumber,
							   MAX(BuildingAreaSqFt) AS SqFeet
						FROM %s.utbuildingareas
						WHERE BuildingAreaStndCode
						IN( 'BAL',  /* Building Area Living */
							'BAF',  /* Building Area Finished */
							'BAE',  /* Effective Building Area */
							'BAG',  /* Gross Building Area */
							'BAJ',  /* Building Area Adjusted */
							'BAT',  /* Building Area Total */
							'BLF')
						GROUP BY RowID, BuildingOrImprovementNumber
					)
					SELECT ATTR.*,
						   SQFT.SqFeet
					INTO %s.HEDONICS
					FROM ATTR
					LEFT JOIN SQFT
					ON ATTR.RowID = SQFT.RowID
					AND ATTR.BuildingOrImprovementNumber = SQFT.BuildingOrImprovementNumber
			   """ % (zasmschema, zasmschema, zasmschema, zasmschema))
conn.commit()
print("ZAsmt hedonics finished")

# ZTrans hedonics
cursor.execute(""" SELECT *
					INTO %s.PROPTRANS
					FROM %s.utpropertyinfo
			   """ % (ztransschema, ztransschema))
# delete duplicates
cursor.execute(""" WITH DATA AS (
						  SELECT PropertySequenceNumber,
						  		 TransId,
						  		 MAX(LoadID) AS LoadID,
								 Count(*)
						  FROM %s.PROPTRANS
						  GROUP BY PropertySequenceNumber, TransId
						  HAVING COUNT(*) > 1
					 )
					DELETE FROM %s.PROPTRANS AS PROP
					USING DATA
					WHERE DATA.TransId = PROP.TransId
					AND DATA.PropertySequenceNumber = PROP.PropertySequenceNumber
					AND DATA.LoadID > PROP.LoadID;
			   """ % (ztransschema, ztransschema))
cursor.execute(""" SELECT TransId, LoadID,
                          RecordingDate, DocumentDate,
                          SignatureDate, EffectiveDate,
                          SalesPriceAmount, LoanAmount,
                          SalesPriceAmountStndCode, LoanAmountStndCode,
                          DataClassStndCode, DocumentTypeStndCode,
                          PartialInterestTransferStndCode,
                          IntraFamilyTransferFlag,
                          TransferTaxExemptFlag, PropertyUseStndCode,
                          AssessmentLandUseStndCode, OccupancyStatusStndCode
					INTO %s.TRANS
					FROM %s.utmain
					WHERE DataClassStndCode IN ('D', 'H', 'F', 'M')
		   	   """ % (ztransschema, ztransschema))
# delete duplicates
cursor.execute(""" WITH DUP AS (
						SELECT TransId, MAX(LoadID) AS LoadID, COUNT(*)
						FROM %s.TRANS
						GROUP BY TransId
						HAVING COUNT(*) > 1
					)
					DELETE FROM %s.TRANS as TR
					USING DUP
					WHERE DUP.TransId = TR.TransId
					AND DUP.LoadID > TR.LoadID;
			   """ % (ztransschema, ztransschema))
# collect ztrans hedonics
cursor.execute(""" SELECT PROP.*,
						  TR.RecordingDate,
						  TR.DocumentDate,
						  TR.SignatureDate,
						  TR.EffectiveDate,
						  TR.SalesPriceAmount,
					      TR.SalesPriceAmountStndCode,
						  TR.LoanAmountStndCode,
						  TR.DataClassStndCode,
						  TR.DocumentTypeStndCode,
						  TR.PartialInterestTransferStndCode,
						  TR.IntraFamilyTransferFlag,
						  TR.TransferTaxExemptFlag,
						  TR.PropertyUseStndCode,
						  TR.AssessmentLandUseStndCode,
						  TR.OccupancyStatusStndCode
					INTO %s.HEDONICS
					FROM %s.PROPTRANS AS PROP
					INNER JOIN %s.TRANS AS TR
					USING (TransId)
			   """ % (ztransschema, ztransschema, ztransschema))
conn.commit()
print("ZTrans hedonics finished")
# Final hedonics
cursor.execute(""" SELECT ZASMTHED.*,
					      ZTRANSHED.transid,
						  ZTRANSHED.assessorparcelnumber,
						  ZTRANSHED.unformattedassessorparcelnumber,
						  ZTRANSHED.legallotsize,
						  ZTRANSHED.propertysequencenumber,
						  ZTRANSHED.propertyaddressmatchcode,
						  ZTRANSHED.propertyaddressgeocodematchcode,
						  ZTRANSHED.legalsectwnrngmer,
						  ZTRANSHED.legalcity,
						  ZTRANSHED.bkfspid,
						  ZTRANSHED.assessmentrecordmatchflag,
						  ZTRANSHED.recordingdate,
						  ZTRANSHED.documentdate,
						  ZTRANSHED.signaturedate,
						  ZTRANSHED.salespriceamountstndcode,
						  ZTRANSHED.loanamountstndcode,
						  ZTRANSHED.dataclassstndcode,
						  ZTRANSHED.partialinteresttransferstndcode,
						  ZTRANSHED.salespriceamount,
						  ZTRANSHED.IntraFamilyTransferFlag,
						  ZTRANSHED.TransferTaxExemptFlag
					INTO %s
					FROM %s.HEDONICS AS ZASMTHED
					INNER JOIN %s.HEDONICS AS ZTRANSHED
					ON ZASMTHED.importparcelid = ZTRANSHED.importparcelid
			   """ % ('hedonics_new.hedonics_'+st_num, zasmschema, ztransschema))
# Final commit
conn.commit()
print("Final hedonics finished")

# Deleting temporary schemas if necessary
if delete:
	print("Deleting temporary files...")
	cursor.execute(""" DROP SCHEMA %s CASCADE """ % (zasmschema))
	cursor.execute(""" DROP SCHEMA %s CASCADE """ % (ztransschema))
	conn.commit()

# Finished!
print("Extraction of hedonics for State %s finished" % st_num)
