#     ------------------------------------------------------------------------
#   |                                                                         |
#   |  Creates Hedonics Dataset for all states                                |
#   |                                                                         |
#   |  By:                                                                    |
#   |  Yifang Zhang                                                           |
#   |  Aug. 9, 2017                                                           |    
#   |  Data Analytics                                                         |
#   |  National Center for Supercomputing Application                         |
#   |                                                                         |
#   |  Revised: Sep. 17, 2018, Yuchen Liang                                   |
#   |                                                                         |
#     ------------------------------------------------------------------------

###################################
# Prelimilary
rm(list=ls())
pkgTest <- function(x) {
  if (!require(x, character.only = TRUE))
  {
    install.packages(x, dep = TRUE)
    if(!require(x, character.only = TRUE)) stop("Package not found")
  }
}

## These lines load the required packages
packages <- c("readxl", "data.table", "rgdal", "sp")
lapply(packages, pkgTest)

abbr <- c("AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", 
          "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", 
          "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", 
          "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", 
          "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", 
          "WY", "AS", "GU", "MP", "PR", "VI", "UM")

code <- c("01", "02", "04", "05", "06", "08", "09", "10", "11", "12", 
          "13", "15", "16", "17", "18", "19", "20", "21", "22", "23", 
          "24", "25", "26", "27", "28", "29", "30", "31", "32", "33", 
          "34", "35", "36", "37", "38", "39", "40", "41", "42", "44", 
          "45", "46", "47", "48", "49", "50", "51", "53", "54", "55", 
          "56", "60", "66", "69", "72", "78", "74")

###################################
# specifying the state
state <- "SD"     #testing
state_code <- code[which(abbr == state)]

#####################################
# speciying parameters
on_rstudio <- TRUE

if (on_rstudio) ## if running on rstudio, using the parameters here, otherwise, take it from the command line###still need to implement that
{
  DB_path <- "/home/bdeep/share/projects/Zillow_Housing/stores/Zillow_2017_Nov/"

  hedonics_output <- "/home/schadri/testHedonics/"

  census_block_input <- paste0("/home/bdeep/share/projects/Zillow_Housing/stores/CensusBlocks/tl_2016_", state_code, "_tabblock10.shp")
  
  default_projection <- "+proj=longlat +datum=WGS84 +no_defs"
}

#####################################
# Running the Hedonics

{
  ######################################################################
  # Prepare the tables and fields for extraction Zillow Data
  # Change directory to where you've stored ZTRAX
  # dir <- paste0(DB_path,"DB", state_code) #"DB17"
  dir <- paste0(DB_path, state_code) #"DB17"

  if (!file.exists(dir))
  {
    stop(paste("no folder for DB", toString(state_code),"found"))
  }
  
  ts <- Sys.time()
  
  ## the state code can be find at:
  ## https://en.wikipedia.org/wiki/Federal_Information_Processing_Standard_state_code
  layoutZAsmt <- read_excel(file.path(DB_path, 'Layout.xlsx'), sheet = 1) #("../stores/", 'Layout.xlsx'), sheet = 1)
  layoutZTrans <- read_excel(file.path(DB_path, 'Layout.xlsx'),            #("../stores/", 'Layout.xlsx'),
                             sheet = 2,
                             col_types = c("text", "text", "numeric", "text", "text"))
  
  ## always NOT do prototyping
  rows2load <- -1 # load everything
  options(scipen = 999) # Do not print scientific notation
  options(stringsAsFactors = FALSE) ## Do not load strings as factors

  ##  Create property attribute table
  #    Need 3 tables
  #    1) Main table in assessor database
  #    2) Building table
  #    3) BuildingAreas
  
  col_namesMain <- t(layoutZAsmt[layoutZAsmt$TableName == 'utMain', 'FieldName'])
  col_namesBldg <- t(layoutZAsmt[layoutZAsmt$TableName == 'utBuilding', 'FieldName'])
  col_namesBldgA <- t(layoutZAsmt[layoutZAsmt$TableName == 'utBuildingAreas', 'FieldName'])
  
  ######################################################################
  # Pull address, geographic, lot size, and tax data from main table
  ## get main table
  print("Get main table")
  base <- read.table(file.path(dir, "ZAsmt/Main.txt"),
                     nrows = rows2load,
                     sep = '|',
                     header = FALSE,
                     stringsAsFactors = FALSE,
                     skipNul = TRUE,                            # tells R to treat two ajacent delimiters as dividing a column
                     comment.char="",                           # tells R not to read any symbol as a comment
                     quote = "",                                # this tells R not to read quotation marks as a special symbol
                     col.names = col_namesMain
  )
  
  base <- as.data.table(base)
  base <- base[ , list(RowID, ImportParcelID, LoadID,
                       FIPS, State, County,
                       PropertyFullStreetAddress,
                       PropertyHouseNumber, PropertyHouseNumberExt, PropertyStreetPreDirectional, PropertyStreetName, PropertyStreetSuffix, PropertyStreetPostDirectional,
                       PropertyCity, PropertyState, PropertyZip,
                       PropertyBuildingNumber, PropertyAddressUnitDesignator, PropertyAddressUnitNumber,
                       PropertyAddressLatitude, PropertyAddressLongitude, PropertyAddressCensusTractAndBlock,
                       NoOfBuildings,
                       LotSizeAcres, LotSizeSquareFeet,
                       TaxAmount, TaxYear)]
  ## we want this
  # Keep only one record for each ImportPropertyID.
  # ImportParcelID is the unique identifier of a parcel. Multiple entries for the same ImportParcelID are due to updated records.
  # The most recent record is identified by the greatest LoadID.
  #   **** This step may not be necessary for the published dataset as we intend to only publish the updated records, but due dilligence demands we check.
  
  length(unique(base$ImportParcelID))  # Number of unique ImportParcelIDs
  dim(base)[1]                         # Number of rows in the base dataset
  
  if( length(unique(base$ImportParcelID)) != dim(base)[1] ){
    
    #Example: Print all entries for parcels with at least two records.
    base[ImportParcelID %in% base[duplicated(ImportParcelID), ImportParcelID], ][order(ImportParcelID)]
    
    setkeyv(base, c("ImportParcelID", "LoadID"))  # Sets the index and also orders by ImportParcelID, then LoadID increasing
    keepRows <- base[ ,.I[.N], by = c("ImportParcelID")]   # Creates a table where the 1st column is ImportParcelID and the second column
    # gives the row number of the last row that ImportParcelID appears.
    base <- base[keepRows[[2]], ] # Keeps only those rows identified in previous step
    
  }
  
  ######################################################################
  #### Load most property attributes
  print("Load most property")
  bldg <- read.table(file.path(dir, "ZAsmt/Building.txt"),
                     nrows = rows2load,                    # this is set just to test it out. Remove when code runs smoothly.
                     sep = '|',
                     header = FALSE,
                     stringsAsFactors = FALSE,
                     skipNul = TRUE,                            # tells R to treat two ajacent delimiters as dividing a column
                     comment.char="",                           # tells R not to read any symbol as a comment
                     quote = "",                                # this tells R not to read quotation marks as a special symbol
                     col.names = col_namesBldg
  )
  
  bldg <- as.data.table(bldg)
  
  bldg <- bldg[ , list(RowID, NoOfUnits, BuildingOrImprovementNumber,
                       YearBuilt, EffectiveYearBuilt, YearRemodeled,
                       NoOfStories, StoryTypeStndCode, TotalRooms, TotalBedrooms,
                       FullBath, ThreeQuarterBath, HalfBath, QuarterBath,
                       HeatingTypeorSystemStndCode,
                       PropertyLandUseStndCode, WaterStndCode)]
  
  
  #  Reduce bldg dataset to Single-Family Residence, Condo's, Co-opts (or similar)
  print("Reducing bldg dataset") 
  bldg <- bldg[PropertyLandUseStndCode %in% c('RR101',  # SFR
                                              'RR999',  # Inferred SFR
                                              # 'RR102',  # Rural Residence   (includes farm/productive land?)
                                              'RR104',  # Townhouse
                                              'RR105',  # Cluster Home
                                              'RR106',  # Condominium
                                              'RR107',  # Cooperative
                                              'RR108',  # Row House
                                              'RR109',  # Planned Unit Development
                                              'RR113',  # Bungalow
                                              'RR116',  # Patio Home
                                              'RR119',  # Garden Home
                                              'RR120'), # Landominium
               ]
  
  ######################################################################
  #### Load building squarefoot data
  print("Load building sqft data")
  sqft <- read.table(file.path(dir, "ZAsmt/BuildingAreas.txt"),
                     nrows = rows2load,                    # this is set just to test it out. Remove when code runs smoothly.
                     sep = '|',
                     header = FALSE,
                     stringsAsFactors = FALSE,
                     skipNul = TRUE,                            # tells R to treat two ajacent delimiters as dividing a column
                     comment.char="",                           # tells R not to read any symbol as a comment
                     quote = "",                                # this tells R not to read quotation marks as a special symbol
                     col.names = col_namesBldgA
  )
  
  
  sqft <- as.data.table(sqft)
  
  # Counties report different breakdowns of building square footage and/or call similar concepts by different names.
  # The structure of this table is to keep all entries reported by the county as they are given. See 'Bldg Area' table in documentation.
  # The goal of this code is to determine the total square footage of each property.
  # We assume a simple logic to apply across all counties here. Different logic may be as or more valid.
  # The logic which generates square footage reported on our sites is more complex, sometimes county specific, and often influenced by user interaction and update.
  print("Finding total sqft of each property")
  sqft <- sqft[BuildingAreaStndCode %in% c('BAL',  # Building Area Living
                                           'BAF',  # Building Area Finished
                                           'BAE',  # Effective Building Area
                                           'BAG',  # Gross Building Area
                                           'BAJ',  # Building Area Adjusted
                                           'BAT',  # Building Area Total
                                           'BLF'), # Building Area Finished Living
               ]
  
  table(sqft$BuildingOrImprovementNumber)  # BuildingOrImprovementNumber > 1  refers to additional buildings on the parcel.
  
  sqft <- sqft[ , list(sqfeet = max(BuildingAreaSqFt, na.rm = T)), by = c("RowID", "BuildingOrImprovementNumber")]
  
  ###############################################################################
  #   Merge previous three datasets together to form attribute table
  
  attr <- merge(base, bldg, by = "RowID")
  attr <- merge(attr, sqft, by = c("RowID", "BuildingOrImprovementNumber"), all.x=TRUE)
  
  ## write an intermediate file ##
  #saveRDS(attr, paste0(hedonics_output, "../temp_attr/", state, ".rds"))
  
  rm(base)
  rm(bldg)
  rm(sqft)
  gc() ## collecting garbage
  
  ###############################################################################
  #  Load transaction dataset.
  #     Need two tables
  #      1) PropertyInfo table provided ImportParcelID to match transaction to assessor data loaded above
  #      2) Main table in Ztrans database provides information on real estate events
  print("Loading transaction dataset")
  col_namesProp <- t(layoutZTrans[layoutZTrans$TableName == 'utPropertyInfo', 'FieldName'])
  col_namesMainTr <-t(layoutZTrans[layoutZTrans$TableName == 'utMain', 'FieldName'])
  
  ###############################################################################
  #   Load PropertyInfo table for later merge
  print("Load propertyinfo table")
  propTrans <- read.table(file.path(dir, "ZTrans/PropertyInfo.txt"),
                          nrows = rows2load,                    # this is set just to test it out. Remove when code runs smoothly.
                          sep = '|',
                          header = FALSE,
                          stringsAsFactors = FALSE,
                          skipNul = TRUE,                            # tells R to treat two ajacent delimiters as dividing a column
                          comment.char="",                           # tells R not to read any symbol as a comment
                          quote = "",                                # this tells R not to read quotation marks as a special symbol
                          col.names = col_namesProp
  )
  
  propTrans <- as.data.table(propTrans)
  
  propTrans <- propTrans[ , list(TransId, PropertySequenceNumber, LoadID, ImportParcelID)]
  
  ## we do that
  
  # Keep only one record for each TransID and PropertySequenceNumber.
  # TransID is the unique identifier of a transaction, which could have multiple properties sequenced by PropertySequenceNumber.
  # Multiple entries for the same TransID and PropertySequenceNumber are due to updated records.
  # The most recent record is identified by the greatest LoadID.
  #   **** This step may not be necessary for the published dataset as we intend to only publish most updated record.
  
  setkeyv(propTrans, c("TransId", "PropertySequenceNumber", "LoadID"))
  keepRows <- propTrans[ ,.I[.N], by = c("TransId", "PropertySequenceNumber")]
  propTrans <- propTrans[keepRows[[3]], ]
  propTrans[ , LoadID:= NULL]
  
  # Drop transactions of multiple parcels (transIDs associated with PropertySequenceNumber > 1)
  
  dropTrans <- unique(propTrans[PropertySequenceNumber > 1, TransId])
  propTrans <- propTrans[!(TransId %in% dropTrans), ]   # ! is "not"
  
  rm(keepRows)
  gc()
  
  #######################################################################################
  #  Load main table in Ztrans database, which provides information on real estate events
  print ("Load main table")
  trans <- read.table(file.path(dir, "ZTrans/Main.txt"),
                      nrows = rows2load,                    # this is set just to test it out. Remove when code runs smoothly.
                      sep = '|',
                      header = FALSE,
                      stringsAsFactors = FALSE,
                      skipNul = TRUE,                            # tells R to treat two ajacent delimiters as dividing a column
                      comment.char="",                           # tells R not to read any symbol as a comment
                      quote = "",                                # this tells R not to read quotation marks as a special symbol
                      col.names = col_namesMainTr
  )
  
  trans <- as.data.table(trans)
  
  trans <- trans[ , list(TransId, LoadID,
                         RecordingDate, DocumentDate, SignatureDate, EffectiveDate,
                         SalesPriceAmount, LoanAmount,
                         SalesPriceAmountStndCode, LoanAmountStndCode,
                         # These remaining variables may be helpful to, although possibly not sufficient for, data cleaning. See documentation for all possible variables.
                         DataClassStndCode, DocumentTypeStndCode,
                         PartialInterestTransferStndCode, IntraFamilyTransferFlag, TransferTaxExemptFlag,
                         PropertyUseStndCode, AssessmentLandUseStndCode,
                         OccupancyStatusStndCode)]
  
  # Keep only one record for each TransID.
  # TransID is the unique identifier of a transaction.
  # Multiple entries for the same TransID are due to updated records.
  # The most recent record is identified by the greatest LoadID.
  #   **** This step may not be necessary for the published dataset as we intend to only publish most updated record.
  
  setkeyv(trans, c("TransId", "LoadID"))
  keepRows <- trans[ ,.I[.N], by = "TransId"]
  trans <- trans[keepRows[[2]], ]
  trans[ , LoadID:= NULL]
  
  rm(keepRows)
  #  Keep only events which are deed transfers (excludes mortgage records, foreclosures, etc. See documentation.)
  
  trans <- trans[DataClassStndCode %in% c('D', 'H', 'F', 'M'), ] ## D: Deed Transfer, H: Mortage, F: foreclosure, M: mortage
  
  ###############################################################################
  #   Merge previous two datasets together to form transaction table
  
  transComplete <- merge(propTrans, trans, by = "TransId")
  rm(propTrans)
  rm(trans)
  gc()
  
  ###############################################################################
  #   Merge the trans and azmt table together
  print("getting finalResult")
  finalResult <- merge(transComplete, attr, by = "ImportParcelID")
  
  ###############################################################################
  #   write the the final merging result 
  WRITEFILE = TRUE
  print(paste0("Writing to file ", paste0(hedonics_output, state, "Hedonics.rds")))
  if(WRITEFILE){
    #saveRDS(finalResult, paste0(hedonics_output, state, "Hedonics.rds")) #"~/share/projects/VotingBehavior/production/ILHedonics.rds")
  }
  
  rm(transComplete)
  rm(attr)
  gc()
  
  td <- difftime(Sys.time(), ts, units = c("mins"))
  print(td)
}
# 
# ###################################
# # Adding Census Blocks
# # function for extracting layer names form the path
# 
# get_layerName <- function(shp_file_path)
# {
#   relative_path <- path.expand(shp_file_path)
#   return(ogrListLayers(relative_path)[1])
# }
# 
# 
# {
#   ##################################################
#   ## please filling in the dataframe you need to use as points here
#   points <- finalResult
#   # points <- readRDS(paste0(hedonics_output, state, "Hedonics.rds"))
#   
#   ## remove NAs for lat and lon for shapefile 
#   points <- points[which(!is.na(points$PropertyAddressLongitude) & !is.na(points$PropertyAddressLatitude)),]
#   # step 2-2: transfer it into shapefile #
#   coordinates(points) <- cbind(points$PropertyAddressLongitude, points$PropertyAddressLatitude)
#   proj4string(points) <- CRS("+proj=longlat")
#   
#   
#   ##################################################
#   ## please filling in the polygon path you wanted in here
#   polygons_path <- paste0("~/share/projects/zillow/stores/CensusBlocks/", state_code, "/tl_2016_", state_code, "_tabblock10.shp") # "~/share/projects/zillow/stores/CensusBlocks/09/tl_2016_09_tabblock10.shp"
#   
#   # step 4: preprocess for the polygons #
#   shpLayerName <- get_layerName(polygons_path)
#   shp_poly <- readOGR(path.expand(polygons_path), shpLayerName)
#   # step 4-1: checking for default projection #
#   if(is.na(proj4string(shp_poly))){
#     default_projection <- CRS("+proj=longlat +datum=WGS84 +ellps=WGS84 +towgs84=0,0,0")
#     proj4string(shp_poly) <- CRS(default_projection)
#   }
#   
#   # step 4-2: assigning indices on polygon dataframe #
#   shp_poly$ID <- seq.int(nrow(shp_poly)) # not useful ID, remove later
#   
#   # step 5: transform the projection from points to the projection of the shapefile #
#   points <- spTransform(points, proj4string(shp_poly))
#   proj4string(points) <- proj4string(shp_poly)
#   gc()
#   
#   # step 6: perform the over operation #
#   res <- over(points, shp_poly)
#   
#   # step 7: Appending the polygons' information to points dataframe #
#   points_res <- as.data.frame(points)
#   points_res <- cbind(points_res, res)
#   
#   saveRDS(points_res, paste0(hedonics_output, state, "Hedonics_withTract.rds"))
# }
# 

# 
# ##############################################################
# ################# Subsetting for cities ######################
# ##############################################################
# 
# state <- "MI"
# temp_state <- readRDS(paste0("~/share/projects/zillow/intermediate/Hedonics/", state, "Hedonics_withTract.rds"))
# 
# cities <- c("BATTLE CREEK", "LANSING", "KALAMAZOO", "WARREN", "PONTIAC", "SOUTHFIELD", "SAGINAW", "TAYLOR", "DEARBORN", "WESTLAND")
# cities_save <- c("battle_creek", "lansing", "kalamazoo", "warren", "pontiac", "southfield", "saginaw", "taylor", "dearborn", "westland")
# 
# for (city in cities){
#   temp_trans <- temp_state[temp_state$PropertyCity == city,]
#   city_save <- cities_save[which(cities == city)]
#   saveRDS(temp_trans, paste0("~/share/projects/zillow/intermediate/Hedonics/cities/", city_save, "_", state, ".rds"))
#   write.csv(temp_trans, paste0("~/share/projects/zillow/intermediate/Hedonics/cities/", city_save, "_", state, ".csv"), row.names = FALSE)
# }
# 
# ##############################################################
# 
# state <- "CT"
# temp_state <- readRDS("~/share/projects/zillow/intermediate/Hedonics/", state, "Hedonics_withTract.rds")
# 
# cities <- c("BRIDGEPORT", "HARTFORD")
# cities_save <- c("bridgeport", "hartford")
# 
# for (city in cities){
#   temp_trans <- temp_state[temp_state$PropertyCity == city,]
#   city_save <- cities_save[which(cities == city)]
#   saveRDS(temp_trans, paste0("~/share/projects/zillow/intermediate/Hedonics/cities/", city_save, "_", state, ".rds"))
#   write.csv(temp_trans, paste0("~/share/projects/zillow/intermediate/Hedonics/cities/", city_save, "_", state, ".csv"), row.names = FALSE)
# }
# 
# ##############################################################
# 
# state <- "DE"
# temp_state <- readRDS("~/share/projects/zillow/intermediate/Hedonics/", state, "Hedonics_withTract.rds")
# 
# cities <- c("WILMINGTON")
# cities_save <- c("wilmington")
# 
# for (city in cities){
#   temp_trans <- temp_state[temp_state$PropertyCity == city,]
#   city_save <- cities_save[which(cities == city)]
#   saveRDS(temp_trans, paste0("~/share/projects/zillow/intermediate/Hedonics/cities/", city_save, "_", state, ".rds"))
#   write.csv(temp_trans, paste0("~/share/projects/zillow/intermediate/Hedonics/cities/", city_save, "_", state, ".csv"), row.names = FALSE)
# }
# 
# ##############################################################
# 
# state <- "IL"
# temp_state <- readRDS("~/share/projects/zillow/intermediate/Hedonics/", state, "Hedonics_withTract.rds")
# 
# cities <- c("THORNTON", "BLOOMINGTON", "DECATUR")
# cities_save <- c("thornton", "bloomington", "decatur")
# 
# for (city in cities){
#   temp_trans <- temp_state[temp_state$PropertyCity == city,]
#   city_save <- cities_save[which(cities == city)]
#   saveRDS(temp_trans, paste0("~/share/projects/zillow/intermediate/Hedonics/cities/", city_save, "_", state, ".rds"))
#   write.csv(temp_trans, paste0("~/share/projects/zillow/intermediate/Hedonics/cities/", city_save, "_", state, ".csv"), row.names = FALSE)
# }
# 
# ##############################################################
# 
# state <- "IN"
# temp_state <- readRDS("~/share/projects/zillow/intermediate/Hedonics/", state, "Hedonics_withTract.rds")
# 
# cities <- c("WAYNE", "GRAY", "PORTAGE")
# cities_save <- c("wayne", "gray", "portage")
# 
# for (city in cities){
#   temp_trans <- temp_state[temp_state$PropertyCity == city,]
#   city_save <- cities_save[which(cities == city)]
#   saveRDS(temp_trans, paste0("~/share/projects/zillow/intermediate/Hedonics/cities/", city_save, "_", state, ".rds"))
#   write.csv(temp_trans, paste0("~/share/projects/zillow/intermediate/Hedonics/cities/", city_save, "_", state, ".csv"), row.names = FALSE)
# }
# 
# ##############################################################
# 
# state <- "NJ"
# temp_state <- readRDS("~/share/projects/zillow/intermediate/Hedonics/", state, "Hedonics_withTract.rds")
# 
# cities <- c("CAMDEN", "EAST ORANGE", "IRVINGTON", "TRENTON")
# cities_save <- c("camdem", "est_orange", "irvington", "trenton")
# 
# for (city in cities){
#   temp_trans <- temp_state[temp_state$PropertyCity == city,]
#   city_save <- cities_save[which(cities == city)]
#   saveRDS(temp_trans, paste0("~/share/projects/zillow/intermediate/Hedonics/cities/", city_save, "_", state, ".rds"))
#   write.csv(temp_trans, paste0("~/share/projects/zillow/intermediate/Hedonics/cities/", city_save, "_", state, ".csv"), row.names = FALSE)
# }
# 
# ##############################################################
# 
# state <- "OH"
# temp_state <- readRDS("~/share/projects/zillow/intermediate/Hedonics/", state, "Hedonics_withTract.rds")
# 
# cities <- c("YOUNGSTOWN", "DAYTON")
# cities_save <- c("youngstown", "dayton")
# 
# for (city in cities){
#   temp_trans <- temp_state[temp_state$PropertyCity == city,]
#   city_save <- cities_save[which(cities == city)]
#   saveRDS(temp_trans, paste0("~/share/projects/zillow/intermediate/Hedonics/cities/", city_save, "_", state, ".rds"))
#   write.csv(temp_trans, paste0("~/share/projects/zillow/intermediate/Hedonics/cities/", city_save, "_", state, ".csv"), row.names = FALSE)
# }
# 
# ##############################################################
# 
# state <- "PA"
# temp_state <- readRDS("~/share/projects/zillow/intermediate/Hedonics/", state, "Hedonics_withTract.rds")
# 
# cities <- c("READING")
# cities_save <- c("reading")
# 
# for (city in cities){
#   temp_trans <- temp_state[temp_state$PropertyCity == city,]
#   city_save <- cities_save[which(cities == city)]
#   saveRDS(temp_trans, paste0("~/share/projects/zillow/intermediate/Hedonics/cities/", city_save, "_", state, ".rds"))
#   write.csv(temp_trans, paste0("~/share/projects/zillow/intermediate/Hedonics/cities/", city_save, "_", state, ".csv"), row.names = FALSE)
# }


