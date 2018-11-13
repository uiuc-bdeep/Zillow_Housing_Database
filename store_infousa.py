from sqlalchemy import create_engine
from sqlalchemy.types import *
import numpy as np
import sys
import pandas as pd

# Global constants
DB_NAME = 'infousa_2018'
FILE_PATH =  '/home/schadri/share/projects/InfoUSA/stores/'
COLUMNS = ['FAMILYID', 'LOCATION_TYPE', 'PRIMARY_FAMILY_IND', 'HOUSEHOLDSTATUS',
		   'TRADELINE_COUNT', 'HEAD_HH_AGE_CODE', 'LENGTH_OF_RESIDENCE',
		   'CHILDRENHHCOUNT', 'CHILDREN_IND', 'ADDRESSTYPE', 'MAILABILITY_SCORE',
		   'WEALTH_FINDER_SCORE', 'FIND_DIV_1000', 'OWNER_RENTER_STATUS',
		   'ESTMTD_HOME_VAL_DIV_1000', 'MARITAL_STATUS', 'PPI_DIV_1000',
		   'MSA2000_CODE', 'MSA2000_IDENTIFIER', 'CSA2000_CODE', 'CBSACODE',
		   'CBSATYPE', 'CSACODE', 'LOCATIONID', 'HOUSE_NUM', 'HOUSE_NUM_FRACTION',
		   'STREET_PRE_DIR', 'STREET_NAME', 'STREET_POST_DIR', 'STREET_SUFFIX',
		   'UNIT_TYPE', 'UNIT_NUM', 'BOX_TYPE', 'BOX_NUM', 'ROUTE_TYPE',
		   'ROUTE_NUM', 'CITY', 'STATE', 'ZIP ', 'ZIP4', 'DPBC', 'VACANT',
		   'USPSNOSTATS', 'LATITUDE', 'LONGITUDE', 'MATCHLEVEL', 'CENSUSSTATECODE',
		   'CENSUSCOUNTYCODE', 'CENSUSTRACT', 'CENSUSBLOCKGROUP',
		   'CENSUS2010COUNTYCODE', 'CENSUS2010TRACT', 'CENSUS2010BLOCK',
		   'FIRST_NAME_1', 'LAST_NAME_1', 'ETHNICITY_CODE_1',
		   'FIRST_NAME_2', 'LAST_NAME_2', 'ETHNICITY_CODE_2',
		   'FIRST_NAME_3', 'LAST_NAME_3', 'ETHNICITY_CODE_3']
DTYPEIN = {'FAMILYID': np.int64, 'LOCATION_TYPE': str,
		'PRIMARY_FAMILY_IND': np.int16, 'HOUSEHOLDSTATUS': str,
		'TRADELINE_COUNT': np.int16, 'HEAD_HH_AGE_CODE': str,
		'LENGTH_OF_RESIDENCE': np.int16, 'CHILDRENHHCOUNT': np.int16,
		'CHILDREN_IND': np.int16, 'ADDRESSTYPE': str,
		'MAILABILITY_SCORE': np.int16, 'WEALTH_FINDER_SCORE': np.int32,
		'FIND_DIV_1000': np.int32, 'OWNER_RENTER_STATUS': np.int16,
		'ESTMTD_HOME_VAL_DIV_1000': np.int16, 'MARITAL_STATUS': np.int16,
		'PPI_DIV_1000': np.int16, 'MSA2000_CODE': object,
		'MSA2000_IDENTIFIER': object, 'CSA2000_CODE': object,
		'CBSACODE': object, 'CBSATYPE': object, 'CSACODE': object,
		'LOCATIONID': object, 'HOUSE_NUM': object,
		'HOUSE_NUM_FRACTION': str, 'STREET_PRE_DIR': str,
		'STREET_NAME': str, 'STREET_POST_DIR': str,
		'STREET_SUFFIX': str, 'UNIT_TYPE': str,
		'UNIT_NUM': object, 'BOX_TYPE': str, 'BOX_NUM': object,
		'ROUTE_TYPE': str, 'ROUTE_NUM': object, 'CITY': str,
		'STATE': str, 'ZIP': np.int32, 'ZIP4': object,
		'DPBC': object, 'VACANT': bool, 'USPSNOSTATS': bool,
		'LATITUDE': float, 'LONGITUDE': float,
		'MATCHLEVEL': str, 'CENSUSSTATECODE': object,
		'CENSUSCOUNTYCODE': object, 'CENSUSTRACT': object,
		'CENSUSBLOCKGROUP': object, 'CENSUS2010COUNTYCODE': object,
		'CENSUS2010TRACT': object, 'CENSUS2010BLOCK': object,
		'FIRST_NAME_1': str, 'LAST_NAME_1': object, 'ETHNICITY_CODE_1': str,
		'FIRST_NAME_2': str, 'LAST_NAME_2': object, 'ETHNICITY_CODE_2': str,
		'FIRST_NAME_3': str, 'LAST_NAME_3': object, 'ETHNICITY_CODE_3': str}
DTYPE = {'FAMILYID': BigInteger, 'LOCATION_TYPE': CHAR(1),
		'PRIMARY_FAMILY_IND': SmallInteger, 'HOUSEHOLDSTATUS': CHAR(1),
		'TRADELINE_COUNT': SmallInteger, 'HEAD_HH_AGE_CODE': CHAR(1),
		'LENGTH_OF_RESIDENCE': SmallInteger, 'CHILDRENHHCOUNT': SmallInteger,
		'CHILDREN_IND': SmallInteger, 'ADDRESSTYPE': CHAR(1),
		'MAILABILITY_SCORE': SmallInteger, 'WEALTH_FINDER_SCORE': Integer(),
		'FIND_DIV_1000': Integer(), 'OWNER_RENTER_STATUS': SmallInteger,
		'ESTMTD_HOME_VAL_DIV_1000': SmallInteger, 'MARITAL_STATUS': SmallInteger,
		'PPI_DIV_1000': SmallInteger, 'MSA2000_CODE': Integer(),
		'MSA2000_IDENTIFIER': SmallInteger, 'CSA2000_CODE': SmallInteger,
		'CBSACODE': Integer(), 'CBSATYPE': SmallInteger, 'CSACODE': SmallInteger,
		'LOCATIONID': BigInteger, 'HOUSE_NUM': String(16),
		'HOUSE_NUM_FRACTION': String(4), 'STREET_PRE_DIR': String(2),
		'STREET_NAME': String(48), 'STREET_POST_DIR': String(2),
		'STREET_SUFFIX': CHAR(2), 'UNIT_TYPE': String(16),
		'UNIT_NUM': String(16), 'BOX_TYPE': String(2), 'BOX_NUM': String(16),
		'ROUTE_TYPE': String(2), 'ROUTE_NUM': String(8), 'CITY': String(32),
		'STATE': CHAR(2), 'ZIP': Integer(), 'ZIP4': SmallInteger,
		'DPBC': SmallInteger, 'VACANT': Boolean, 'USPSNOSTATS': Boolean,
		'LATITUDE': Numeric(precision=9,scale=6), 'LONGITUDE': Numeric(precision=9,scale=6),
		'MATCHLEVEL': CHAR(1), 'CENSUSSTATECODE': SmallInteger,
		'CENSUSCOUNTYCODE': SmallInteger, 'CENSUSTRACT': Integer(),
		'CENSUSBLOCKGROUP': SmallInteger, 'CENSUS2010COUNTYCODE': SmallInteger,
		'CENSUS2010TRACT': Integer(), 'CENSUS2010BLOCK': SmallInteger,
		'FIRST_NAME_1': String(32), 'LAST_NAME_1': String(32), 'ETHNICITY_CODE_1': CHAR(2),
		'FIRST_NAME_2': String(32), 'LAST_NAME_2': String(32), 'ETHNICITY_CODE_2': CHAR(2),
		'FIRST_NAME_3': String(32), 'LAST_NAME_3': String(32), 'ETHNICITY_CODE_3': CHAR(2)}
NUM_RECS = {'2006': 130286699,
			'2007': 134605437,
			'2008': 129712763,
			'2009': 129795480,
			'2010': 128396746,
			'2011': 136644072,
			'2012': 141255062,
			'2013': 159093050,
			'2014': 153464188,
			'2015': 147934367,
			'2016': 144796815,
			'2017': 151324487}

# Define chunk size
CHUNKSIZE = 100000

# Input check
if len(sys.argv) < 2:
	print("Please enter a year!")
	sys.exit(-1)
elif len(sys.argv) > 3:
	print("Invalid input format!")
	sys.exit(-1)

# I/O constants
FILENAME = 'Household_Ethnicity_%s.txt' % str(sys.argv[1])
TABLENAME = 'year' + str(sys.argv[1])

# Where to start
START = 0
if len(sys.argv) > 2:
	START = int(sys.argv[2])
assert(START >= 0)

# Read data
df = pd.read_table(FILE_PATH + FILENAME, chunksize=CHUNKSIZE, dtype=DTYPEIN, encoding='latin1')
print("Reading data from disk...")

# Use SQLalchemy to create connection
conn = create_engine('postgresql://postgres:bdeep@localhost:5432/%s' % DB_NAME, encoding='latin1')

# First create table in the database...
# (in order to specify type)

# Insert data
progress = 0
for chunk in df:
	chunk.columns = COLUMNS
	if progress < START - 1:
		progress += chunk.shape[0]
		print("Skipped "+ str(progress))
	else:
		chunk.to_sql(TABLENAME,
					 conn,
					 if_exists = 'append',
					 index = False,
					 dtype = DTYPE
					 )
		progress += chunk.shape[0]
		print("Inserted "+ str(progress))

# success
print("Finished!")
print("Total %s out of %s records (%s) are inserted!"
						 % (str(progress),
							NUM_RECS[str(sys.argv[1])],
							progress / NUM_RECS[str(sys.argv[1])]))
