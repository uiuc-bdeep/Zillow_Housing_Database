from sqlalchemy import create_engine
import json
import psycopg2
import sys
import pandas as pd
#We are going to preprocess the data before we start editing it!
if len(sys.argv) == 1:
	print("Please enter in a filename followed by a tablename")
	sys.exit()
conn_string = "host='localhost' dbname='Air_BnB' user='postgres' password='bdeep'"
print ("Connecting to database...\n host = localhost dbname='Air_BnB' user ='postgres'\n")
#conn = psycopg2.connect(conn_stringi)
conn = create_engine('postgresql://postgres:bdeep@localhost:5432/Air_BnB')
#cursor = conn.cursor()
filename = sys.argv[1]
tableName = sys.argv[2]
print("FILTERING/INSERTING DATA from "+ filename+ " into Table: " + tableName)
filepath = '/home/ubuntu/stores/AirBnB/us_mean/'+filename
#chunks = pd.read_csv('filename.csv', chunksize=100000)
df = pd.read_csv(filepath, chunksize=1000000)
first = False
progress = 0
for chunk in df:
	chunk.columns = ['No','PropertyID','Date','TMean']
	if first:
		chunk.to_sql(tableName, conn, if_exists = 'replace')
		first = False
	else: 
		chunk.to_sql(tableName, conn, if_exists='append')
	progress += chunk.shape[0]
	print("Inserted "+ str(progress))
#cursor.execute("COPY US_Daily FROM '/home/ubuntu/stores/"+filename +"' DELIMITER ',' CSV HEADER NULL AS ['NA','']")

print("Inserted approximetley "+ str(progress) +" into "+tableName + " successfully stored")
	

