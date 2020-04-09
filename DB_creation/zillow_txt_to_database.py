import psycopg2
import sys
import os

## Run in format: python3 zillow_txt_to_database.py ST_num

## Declare gloabal variables
# data path
path = '/home/schadri/share/projects/Zillow_Housing/stores/Zillow_2017_Nov/'
# database name to connect to and insert into
dbname = 'zillow_2017_nov'
# dataset name, can choose only ZAsmt or ZTrans if needed
dataset_list = ['ZAsmt', 'ZTrans']
# dataset dependency files
dataset_dep_files = {'ZAsmt':['Main.txt', 'Building.txt', 'BuildingAreas.txt'],
                     'ZTrans':['Main.txt', 'PropertyInfo.txt']}
# state number, see https://wiki.ncsa.illinois.edu/display/BDEEP/State+Code+Data?src=search
st_num = sys.argv[1]
# completion file name to check status
completionfilename = './store_records_new/Data_Stored_%s.txt' % st_num

## Sanity checks
# check input format
if len(sys.argv) != 2:
	print("Please enter a state number.")
	sys.exit(-1)

# check state number exists
if not os.path.exists(path + st_num):
	print("Please enter a valid state number (i.e. two digits).")
	sys.exit(-2)

## Connect to database
conn_string = "host='localhost' dbname='{}' user='postgres' password='bdeep'".format(dbname)
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()
print("Connected to database: host = localhost dbname = {} user = postgres".format(dbname))

## Start processing txt data
print("Start task for state: %s" % st_num)
for ds in dataset_list:
	# Create new schemas in the database
	schemaname = 'new' + ds.lower() + st_num
    # Note: delete the following line if schema already exists
	cursor.execute(""" CREATE SCHEMA %s """ % (schemaname))
	for ds_dep in dataset_dep_files[ds]:
		# Reset tracker and progress
		progress = 0
		tracker = 0
		# Create new table
		tablename = ds_dep[:-4].lower()
		cursor.execute(""" SELECT * INTO %s FROM %s """ % (schemaname+'.ut'+tablename, ds.lower()+'.'+tablename))
		conn.commit()
		# Interprete txt data
		print("Start processing %s/%s" % (ds, ds_dep))
		ds_file = path + st_num + '/' + ds + '/' + ds_dep
        # Note: Need to use latin-1, a single byte encoding style, to open file.
        #       Otherwise get UnicodeDecodeError.
		with open(ds_file, 'r', encoding='latin-1') as fileObj:
			try:
                # each line is a record
				for line in fileObj:
					progress += 1
					listFile = []
					line = line.rstrip('\n')
					line = line.strip()
					line = line.replace("'","")
                    # Note: listFile contains a list of attributes corresponding to
                    #       the given column
					listFile = line.split('|')
					counter = 0
					values = ''
					for item in listFile:
						if counter < len(listFile):
							if item == "" or item == '\x00':
								listFile[counter] = 'null'
							else:
								listFile[counter] = "'"+listFile[counter]+"'"
							counter = counter + 1
					for item in listFile:
						if values == "":
							values = values + item
						else:
                            # SQL requires comma separated insert value
							values = values + ',' + item
					try:
                        # Do not quite understand savepoint and recovery...
						cursor.execute("SAVEPOINT recovery")
						cursor.execute(""" INSERT INTO %s VALUES (%s)""" % (schemaname+'.ut'+tablename, values))
					except psycopg2.DataError as err:
						# Error! Store into Error File...
						errfilename = './error_records/failedAttempts_%s_%s_%s' % (st_num, ds, ds_dep)
						if not os.path.exists(errfilename):
							os.system('touch %s' % errfilename)
						with open(errfilename,'a+') as errFile:
							errFile.write("FAILED ATTEMPT, TRY AGAIN: ")
							errFile.write("INSERT INTO %s VALUES (%s)\n\n" % (schemaname+'.ut'+tablename, values))
							errFile.write("ERROR WAS %s \n\n" % err)
						progress -= 1
						cursor.execute("ROLLBACK TO SAVEPOINT recovery")
					tracker += 1
					cursor.execute("RELEASE recovery")
                    # Regular commit & report
					if progress % 100000 == 0 and progress != 0:
						conn.commit()
						print("Commited %s entries" % progress)
			except:
				print("Error occurs here!")
				sys.exit(-3)
		# Final commit
		conn.commit()
		# Report total progress
		print("Totally commited %s entries" % progress)
		print("Processed %s lines" % tracker)
		if tracker != 0:
			final_percent = (progress / tracker) * 100
		else:
			final_percent = 100
		print("Success percentage: %s%%\n" % final_percent)
		# Save to completion file
		if not os.path.exists(completionfilename):
			os.system('touch %s' % completionfilename)
		with open(completionfilename, 'a+') as completionFile:
			completionFile.write("File finished: "+ds_dep+"\t\tState Code: "+st_num+"\tSuccess rate:" + str(final_percent)+"%% \n")
# Finished!
print("Store into database for State %s finished" % st_num)
