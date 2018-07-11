# Zillow_Housing_Database

## TXT -> Postgres Database
This directory contains scripts for converting Zillow_Housing data from txt file to postgresql hedonics.
The first script converts txt file into database. For example, if you want to convert for state AL (state code 01):
```
python3 zillow_txt_to_database.py 01
```
Next, you can use the second script to convert **the output from the above** to the final hedonics ready for csv conversion. If you still want the raw data, use:
```
python3 zillow_extract_hedonics.py 01
```
Otherwise, if the **output from the first script** is no longer needed, add `delete` prompt at the end. This will delete the files created by the first script at last:
```
python3 zillow_extract_hedonics.py 01 delete
```

## Postgres Database -> R
To transfer data from database into rds files, there are 2 possible ways.

- First method: Database -> rds
Set up a direct connection to the database and get the data, using `get_from_db.R`. Details in the script.

The advantage of this method is simple and direct, but the maximum transfer file size is somewhere between 6.5-9 GB for this VM with 64GB RAM.

- Second method: Database -> CSV -> rds
To convert to CSV tables, psql has a built-in function to convert to csv files:
```
COPY <tablename> TO '<csv filename>' DELIMITER ',' CSV HEADER;
```
Note that user postgres does not have permission to access (and save files at) `/home/ubuntu`. To get around, save files into `/tmp/` and then move to the desired location.

This method is a bit messy. The maximum transfer file size is somewhere between 17-22 GB for this VM with 64GB RAM.
