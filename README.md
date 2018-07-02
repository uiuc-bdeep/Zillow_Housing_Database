# Zillow_Housing_Database

This directory contains scripts for converting Zillow_Housing data from txt file to postgresql hedonics.
The first script converts txt file into database. For example, if you want to convert for state AL (state code 01):
```
python3 zillow_txt_to_database.py 01
```
Next, you can use the second script to convert **the output from the above** to the final hedonics ready for csv conversion. If you still want the raw data, use:
```
python3 zillow_extract_hedonics.py 01
```
Otherwise, if the **output from the first script** is longer needed, add `delete` prompt at the end. This will delete the files created by the first script at last:
```
python3 zillow_extract_hedonics.py 01 delete
```
Finally, to convert to readable tables, psql has a built-in function to convert to csv files:
```
COPY <tablename> TO '<csv filename>' DELIMITER ',' CSV HEADER;
```
Note that user postgres does not have permission to access (and save files at) `/home/ubuntu`. To get around, save files into `/tmp/` and then move to the desired location.
Otherwise, if rds files are preferred, relate to `get_from_db.R` for details.
