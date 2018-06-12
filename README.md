# Zillow_Housing_Database

This directory contains scripts for converting Zillow_Housing data from txt file to postgresql hedonics.
The first script converts txt file into database. For example, if you want to convert for state AL (state code 01):
'''
python3 zillow_txt_to_database.py 01
'''
Next, you can use the second script to convert the output from the above to the final hedonics ready for csv conversion. If you still want the raw data, use:
'''
python3 zillow_extract_hedonics.py 01
'''
Otherwise, if raw data is going to be deleted, add ' delete ' prompt at the end:
'''
python3 zillow_extract_hedonics.py 01 delete
'''
Finally, to convert to csv files, use psql command:
'''
COPY <tablename> TO '<csv filename>' DELIMITER ',' CSV HEADER;
'''
