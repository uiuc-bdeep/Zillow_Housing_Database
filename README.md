# Zillow_Housing_Database

This directory contains scripts for converting Zillow_Housing data from txt file to postgresql hedonics.
The first script converts txt file into database.
The second script converts the output from the above to the final hedonics ready for csv conversion.
To convert to csv files, use psql command:
COPY <tablename> TO '<csv filename>' DELIMITER ',' CSV HEADER;
