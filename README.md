# Zillow_Housing_Database Overview

Basic pipeline:
```
                                         zillow_extract_hedonics.py
                                                ------------                                                
                                                \          /                                                
    Input        zillow_txt_to_database.py       \        /  package:BDEEPZillow                                              
     Raw        --------------------------->      Postgres   ------------------->     R      ------>    (Further   
     File       --------------------------->      Database   ------------------->    Data    ------>     Processing
(txt, csv, ...)                                                                                          ...)      
```

## TXT -> Postgres Database
[zillow_txt_to_database.py](./zillow_txt_to_database.py) converts Zillow_Housing raw data from txt file to postgresql database hedonics. For example, if you want to convert for state AL (state code 01):
```
python3 zillow_txt_to_database.py 01
```

## In-database Generation
Next, you can use [zillow_extract_hedonics.py](./zillow_extract_hedonics.py) to convert **the output from the above** to the final hedonics ready for csv conversion. If you still want the raw data, use:
```
python3 zillow_extract_hedonics.py 01
```
Otherwise, if the **output from the first script** is no longer needed, add `delete` prompt at the end. This will delete the files created by the first script at last:
```
python3 zillow_extract_hedonics.py 01 delete
```

## Postgres Database -> R
To transfer data from database into rds files, there are 2 possible ways.

#### First method: Database -> rds

Set up a direct connection to the database and get the data, using package [`BDEEPZillow`](./BDEEPZillow/). The reference tables `county_state_fips.rds` and `field_name.rds` are also copied to this root directory. Details in the package folder.

The advantage of this method is simple and direct, but the maximum transfer file size is somewhere between 6.5-9 GB for this VM with 64GB RAM.

#### Second method: Database -> CSV -> rds

To convert to CSV tables, psql has a built-in function to convert to csv files:
```
COPY <tablename> TO '<csv filename>' DELIMITER ',' CSV HEADER;
```
Note that user postgres does not have permission to access (and save files at) `/home/ubuntu`. To get around, save files into `/tmp/` and then move to the desired location.

This method is a bit messy. The maximum transfer file size is somewhere between 17-22 GB for this VM with 64GB RAM.

## File Reference in Nebula
- All scripts: `/projects/Zillow_Housing/scripts/Generate/<script_name>`
- CSV hedonics files: `/projects/Zillow_Housing/stores/Hedonics/new_csv_hedonics_by_states/<state>_hedonics.csv`
- RDS hedonics files (except state CA): `/projects/Zillow_Housing/stores/Hedonics/rds_hedonics_by_states/<state>_hedonics.rds`
