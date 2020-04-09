# Generate the Hedonics

There are 3 ways to generate the final zillow hedonics data.

## Generate.R
Historically, one uses `Generate.R` to read from txt files and output a final hedonics. This avoids any usage of database and is faster for a one-shot operation.

## zillow_extract_hedonics.py
One can also use [zillow_extract_hedonics.py](./zillow_extract_hedonics.py) to convert the **output from the zillow_txt_to_database.py** to the final hedonics. If one still wants the raw data, use:
```
python3 zillow_extract_hedonics.py 01
```
Otherwise, if the intermediate data (i.e. zasmt & ztrans data) are no longer needed, add `delete` prompt at the end. This will delete the files created by the first script at last:
```
python3 zillow_extract_hedonics.py 01 delete
```

Overall, this method requires that the data is already stored in the database. It is significantly faster for a user who constantly refers to the hedonics data.

## Direct manipulation in PostgreSQL
This is the most flexible way to generate the desired hedonics file. One might want to use this method if he/she is familiar with SQL commands.
