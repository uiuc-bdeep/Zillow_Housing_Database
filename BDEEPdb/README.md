# BDEEPdb R package

## Installation
First, to install the package, execute the following in R terminal:
```
install.packages("devtools", "RPostgreSQL", "DBI")
devtools::install_github("uiuc-bdeep/Zillow_Housing_Database/BDEEPdb")
# In some cases, you might need to restart the server to get rid of all errors and warnings before loading library
library(BDEEPdb)
```

## Content
The current available data & functions are as followed. All of them have been tested on zillow_2017_nov database. Function `get_from_db_usr` can also be used in other databases.

### Data
* county_state_fips
```
@description A lookup table for state-county and fips conversion.
@usage       In function get_state_county.
@note        To explicitly see the table, use data(county_state_fips).
```
@description A lookup table for state-county and fips conversion.
@usage       In function get_state_county.
@note        To explicitly see the table, use data(county_state_fips).
* field_name

### Main functions

* get_from_db_fips
* get_from_db_state
* get_from_db_state_county
* get_from_db_usr

### Helper functions
* get_state_county
