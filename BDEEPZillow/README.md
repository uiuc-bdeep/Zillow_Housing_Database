# BDEEPZillow R package

## Installation
First, to install the package dependencies, execute the following in R terminal:
```
install.packages("devtools", dependencies=T)
install.packages("DBI")
install.packages("RPostgreSQL")
```

Finally, to install (only for the first time) or update the package, use:
```
devtools::install_github("uiuc-bdeep/Zillow_Housing_Database/BDEEPZillow")
```

In some cases, you might need to restart the server to get rid of all errors and warnings before loading the library. After this, use command to load the BDEEPZillow library.
```
library(BDEEPZillow)
```

## Content
The current available data & functions are as followed. All of them have been tested on zillow_2017_nov database. Function `get_from_db_usr` can also be used in other databases.

### Data
* [county_state_fips](county_state_fips_table.R)
```
@description A lookup table for state-county and fips conversion.
@usage       In function get_state_county.
@note        To explicitly see the table, use data(county_state_fips).
```
* [field_name](./R/field_name_list.R)
```
@description An array that contains all column names in the hedonics.
@note        To explicitly see the array, use data(field_name).
```

### Main functions
* [get_from_db_fips](./R/zillow.R)
```
@description This function gets a data.frame including all data from the given fips. Use previous
             function.
@param fips          A vector of fips
@param columns       A vector of column names to export. Default to all columns (i.e. "*").
@param database_name A string indicating the database name
@param host_ip       A string indicating the ip address of the database VM
@param append        If append is true, return a single data.frame with rows appended, otherwise a
                     list of data.frames from each state.
@examples data <- get_from_db_fips("10001")
@examples data <- get_from_db_fips(c("01001","17019"))
@return A data.frame including all data from the given fips.
```
* [get_from_db_state](./R/zillow.R)
```
@description This function gets a list of data.frames from the database, according to the given
             states (through abbreviation).
@param states_abbr   A vector of states abbreviation
@param columns       A vector of column names to export. Default to all columns (i.e. "*").
@param max_num_recs  An integer indicating the maximum number of records to select,
                     -1 indicating all (by default)
@param database_name A string indicating the database name
@param host_ip       A string indicating the ip address of the database VM
@param append        If append is true, return a single data.frame with rows appended, otherwise a
                     list of data.frames from each state.
@examples  data <- get_from_db_state("sd")
@examples  data <- get_from_db_state(c("sD","Ne"), append=FALSE)
@examples  data <- get_from_db_state("ca",columns=c("rowid","transid"))
@examples  data <- get_from_db_state("sd",max_num_recs=100)
@return If input is a single state, return a single data.frame. If input is a vector and append = F,
        return a list of data.frames, in the same order of that in states_abbr, otherwise the result
        is appended into one whole data.frame.
```
* [get_from_db_state_county](./R/zillow.R)
```
@description This function gets a data.frame including all data from the given state abbr. and county.
@param state_county  A data.frame with two columns representing state and county name
@param columns       A vector of column names to export. Default to all columns (i.e. "*").
@param database_name A string indicating the database name
@param host_ip       A string indicating the ip address of the database VM
@param append        If append is true, return a single data.frame with rows appended, otherwise a
                     list of data.frames from each state.
@examples table <- get_state_county("01001")[, c("state","county")]
@examples data <- get_from_db_state_county(table)
@return A data.frame including all data from the given state and county
```
* [get_from_db_usr](./R/zillow.R)
```
@description This function gets from database according to a user-specified query.
@param query         A string specifying the query sent to database
@param database_name A string indicating the database name
@param host_ip       A string indicating the ip address of the database VM
@examples data <- get_from_db_usr("SELECT loadid FROM hedonics_new.sd_hedonics_new")
@return A data.frame returned by the given query.
```

### Helper functions
* [get_state_county](./R/county_state_fips_table.R)
```
@description This function convert a vector of fips to corresponding state-county pairs
@param fips  A vector of fips numbers stored as TEXT (characters)
@return A data.frame with first column as state abbreviation, second column as county name,
        both in lower case, and third column fips.
```

* [db_type_converter](./R/converter.R)
```
@description This function converts the type to align with the requirement. See requirement online.
@param data    The actual data.frame to convert.
@param dbname  The name of the database. Used to distinguish data.
@return The modified data.frame
```
## Notes
### Data Type
For the Zillow_Housing data, please refer to [this table](./Zillow_Housing_Type_Info.pdf) to check for type compatibility for each column.
