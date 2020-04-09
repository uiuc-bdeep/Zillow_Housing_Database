## ---- include = FALSE----------------------------------------------------
knitr::opts_chunk$set(
  collapse = TRUE,
  comment = "#>"
)

## ----setup---------------------------------------------------------------
library(BDEEPdbZillow)

## ------------------------------------------------------------------------
data <- get_from_db_fips("10001")
head(data, n = 5L)
data <- get_from_db_fips(c("01001","17019"))
head(data, n = 5L)

## ------------------------------------------------------------------------
data <- get_from_db_state("sd")
head(data, n = 5L)
data <- get_from_db_state("ca",columns=c("rowid","transid"))
head(data, n = 5L)
data <- get_from_db_state("sd",max_num_recs=100)
head(data, n = 5L)

## ------------------------------------------------------------------------
table <- get_state_county_by_fips("01001")[, c("state","county")]
data <- get_from_db_state_county(table)
head(data, n = 5L)

## ------------------------------------------------------------------------
data <- get_from_db_usr("SELECT loadid FROM hedonics_new.sd_hedonics_new")
head(data, n = 5L)

