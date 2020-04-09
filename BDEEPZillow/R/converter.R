# This file contains a function for data type converter in transfers from database to R

#' db_type_converter
#' @description This function converts the type to align with the requirement. See requirement online.
#' @param data    The actual data.frame to convert.
#' @param dbname  The name of the database. Used to distinguish data.
#' @return The modified data.frame
#' @examples # data is the output from any get_from_db function
#' @examples data <- get_from_db_usr("SELECT loadid FROM hedonics_new.sd_hedonics_new")
#' @examples # convert the type of the columns
#' @examples data <- db_type_converter(data)
#' @examples # ... or you can specify the database
#' @examples data <- db_type_converter(data, dbname = "zillow_2017_nov")
#' @export
db_type_converter <- function(data, dbname){
  # Change data type for Zillow_Housing data
  if(dbname == 'zillow_2017_nov'){
    # Get columns
    cols <- colnames(data)
    # importparcelid
    if("importparcelid" %in% cols) data$importparcelid <- as.integer(data$importparcelid)
    # fips
    if("fips" %in% cols) data$fips <- as.integer(data$fips)
    # propertyzip
    if("propertyzip" %in% cols) data$propertyzip <- as.integer(data$propertyzip)
    # propertyaddresscensustractandblock
    if("propertyaddresscensustractandblock" %in% cols)
      data$propertyaddresscensustractandblock <- as.double(data$propertyaddresscensustractandblock)
    # taxamount
    if("taxamount" %in% cols)
      data$taxamount <- as.double(gsub("[\\$,]", "", data$taxamount))
    # noofstories
    if("noofstories" %in% cols) data$noofstories <- as.double(data$noofstories)
    # transid
    if("transid" %in% cols) data$transid <- as.integer(data$transid)
    # recordingdate
    if("recordingdate" %in% cols) data$recordingdate <- as.character(data$recordingdate)
    # documentdate
    if("documentdate" %in% cols) data$documentdate <- as.character(data$documentdate)
    # signaturedate
    if("signaturedate" %in% cols) data$signaturedate <- as.character(data$signaturedate)
    # salespriceamount
    if("salespriceamount" %in% cols)
      data$salespriceamount <- as.double(gsub("[\\$,]", "", data$salespriceamount))
  }
  return(data)
}
