# This file contains functions for data transfer between R and PostgreSQL infousa_2018 database
# For more information about package PostgreSQL, see https://cran.r-project.org/web/packages/RPostgreSQL/RPostgreSQL.pdf

#' get_infousa_location
#' @description This function gets a data.frame including all data from the given location in a single year.
#' @param single_year   An integer indicating a single year
#' @param loc           A vector of integers indicating fips ("fips" method) or a data.frame with first 2
#'                      columns being state abbr. & county name ("names" method).
#'                      If the entire state is needed, set county code to 000 ("fips") or set county name to "*" ("names").
#' @param tract         A vector of integers or chars indicating tract in the county.
#'                      Note that tracts are unique only in the current county. Default to all tracts.
#' @param columns       A vector of column names to export. Default to all columns (i.e. "*").
#' @param method        Method for input. Choose between "fips" and "names". Default to "fips".
#' @param append        If append is true, return a single data.frame with rows appended, otherwise a
#'                      list of data.frames from each state.
#' @examples  # Using fips
#' @examples  test <- get_infousa_location(2006, "01001")
#' @examples  test <- get_infousa_location(2006, "02020", tract=c(001802,002900))
#' @examples   
#' @examples  # Using state and county names
#' @examples  x <- data.frame(state=c('il'), county=c('champaign'))
#' @examples  test <- get_infousa_location(2017, x, method="names")
#' @return A data.frame including all data from the given year, fips and tract
#' @import RPostgreSQL DBI
#' @export
get_infousa_location <- function(single_year, loc, tract="*", columns="*", method="fips", append=TRUE){
  # Check valid input
  if (method=="fips") {
    if (any(nchar(loc)!=5)){
      print("Invalid fips codes! Try entering fips code as characters.")
      return(NULL)
    }
    state_county <- get_state_county_by_fips(loc)[, c("state", "county", "county_code")]
  } else if (method=="names"){
    if (ncol(loc)!=2){
      print("Invalid input data.frame!")
      return(NULL)
    }
    state_county <- get_state_county_by_names(loc)[, c("state", "county", "county_code")]
  } else {
    print("Invalid method!")
    return(NULL)
  }
  
  # state_county$county_code <- as.integer(state_county$county_code)

  # Initialize tract specification
  if(length(tract)>1 || tract[1]!="*"){
    if(nrow(state_county) > 1){
      print("WARNING: Tracts are unique only in one county!")
    }
    tract_spec <- paste0("(\"CENSUS2010TRACT\"=", paste0(tract, collapse = " OR \"CENSUS2010TRACT\"="), ")")
  }
  
  # Initialize list for return
  result <- list()

  # Initialize connection
  drv <- DBI::dbDriver("PostgreSQL")
  con <- RPostgreSQL::dbConnect(drv,
                                dbname = "infousa_2018",
                                host = "141.142.209.139",
                                port = 5432,
                                user = "postgres",
                                password = "bdeep")

  # Process state-county sequentially
  for(i in 1:nrow(state_county)){
    print(paste("Processing YEAR:", single_year,
                "STATE:", toupper(state_county[i, 1]),
                "COUNTY:", state_county[i, 2],
                "CENSUS2010TRACT:", paste0(tract, collapse = ", ")))
    if(state_county[i, 3] == 0){
      result[[i]] <- RPostgreSQL::dbGetQuery(con, paste0("SELECT ",
                                                           paste(columns, collapse = ","),
                                                           " FROM year", single_year, "part.", state_county[i, 1]))
    } else if (length(tract)==1 && tract=="*") {
      result[[i]] <- RPostgreSQL::dbGetQuery(con, paste0("SELECT ",
                                                           paste(columns, collapse = ","),
                                                           " FROM year", single_year, "part.", state_county[i, 1],
                                                           " WHERE \"CENSUS2010COUNTYCODE\"=", state_county[i, 3]))
    } else {
      result[[i]] <- RPostgreSQL::dbGetQuery(con, paste0("SELECT ",
                                                           paste(columns, collapse = ","),
                                                           " FROM year", single_year, "part.", state_county[i, 1],
                                                           " WHERE \"CENSUS2010COUNTYCODE\"=", state_county[i, 3],
                                                           " AND ", tract_spec))
    }
    gc()
  }
  # close the connection
  RPostgreSQL::dbDisconnect(con)
  RPostgreSQL::dbUnloadDriver(drv)

  # Construct final result
  print("Finished!")
  if(append){
    return(do.call("rbind", result))
  } else {
    return(result)
  }
}


#' get_infousa_multiyear
#' @description This function gets a data.frame including all data from the given location from all years.
#' @param startyear     The first year to get data
#' @param endyear       The last year to get data
#' @param loc           A vector of integers indicating fips ("fips" method) or a data.frame with first 2
#'                      columns being state abbr. & county name ("names" method).
#'                      If the entire state is needed, set county code to 000 ("fips") or set county name to "*" ("names").
#' @param tract         A vector of integers or chars indicating tract in the county.
#'                      Note that tracts are unique only in the current county. Default to all tracts.
#' @param columns       A vector of column names to export. Default to all columns (i.e. "*").
#' @param method        Method for input. Choose between "fips" and "names". Default to "fips".
#' @examples  test <- get_infousa_multiyear(2017, 2017, "01001")
#' @examples  test <- get_infousa_multiyear(2006, 2016, "02020", tract=c(001802,002900))
#' @return A data.frame including data from all years, fips and tract
#' @import RPostgreSQL DBI
#' @export
get_infousa_multiyear <- function(startyear, endyear, loc, tract="*", columns="*", method="fips"){
  # Check valid input
  if(startyear < 2006 || startyear > 2017 || endyear < 2006 || endyear > 2017 || endyear < startyear){
    print("Invalid year range! Please ensure startyear <= endyear and both in [2006,2017].")
    return(NULL)
  }
  if (method=="fips") {
    if (any(nchar(loc)!=5)){
      print("Invalid fips codes! Try entering fips code as characters.")
      return(NULL)
    }
    state_county <- get_state_county_by_fips(loc)[, c("state", "county", "county_code")]
  } else if (method=="names"){
    if (ncol(loc)!=2){
      print("Invalid input data.frame!")
      return(NULL)
    }
    state_county <- get_state_county_by_names(loc)[, c("state", "county", "county_code")]
  } else {
    print("Invalid method!")
    return(NULL)
  }
  # state_county$county_code <- as.integer(state_county$county_code)
  
  # Initialize tract specification
  if(length(tract)>1 || tract[1]!="*"){
    if(nrow(state_county) > 1){
      print("WARNING: Tracts are unique only in one county!")
    }
    tract_spec <- paste0("(\"CENSUS2010TRACT\"=", paste0(tract, collapse = " OR \"CENSUS2010TRACT\"="), ")")
  }

  # Initialize connection
  drv <- DBI::dbDriver("PostgreSQL")
  con <- RPostgreSQL::dbConnect(drv,
                                dbname = "infousa_2018",
                                host = "141.142.209.139",
                                port = 5432,
                                user = "postgres",
                                password = "bdeep")
  
  first <- TRUE
  # Iterate over years
  for(yr in startyear:endyear){
    # Process state-county sequentially
    for(i in 1:nrow(state_county)){
      print(paste("Processing YEAR:", yr,
                  "STATE:", toupper(state_county[i, 1]),
                  "COUNTY:", state_county[i, 2],
                  "CENSUS2010TRACT:", paste0(tract, collapse = ", ")))
      if(state_county[i, 3] == 0){
        res_oneyear <- RPostgreSQL::dbGetQuery(con, paste0("SELECT ",
                                                                paste(columns, collapse = ","),
                                                                " FROM year", yr, "part.", state_county[i, 1]))
      } else if (length(tract)==1 && tract=="*") {
        res_oneyear <- RPostgreSQL::dbGetQuery(con, paste0("SELECT ",
                                                           paste(columns, collapse = ","),
                                                           " FROM year", yr, "part.", state_county[i, 1],
                                                           " WHERE \"CENSUS2010COUNTYCODE\"=", state_county[i, 3]))
      } else {
        res_oneyear <- RPostgreSQL::dbGetQuery(con, paste0("SELECT ",
                                                           paste(columns, collapse = ","),
                                                           " FROM year", yr, "part.", state_county[i, 1],
                                                           " WHERE \"CENSUS2010COUNTYCODE\"=", state_county[i, 3],
                                                           " AND ", tract_spec))
      }
      if(nrow(res_oneyear)>0){
        res_oneyear$"YEAR" <- yr
      }
      gc()
    }
    # Append to final result list
    if(nrow(res_oneyear)>0){
      if(first){
        res <- res_oneyear
        first <- FALSE
      } else {
        res <- rbind(res, res_oneyear)
      }
    }
  }
 
  # close the connection
  RPostgreSQL::dbDisconnect(con)
  RPostgreSQL::dbUnloadDriver(drv)
  print("Finished!")

  return(res)
}


#' get_infousa_zip
#' @description This function gets a data.frame including all data from the given location from all years.
#' @param startyear     The first year to get data
#' @param endyear       The last year to get data
#' @param zip           A vector of characters indicating zipcodes to get data from.
#' @param columns       A vector of column names to export. Default to all columns (i.e. "*").
#' @examples  test <- get_infousa_zip(2016, 2017, c("61801", "61820"))
#' @return A data.frame including data from all years, fips and tract
#' @import RPostgreSQL DBI
#' @export
get_infousa_zip <- function(startyear, endyear, zip, columns="*"){
  # Check valid input
  if(startyear < 2006 || startyear > 2017 || endyear < 2006 || endyear > 2017 || endyear < startyear){
    print("Invalid year range! Please ensure startyear <= endyear and both in [2006,2017].")
    return(NULL)
  }
  if (any(nchar(zip)!=5)){
    print("Invalid fips codes! Try entering fips code as characters.")
    return(NULL)
  }
  sc_zip <- get_state_city_zipcode(zip)[, c("state", "zip", "city")]
  
  # Initialize connection
  drv <- DBI::dbDriver("PostgreSQL")
  con <- RPostgreSQL::dbConnect(drv,
                                dbname = "infousa_2018",
                                host = "141.142.209.139",
                                port = 5432,
                                user = "postgres",
                                password = "bdeep")
  
  first <- TRUE
  # Iterate over years
  for(yr in startyear:endyear){
    # Process state-county sequentially
    for(i in 1:nrow(sc_zip)){
      print(paste("Processing YEAR:", yr,
                  "STATE:", toupper(sc_zip[i, 1]),
                  "CITY:", sc_zip[i, 3],
                  "ZIPCODE:", sc_zip[i, 2]))
      res_oneyear <- RPostgreSQL::dbGetQuery(con, paste0("SELECT ",
                                                         paste(columns, collapse = ","),
                                                         " FROM year", yr, "part.", sc_zip[i, 1],
                                                         " WHERE \"ZIP \"=", sc_zip[i, 2]))
      if(nrow(res_oneyear)>0){
        res_oneyear$"YEAR" <- yr
      }
      gc()
    }
    # Append to final result list
    if(nrow(res_oneyear)>0){
      if(first){
        res <- res_oneyear
        first <- FALSE
      } else {
        res <- rbind(res, res_oneyear)
      }
    }
  }
  
  # close the connection
  RPostgreSQL::dbDisconnect(con)
  RPostgreSQL::dbUnloadDriver(drv)
  print("Finished!")
  
  return(res)
}

#' get_infousa_fid
#' @description This function gets a data.frame including all data from the given vector of familyid from given years.
#' @param startyear     The first year to get data
#' @param endyear       The last year to get data
#' @param fid           A vector of characters indicating familyids to get data from.
#' @param columns       A vector of column names to export. Default to all columns (i.e. "*").
#' @examples  test <- get_infousa_fid(2006, 2007, c("54299524", "54320129"))
#' @return A data.frame
#' @import RPostgreSQL DBI
#' @export
get_infousa_fid <- function(startyear, endyear, fid, columns="*"){
  # Check valid input
  if(startyear < 2006 || startyear > 2017 || endyear < 2006 || endyear > 2017 || endyear < startyear){
    print("Invalid year range! Please ensure startyear <= endyear and both in [2006,2017].")
    return(NULL)
  }
  
  # Create placeholder
  fid <- as.integer(fid)
  check <- rep(FALSE, length(fid))
  
  # Initialize connection
  drv <- DBI::dbDriver("PostgreSQL")
  con <- RPostgreSQL::dbConnect(drv,
                                dbname = "infousa_2018",
                                host = "141.142.209.139",
                                port = 5432,
                                user = "postgres",
                                password = "bdeep")

  first <- TRUE
  # Process each split
  for(s in split_points){
    # Find one-year data
    partition <- fid > s & fid < (s+199999999)
    if(all(!partition)){
      next
    }
    part_spec <- paste0("(\"FAMILYID\"=", paste0(fid[which(partition)], collapse = " OR \"FAMILYID\"="),")")
    # Iterate over years
    for(yr in startyear:endyear){
      print(paste("Processing YEAR:", yr, "FID RANGE:", s, " TO ", (s+199999999)))
      # Get data
      res_oneyear <- RPostgreSQL::dbGetQuery(con, paste0("SELECT ",
                                                         paste(columns, collapse = ","),
                                                         " FROM year", yr, "fid.\"", s,
                                                         "\" WHERE ", part_spec))
      if(nrow(res_oneyear)>0){
        res_oneyear$"YEAR" <- yr
      }
      gc()
      # Append to final result list
      if(nrow(res_oneyear)>0){
        if(first){
          res <- res_oneyear
          first <- FALSE
        } else {
          res <- rbind(res, res_oneyear)
        }
      }
    }
    check <- check | partition
  }
  
  # Check else
  check <- !check
  if(any(check) && endyear >= 2017){
    part_spec <- paste0("(\"FAMILYID\"=", paste0(fid[which(check)], collapse = " OR \"FAMILYID\"="),")")
    res_oneyear <- RPostgreSQL::dbGetQuery(con, paste0("SELECT ",
                                                       paste(columns, collapse = ","),
                                                       " FROM year2017fid.\"else\" WHERE ", part_spec))
    if(nrow(res_oneyear)>0){
      res_oneyear$"YEAR" <- 2017
    }
    gc()
    # Append to final result list
    if(nrow(res_oneyear)>0){
      if(first){
        res <- res_oneyear
        first <- FALSE
      } else {
        res <- rbind(res, res_oneyear)
      }
    }
  }
  
  
  # close the connection
  RPostgreSQL::dbDisconnect(con)
  RPostgreSQL::dbUnloadDriver(drv)
  print("Finished!")
  
  return(res)
}
