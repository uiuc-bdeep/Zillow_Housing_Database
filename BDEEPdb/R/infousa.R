# This file contains functions for data transfer between R and PostgreSQL infousa_2018 database
# For more information about package PostgreSQL, see https://cran.r-project.org/web/packages/RPostgreSQL/RPostgreSQL.pdf

#' get_infousa_location
#' @description This function gets a data.frame including all data from the given location in a single year.
#' @param single_year   An integer indicating a single year
#' @param fips          A vector of integers indicating fips.
#'                      If the entire state is wanted, set county code to 000.
#' @param tract         A vector of integers or chars indicating tract in the county.
#'                      Note that tracts are unique only in the current county. Default to all tracts.
#' @param columns       A vector of column names to export. Default to all columns (i.e. "*").
#' @param append        If append is true, return a single data.frame with rows appended, otherwise a
#'                      list of data.frames from each state.
#' @examples  test <- get_infousa_location(2006, "01001")
#' @examples  test <- get_infousa_location(2006, "02020", tract=c(001802,002900))
#' @return A data.frame including all data from the given year, fips and tract
#' @import RPostgreSQL DBI
#' @export
get_infousa_location <- function(single_year, fips, tract="*", columns="*", append=TRUE){
  # Check valid input
  if(any(nchar(fips)!=5)){
    print("Invalid fips codes! Please enter fips code as characters.")
    return(NULL)
  }
  state_county <- get_state_county(fips)[, c("state", "county", "county_code")]
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
    print(paste("Processing STATE:", toupper(state_county[i, 1]),
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
#' @param fips          A vector of integers indicating fips.
#'                      If the entire state is wanted, set county code to 000.
#' @param tract         A vector of integers or chars indicating tract in the county.
#'                      Note that tracts are unique only in the current county. Default to all tracts.
#' @param columns       A vector of column names to export. Default to all columns (i.e. "*").
#' @examples  test <- get_infousa_location("01001")
#' @examples  test <- get_infousa_location("02020", tract=c(001802,002900))
#' @return A data.frame including data from all years, fips and tract
#' @import RPostgreSQL DBI
#' @export
get_infousa_multiyear <- function(fips, tract="*", columns="*", append=TRUE){
  res <- get_infousa_location(2006, fips, tract=tract, columns=columns)
  res$year <- 2006
  for (yr in 2007:2017){
    temp <- get_infousa_location(yr, fips, tract=tract, columns=columns)
    temp$year <- yr
    res <- rbind(res, temp)
  }
  return(res)
}
