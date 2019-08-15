# This file contains functions for data transfer between R and PostgreSQL googlekml database on BDEEP cairo AWS vm.
# For more information about package PostgreSQL, see https://cran.r-project.org/web/packages/RPostgreSQL/RPostgreSQL.pdf

#' get_cairo_usr
#' @description This function gets from database according to a user-specified query.
#' @param query         A string specifying the query sent to database
#' @param database_name A string indicating the database name
#' @param host_ip       A string indicating the ip address of the database VM
#' @examples # Select single field from a given table
#' @examples data <- get_from_db_usr("SELECT COUNT(*) FROM routes")
#' @examples  
#' @examples See descriptions of get_from_db_usr for more details on possible queries
#' @return A data.frame returned by the given query.
#' @import RPostgreSQL DBI
#' @export
get_cairo_usr <- function(query, database_name="googlekml", host_ip="3.90.230.196"){
  # Only one query at a time is supported
  if(length(query)>1){
    print("Only one query at a time is supported!")
    return(NULL)
  }
  # Initialize connection
  drv <- DBI::dbDriver("PostgreSQL")
  con <- RPostgreSQL::dbConnect(drv,
                                dbname = database_name,
                                host = host_ip,
                                port = 5432,
                                user = "postgres",
                                password = "bdeep")
  # Get data
  options(warn = -1)    # suppress warning messages
  res <- RPostgreSQL::dbGetQuery(con, query)
  options(warn = 0)
  gc()
  # Close the connection
  RPostgreSQL::dbDisconnect(con)
  RPostgreSQL::dbUnloadDriver(drv)
  return(db_type_converter(res, dbname=database_name))
}

