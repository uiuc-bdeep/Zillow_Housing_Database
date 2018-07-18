# This file contains functions for data transfer between R and PostgreSQL database: get_from_db & send_to_db
# For more information about package PostgreSQL, see https://cran.r-project.org/web/packages/RPostgreSQL/RPostgreSQL.pdf

#' get_from_db
#' This function gets a list of data.frames from the database.
#' @param states_abbr   A vector of states abbreviation
#' @param columns       A vector of column names to export. Default to all columns (i.e. "*").
#' @param max_num_recs  An integer indicating the maximum number of records to select,
#'                      -1 indicating all (by default)
#' @param database_name A string indicating the database name
#' @param host_ip       A string indicating the ip address of the database VM
#' @param usr           A string indicating the user who can get access to the database on that VM
#' @param pwd           A string of the password to the database (instead of the user mentioned above)
#' @param schema_name   A string containing the schema name in the database
#' @return If input is a single state, return a single data.frame. If input is a vector, return a list
#'         of data.frames, in the same order of that in states_abbr.
#' @export
get_from_db <- function(states_abbr, columns="*", max_num_recs=-1, database_name="zillow_2017_nov",
                        host_ip="141.142.209.139", usr="postgres", pwd="bdeep", schema_name="hedonics_new"){
  # Gets database driver, assuming PostgreSQL database
  drv <- DBI::dbDriver("PostgreSQL")
  # Creates a connection to the postgres database
  # Note that "con" will be used later in each connection to the database
  con <- RPostgreSQL::dbConnect(drv,
                                dbname = database_name,
                                host = host_ip,
                                port = 5432,
                                user = usr,
                                password = pwd)
  # Get input length
  len <- length(states_abbr)
  # Create returned list
  hedonics <- list()
  # Get data from database
  for (i in 1:len){
    state <- states_abbr[i]
    # ignore non-existing states & check for the table
    if(!RPostgreSQL::dbExistsTable(con, c(schema_name, paste0(state, "_hedonics_new")))){
      print(paste("Skipping State:", state))
      next
    }
    print(paste("Processing State:", state))
    if(max_num_recs < 0){
      hedonics[[i]] <- RPostgreSQL::dbGetQuery(con, paste0("SELECT ",
                                                           paste(columns, collapse = ","),
                                                           " FROM ",
                                                           schema_name,
                                                           ".",
                                                           state,
                                                           "_hedonics_new"))
    } else {
      print("Not yet implemented!")
      # Close the connection
      RPostgreSQL::dbDisconnect(con)
      RPostgreSQL::dbUnloadDriver(drv)
      # Exit
      return(NULL)
    }
    # Garbage collection
    gc()
  }
  # Close the connection
  RPostgreSQL::dbDisconnect(con)
  RPostgreSQL::dbUnloadDriver(drv)
  # Return a list if more than one input states
  print("Finished!")
  if(len==1){
    return(hedonics[[1]])
  } else {
    return(hedonics)
  }
}


#' send_to_db
#' This function sends the given table to the database.
#' @param df            The actual data frame to send to the database
#' @param table_name    The table in the database to send to.
#' @param schema_name   The schema in the database to send to. Default to public schema.
#' @param database_name A string indicating the database name
#' @param host_ip       A string indicating the ip address of the database VM
#' @param usr           A string indicating the user who can get access to the database on that VM
#' @param pwd           A string of the password to the database (instead of the user mentioned above)
#' @param action        String in {"create", "append", "overwrite"}. "Create" only creates a new table when
#'                      no table with same name is available. "Append" appends the given table to the existing
#'                      one, and creates a new table if not existing. "Overwrite" overwrites the original table
#'                      if existing, and creates a new table if not. Default to "create".
#' @return TRUE on success, FALSE otherwise
#' @export
send_to_db <- function(df, table_name, schema_name="public", database_name="zillow_2017_nov", host_ip="141.142.209.139",
                       usr="postgres", pwd="bdeep", action="create"){
  # Assert that action is valid
  if(!action %in% c("create", "append", "overwrite")){
    print("Action must be one of create, append, or overwrite")
    return(FALSE)
  }
  # Initialize return value
  res <- FALSE
  # Gets database driver, assuming PostgreSQL database
  drv <- DBI::dbDriver("PostgreSQL")
  # Creates a connection to the postgres database
  # Note that "con" will be used later in each connection to the database
  con <- RPostgreSQL::dbConnect(drv,
                                dbname = database_name,
                                host = host_ip,
                                port = 5432,
                                user = usr,
                                password = pwd)
  # Check whether table exists
  if(!RPostgreSQL::dbExistsTable(con, c(schema_name, table_name))){
    # if not, create one anyway
    RPostgreSQL::dbWriteTable(con, c(schema_name, table_name), df, row.names = FALSE)
    res <- TRUE
  } else if(action == "create"){
    # nothing to do
    print("Target table already exists!")
  } else if(action == "append"){
    RPostgreSQL::dbWriteTable(con, c(schema_name, table_name), df, append = TRUE, row.names = FALSE)
    res <- TRUE
  } else if(action == "overwrite"){
    RPostgreSQL::dbWriteTable(con, c(schema_name, table_name), df, overwrite = TRUE, row.names = FALSE)
    res <- TRUE
  }
  # Garbage collection
  gc()
  # Close the connection
  RPostgreSQL::dbDisconnect(con)
  RPostgreSQL::dbUnloadDriver(drv)
  # Return
  return(res)
}
