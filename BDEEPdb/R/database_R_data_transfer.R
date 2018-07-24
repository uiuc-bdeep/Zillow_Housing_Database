# This file contains functions for data transfer between R and PostgreSQL database: get_from_db & send_to_db
# For more information about package PostgreSQL, see https://cran.r-project.org/web/packages/RPostgreSQL/RPostgreSQL.pdf

#' get_from_db
#' @description This function gets a list of data.frames from the database.
#' @param states_abbr   A vector of states abbreviation
#' @param columns       A vector of column names to export. Default to all columns (i.e. "*").
#' @param max_num_recs  An integer indicating the maximum number of records to select,
#'                      -1 indicating all (by default)
#' @param database_name A string indicating the database name
#' @param host_ip       A string indicating the ip address of the database VM
#' @param append        If append is true, return a single data.frame with rows appended.
#' @return If input is a single state, return a single data.frame. If input is a vector, return a list
#'         of data.frames, in the same order of that in states_abbr.
#' @import RPostgreSQL DBI
#' @export
get_from_db <- function(states_abbr, columns="*", max_num_recs=-1, database_name="zillow_2017_nov",
                        host_ip="141.142.209.139", append=FALSE){
  # Make sure states_abbr are lower cased
  states_abbr <- tolower(states_abbr)
  # Gets database driver, assuming PostgreSQL database
  drv <- DBI::dbDriver("PostgreSQL")
  # Creates a connection to the postgres database
  # Note that "con" will be used later in each connection to the database
  con <- RPostgreSQL::dbConnect(drv,
                                dbname = database_name,
                                host = host_ip,
                                port = 5432,
                                user = "postgres",
                                password = "bdeep")
  # Get input length
  len <- length(states_abbr)
  # Create returned list
  hedonics <- list()
  # Get data from database
  for (i in 1:len){
    state <- states_abbr[i]
    # ignore non-existing states & check for the table
    if(!RPostgreSQL::dbExistsTable(con, c("hedonics_new", paste0(state, "_hedonics_new")))){
      print(paste("Skipping State:", state))
      next
    }
    print(paste("Processing State:", state))
    if(max_num_recs < 0){
      hedonics[[i]] <- RPostgreSQL::dbGetQuery(con, paste0("SELECT ",
                                                           paste(columns, collapse = ","),
                                                           " FROM hedonics_new.",
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
  } else if(append){
    return(do.call("rbind", hedonics))
  } else{
    return(hedonics)
  }
}

#' get_from_db_state_county
#' @description This function gets a data.frame including all data from the given state abbr. and county.
#' @param state_county  A data.frame with two columns representing state and county name
#' @param columns       A vector of column names to export. Default to all columns (i.e. "*").
#' @param database_name A string indicating the database name
#' @param host_ip       A string indicating the ip address of the database VM
#' @return A data.frame including all data from the given state and county
#' @import RPostgreSQL DBI
#' @export
get_from_db_state_county <- function(state_county, columns="*", database_name="zillow_2017_nov",
                                     host_ip="141.142.209.139"){
  # Check valid input
  if(nrows(state_county)==0 || ncols(states_county)!=2 || any(nchar(state_county[,1])!=2)){
    print("Invalid argument!")
    return(NULL)
  }
  # Initialize list for return
  hedonics <- list()
  # Initialize connection
  drv <- DBI::dbDriver("PostgreSQL")
  con <- RPostgreSQL::dbConnect(drv,
                                dbname = database_name,
                                host = host_ip,
                                port = 5432,
                                user = "postgres",
                                password = "bdeep")
  # Process state-county sequentially
  for(i in 1:nrows(state_county)){
    r <- state_county[i, ]
    hedonics[[i]] <- RPostgreSQL::dbGetQuery(con, paste0("SELECT ",
                                                         paste(columns, collapse = ","),
                                                         " FROM hedonics_new.",
                                                         tolower(r[1]),
                                                         "_hedonics_new WHERE county='",
                                                         toupper(r[2]),
                                                         "'"))
    gc()
  }
  # Close the connection
  RPostgreSQL::dbDisconnect(con)
  RPostgreSQL::dbUnloadDriver(drv)
  # Construct returned hedonics
  print("Finished!")
  return(do.call("rbind", hedonics))
}

#' get_from_db_fips
#' @description This function gets a data.frame including all data from the given fips. Use previous
#'              function.
#' @param fips          A vector of fips
#' @param columns       A vector of column names to export. Default to all columns (i.e. "*").
#' @param database_name A string indicating the database name
#' @param host_ip       A string indicating the ip address of the database VM
#' @return A data.frame including all data from the given fips.
#' @import RPostgreSQL DBI
#' @export
get_from_db_fips <- function(fips, columns="*", database_name="zillow_2017_nov",
                             host_ip="141.142.209.139"){
  sc <- get_state_county(fips)
  return(get_from_db_state_county(sc,
                                  columns=columns,
                                  database_name=database_name,
                                  host_ip=host_ip))
}

#' send_to_db
#' @description This function sends the given table to the database.
#' @param df            The actual data frame to send to the database
#' @param table_name    The table in the database to send to.
#' @param schema_name   The schema in the database to send to. Default to public schema.
#' @param database_name A string indicating the database name
#' @param host_ip       A string indicating the ip address of the database VM
#' @param action        String in {"create", "append", "overwrite"}. "Create" only creates a new table when
#'                      no table with same name is available. "Append" appends the given table to the existing
#'                      one, and creates a new table if not existing. "Overwrite" overwrites the original table
#'                      if existing, and creates a new table if not. Default to "create".
#' @return TRUE on success, FALSE otherwise
#' @import RPostgreSQL DBI
#' @export
send_to_db <- function(df, table_name, schema_name="public", database_name="zillow_2017_nov",
                       host_ip="141.142.209.139", action="create"){
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
                                user = "postgres",
                                password = "bdeep")
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

####################################### Helper Functions below ####################################
#' get_state_county
#' @description This function convert a vector of fips to corresponding state-county pairs
#' @param fips A vector of fips numbers stored as TEXT (characters)
#' @return A data.frame with first column as state abbreviation and second column as county name,
#'         all in lower case.
#' @export
get_state_county <- function(fips){
  county_state_fips <- data("./data/county_state_fips.rda")
  ret <- setNames(data.frame(matrix(ncol = 2, nrow = 0)), c("state", "county"))
  for (n in fips){
    # Ensure correct format
    if(nchar(n)!=5 || n < "01001" || n > "56045"){
      print(paste("FIPS code invalid format:", n))
      next
    }
    # Find row
    r <- county_state_fips[which(county_state_fips$fips == n), ]
    if(nrow(r)!=1){
      print(paste("FIPS code not found:", n))
    }
    # Append
    ret <- rbind(ret, data.frame(state=r$st_abbr, county=r$ct_name))
  }
  return(ret)
}
