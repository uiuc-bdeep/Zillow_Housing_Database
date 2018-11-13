# This file contains functions for data transfer between R and PostgreSQL infousa_2018 database
# For more information about package PostgreSQL, see https://cran.r-project.org/web/packages/RPostgreSQL/RPostgreSQL.pdf

#' get_from_db_state_city
#' @description This function gets a data.frame including all data from the given state abbr. and county.
#' @param state_city    A data.frame with two columns representing state and county name
#' @param columns       A vector of column names to export. Default to all columns (i.e. "*").
#' @param append        If append is true, return a single data.frame with rows appended, otherwise a
#'                      list of data.frames from each state.
#' @examples table <- data.frame(state=c("IL"),city=c("Chicago"))
#' @examples data <- get_infousa_state_city(table)
#' @return A data.frame including all data from the given state and county
#' @import RPostgreSQL DBI
#' @export
get_infousa_state_city <- function(state_city, year, columns="*", append=TRUE){
  # Check valid input
  if(nrow(state_city)==0 || ncol(state_city)!=2 || any(nchar(as.character(state_city[,1]))!=2)){
    print("Invalid argument! Please use this function as followed:")
    print("get_infousa_state_city(data.frame(state=c(\"IL\"),city=c(\"Chicago\")))")
    return(NULL)
  }
  # Initialize list for return
  hedonics <- list()
  # Initialize connection
  drv <- DBI::dbDriver("PostgreSQL")
  con <- RPostgreSQL::dbConnect(drv,
                                dbname = "infousa_2018",
                                host = "141.142.209.139",
                                port = 5432,
                                user = "postgres",
                                password = "bdeep")
  # Process state-county sequentially
  for(i in 1:nrow(state_city)){
    print(paste("Processing state", toupper(state_city[i, 1]), "city", state_city[i, 2]))
    hedonics[[i]] <- RPostgreSQL::dbGetQuery(con, paste0("SELECT ",
                                                         paste(columns, collapse = ","),
                                                         " FROM year", year, " WHERE \"STATE\"='",
                                                         toupper(state_city[i, 1]), "' AND \"CITY\"='",
                                                         toupper(state_city[i, 2]), "'"))
    gc()
  }
  # Close the connection
  RPostgreSQL::dbDisconnect(con)
  RPostgreSQL::dbUnloadDriver(drv)
  # Construct returned hedonics
  print("Finished!")
  if(append){
    return(do.call("rbind", hedonics))
  } else {
    return(hedonics)
  }
}