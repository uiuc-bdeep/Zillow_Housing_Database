require(RPostgreSQL)
require(BDEEPdb)
# source("/home/bdeep/share/projects/Zillow_Housing/scripts/Generate/Generate.R")

# Define global constants
NUMBER_OF_TESTS <- 3
test_columns <- paste(c("loadid", "fips", "rowid"), collapse = ",")

# Generate the query for test, same as in BDEEPdb package
gen_query <- function(tnum, st){
  switch(tnum,
         return(paste0("SELECT loadid FROM hedonics_new.", st, "_hedonics_new;")),
         return(paste0("SELECT ", test_columns, " FROM hedonics_new.", st, "_hedonics_new;")),
         return(paste0("SELECT * FROM hedonics_new.", st, "_hedonics_new FETCH FIRST 100 ROWS ONLY;"))
        )
}

# Run a single test specified by the test number
run_single_test <- function(tnum, st){
  print(paste("Running test", tnum))
  # Read from txt file
  txtdata <- readRDS("/home/schadri/testHedonics/SDHedonics.rds")
  # Read table from database
  dbdata <- get_from_db_usr(gen_query(tnum, st))
  
}

run_all_tests <- function(st){
  
}

# Get test information
state <- as.character(readline(prompt = "Enter state: "))
test_num <- as.integer(readline(prompt = "Enter test number: "))
if(test_num < 0 || test_num > NUMBER_OF_TESTS){
  print("Test Methods:")
  print("1: Select one column")
  print("2: Select multiple columns")
  print("3: Select first 100 rows with all columns")
  stop()
}

# Switch to different tests
if(test_num > 0){
  run_single_test(test_num, state)
} else {
  run_all_tests(state)
}
