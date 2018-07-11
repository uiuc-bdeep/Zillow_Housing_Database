#install.packages("RPostgreSQL")
# For information, see https://cran.r-project.org/web/packages/RPostgreSQL/RPostgreSQL.pdf
require("RPostgreSQL")
# list of state abbreviations
# abbr <- tolower(c("AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL",
#                   "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
#                   "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
#                   "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
#                   "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI",
#                   "WY", "AS", "GU", "MP", "PR", "VI", "UM"))
# abbr <- c("tx", "fl", "ca")
abbr <- c("sd")
# create a connection
# loads the PostgreSQL driver
drv <- dbDriver("PostgreSQL")
# creates a connection to the postgres database
# note that "con" will be used later in each connection to the database
con <- dbConnect(drv, dbname = "zillow_2017_nov",
                 host = "141.142.209.139",
                 port = 5432,
                 user = "postgres",
                 password = "bdeep")

for (state in abbr){
  # ignore non-existing states & check for the table
  if(!dbExistsTable(con, c("hedonics_new", paste0(state, "_hedonics_new")))){
    print(paste("Skipping State:", state))
    next
  }
  print(paste("Processing State:", state))
  hedonics <- dbGetQuery(con, paste0("SELECT * FROM hedonics_new.", state, "_hedonics_new"))
  # saveRDS(hedonics, paste0('/home/bdeep/share/projects/Zillow_Housing/stores/Hedonics/', state, '_hedonics_test.rds'))
  # rm(hedonics)
  # gc()
}

# close the connection
dbDisconnect(con)
dbUnloadDriver(drv)
