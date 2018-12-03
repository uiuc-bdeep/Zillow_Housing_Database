#' county_state_fips
#' @docType data
#'
#' @usage data(county_state_fips)
#'
#' @format A data.frame with 6 variables indicating state abbr., state name, county name,
#'         state fips, county fips, and total fips
#'
#' @keywords fips
#'
#' @references \url{www.mdreducation.com/pdfs/US_FIPS_Codes.xls}
"county_state_fips"

#' get_state_county_by_fips
#' @description This function convert a vector of fips to corresponding state-county pairs
#' @param fips A vector of fips numbers
#' @return A data.frame with first column as state abbreviation, second column as county name,
#'         third column county_code, and last column fips.
#' @export
get_state_county_by_fips <- function(fips){
  ret <- setNames(data.frame(matrix(ncol = 4, nrow = 0)), c("state", "county", "county_code", "fips"))
  for (n in fips){
    # Ensure correct format
    if(nchar(n)!=5 || n < "01000" || n > "56045"){
      print(paste("FIPS code invalid format:", n))
      next
    }
    # Find row
    if(substr(n, 3, 5) == "000"){
      # all counties
      r <- county_state_fips[which(county_state_fips$fips_st == substr(n, 1, 2)), ][1,]
      r$ct_name <- "ALL"
      r$fips_ct <- "000"
      f$fips <- n
    } else {
      r <- county_state_fips[which(county_state_fips$fips == n), ]
    }
    if(nrow(r)!=1){
      print(paste("FIPS code not found:", n))
    }
    # Append
    ret <- rbind(ret, data.frame(state=r$st_abbr, county=r$ct_name, county_code=r$fips_ct, fips=r$fips))
  }
  return(ret)
}

#' get_state_county_by_names
#' @description This function convert a data.frame of state abbr. and county names to corresponding state-county pairs + fips
#' @param sc    A data.frame containing state abbreviation and county names
#' @return A data.frame with first column as state abbreviation, second column as county name,
#'         both in lower case, and third column fips.
#' @export
get_state_county_by_names <- function(sc){
  ret <- setNames(data.frame(matrix(ncol = 4, nrow = 0)), c("state", "county", "county_code", "fips"))
  for (i in 1:nrow(sc)){
    # Find row
    if(sc[i,2]=="*"){
      # all counties
      r <- county_state_fips[which(county_state_fips$st_abbr==sc[i,1]),][1,]
      r$ct_name <- "ALL"
      r$fips_ct <- "000"
      f$fips <- paste0(r$fips_st, "000")
    } else {
      r <- county_state_fips[which(county_state_fips$ct_name==sc[i,2] & county_state_fips$st_abbr==sc[i,1]),]
    }
    if(nrow(r)!=1){
      print(paste("Area not found:", n))
    }
    # Append
    ret <- rbind(ret, data.frame(state=r$st_abbr, county=r$ct_name, county_code=r$fips_ct, fips=r$fips))
  }
  return(ret)
}