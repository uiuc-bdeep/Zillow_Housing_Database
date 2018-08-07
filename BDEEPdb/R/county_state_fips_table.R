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

#' get_state_county
#' @description This function convert a vector of fips to corresponding state-county pairs
#' @param fips A vector of fips numbers stored as TEXT (characters)
#' @return A data.frame with first column as state abbreviation, second column as county name,
#'         both in lower case, and third column fips.
#' @export
get_state_county <- function(fips){
  ret <- setNames(data.frame(matrix(ncol = 3, nrow = 0)), c("state", "county", "fips"))
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
    ret <- rbind(ret, data.frame(state=r$st_abbr, county=r$ct_name, fips=r$fips))
  }
  return(ret)
}
