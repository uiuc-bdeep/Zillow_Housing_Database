#' zip_state_city
#' @docType data
#'
#' @usage data(zip_state_city)
#'
#' @format A data.frame with combinations of zipcode, city and state names
#'
#' @keywords zip, zipcode, city, state
#'
#' @references \url{www.pier2pier.com/links/files/Countrystate/USA-Zip.xls}
"zip_state_city"

#' get_state_city_zipcode
#' @description This function convert a vector of zipcodes to corresponding state-city pairs
#' @param zips A vector of zip codes
#' @return A data.frame with first column as state, second column as zipcode,
#'         and last column the corresponding city.
#' @export
get_state_city_zipcode <- function(zips){
  ret <- setNames(data.frame(matrix(ncol = 3, nrow = 0)), c("state", "zip", "city"))
  for (n in zips){
    r <- zip_state_city[which(zip_state_city$zipcode == n), ]
    if (nrow(r) < 1){
      print("Zipcode area not found!")
    } else {
      ret <- rbind(ret, data.frame(state=r$st_abbr, zip=r$zipcode, city=r$city))
    }
  }
  return(ret)
}