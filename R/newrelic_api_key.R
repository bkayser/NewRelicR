#' Create API credentials for New Relic
#'
#' @param account_id the account ID
#' @param nrdb_api_key the REST API key from your account settings
#' @param rpm_api_key the Insights API key from your insights account settings
#'
#' @return the API credentials object
#' @export
#'
#' @examples
newrelic_api <- function(account_id, nrdb_api_key=NULL, rpm_api_key=NULL) {
    c(account_id=account_id,
      rpm_api_key=api_key,
      nrdb_api_key=nrdb_api_key)
}
