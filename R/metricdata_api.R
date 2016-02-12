#' Query New Relic Application Metricdata
#'
#' @param nr_credentials the credentials obtained from calling \code{newrelic_api()}`
#' @param app_id the application id
#' @param duration length of time in seconds of the time window of the metric data
#' @param end_time the end of the time window
#' @param period duration of each timeslice in seconds, minimum of 60
#' @param metrics a character vector of metric names
#' @param verbose show extra information about the calls
#' @param values a character vector of metric values (calls_per_minute, average_response_time, etc)
#' @param cache TRUE if the query results should be cached to local disk
#' @seealso \code{newrelic_api}
#' @return a data table with observations as timeslices and the timeslice start time, metric name, and values in the variables
#' @description
#' This executes a query using the underlying Metric Data REST API at api.newrelic.com.
#' Depending on the duration and period the data may be fetched in successive batches
#' due to the limitations on the amount of data that can be retrieved.
#' @import dplyr httr lubridate digest
#' @export
rpm_query <- function(nr_credentials,
                      app_id,
                      duration=60 * 60,
                      end_time=Sys.time(),
                      period=NULL,
                      metrics='HttpDispatcher',
                      verbose=F,
                      values='average_response_time',
                      cache=T) {

    metrics <- as.list(metrics)
    names(metrics) <- rep('names[]', length(metrics))

    values <- as.list(values)
    names(values) <- rep('values[]', length(values))

    query <- append(metrics, values)

    start_time <- end_time - duration
    period <- max(60, period)
    if (is.null(end_time)) end_time <- Sys.time()
    from <- to_json_time(start_time)
    to <-to_json_time(end_time)

    query <- append(query,
                    list( from=from,
                          to=to,
                          period=period,
                          raw=TRUE))

    url <- paste("https://api.newrelic.com/v2/applications/",
                 app_id,"/metrics/data.json",
                 sep = '')

    key <- digest(c(url, start_time, end_time, period, metrics))
    cachefile <- paste('query_cache/', key, '.RData', sep='')
    if (cache & file.exists(cachefile)) {
        load(file=cachefile)
    } else {
        if (cache & !dir.exists('query_cache')) {
            dir.create('query_cache')
        }
        response <- GET(url,
                        query=query,
                        as='text',
                        config(verbose=verbose),
                        accept("application/json"),
                        add_headers('X-Api-Key'=nr_credentials$rpm_api_key))
        result <- content(response)
        if (!is.null(result$error)) {
            stop("Error in response: ", result$error)
        }
        metric_data_frames <- lapply(result$metric_data$metrics, parse_metricdata)
        data <- bind_rows(metric_data_frames)
        save(data, file=cachefile)
    }
    return(data)
}

## Utility functions

to_json_time <- function(time) { strftime(time, '%Y-%m-%dT%H:%M:00%z') }
from_json_time <- function(timestr) { ymd_hms(timestr, tz='America/Los_Angeles', quiet = T)}
parse_metricdata <- function(metric_timeslices) {
    ts_index <- data.frame(name=metric_timeslices$name,
                           start=from_json_time(sapply(metric_timeslices$timeslices, function(ts) ts$from)),
                           stringsAsFactors=F)
    bind_cols(ts_index, rbindlist(lapply(metric_timeslices$timeslices, function(e) e$values)))
}
