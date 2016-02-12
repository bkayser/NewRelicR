#' NRDB Query
#'
#' @param nr_credentials the credentials object created with \code{newrelic_api()}
#' @param nrql_query the NRQL query to execute
#'
#' @return a data frame with the results
#' @import httr dplyr data.table
#' @export
#'
#' @examples
nrdb_query <- function(nr_credentials, nrql_query) {
    message(paste("Query:", nrql_query))
    response <- GET(paste("https://insights-api.newrelic.com/v1/accounts/",
                          nr_credentials$account_id, "/query", sep = ''),
                    query=list(nrql=nrql_query),
                    as='text',
                    accept("application/json"),
                    add_headers('X-Query-Key'=nr_credentials$nrdb_api_key))
    result <- content(response)
    if (!is.null(result$error)) {
        stop("Error in response: ", result$error)
    }
    if (!is.null(result$facets)) {
        tbl_df(ldply(result$facets, as.data.frame))
    } else if (!is.null(result$timeSeries)) {
        times <- rbindlist(result$timeSeries)
        timeseries <- rbindlist(times$results)
        timeseries$beginTime <- nrdb_timestamp(times$beginTimeSeconds * 1000)
        timeseries$endTime <- nrdb_timestamp(times$endTimeSeconds * 1000)
        tbl_df(timeseries)
    } else if (names(result$results[[1]])[1] == 'events') {
        tbl_df(ldply(result$results[[1]]$events, as.data.frame))
    } else if (!is.null(result$results)) {
        tbl_df(return(unpack(result$results[[1]])))
    } else {
        stop("Unsupported result type; only facets, timeseries and events supported now.")
    }
}

#' Get User Session IDs
#'
#' @param nr_credentials the credentials with the Insights API key.
#' @param app_id the application id; you can find the id for your application in the RPM application
#' by using the ID in the URL when viewing the application.
#' @param from number of hours into the past to begin the hunt for sessions
#' @param to number of hours into the past to end the hunt for sessions.  The default is six because
#' if you fetch sessions more recent than that they may still be open and therefore incomplete.
#' @param size the maximum number of session ids to fetch
#' @param diverse a boolean indicating that the session ids returned will be prioritized according to
#' how many distinct pages were accessed during the session. This is important if you have sessions from
#' robots or monitors that you want to ignore.
#'
#' @return a character vector of session ids
#' @export
#'
nrdb_session_ids <- function(nr_credentials, app_id, from=10, to=6, size=50, diverse=T) {
    if (diverse) {
        # Get the users with their pages:
        df <- nrdb_query(nr_credentials,
                         paste('select count(*) from PageView ',
                               'where appId =', app_id,
                               'since', from, 'hours ago until',
                               to, 'hours ago facet session',
                               'limit', size))
        return(df$name)
    } else {
        session_ids <- vector('character')
        since_minutes_ago <- from * 60
        until_minutes_ago <- since_minutes_ago + 30
        while (size > length(session_ids) & (since_minutes_ago > (to * 60))) {
            limit <- min(c(size, 60))
            v <- nrdb_query(nr_credentials,
                            paste('select uniques(name) from PageView ',
                                  'where appId =', app_id,
                                  'since', since_minutes_ago, "minutes ago",
                                  'until', until_minutes_ago, "minutes ago",
                                  'facet session',
                                  'limit', limit))
            session_ids <- unique(append(session_ids, as.character(v$name)))
            until_minutes_ago <- until_minutes_ago + 30
        }
        return(session_ids)
    }
}

#' Get the Page Views for a list of session Ids
#'
#' @param nr_credentials the credentials with the Insights API key.
#' @param session_ids the list of session ids obtained using \code{\link{nrdb_session_ids}}
#' @param limit the limit on the number of PageViews in a session to this number; if there are
#' more than this number of pages the session is skipped.  Default limit is 750 and the max limit
#' is 1000.
#'
#' @return
#' @export
#'
#' @examples
nrdb_sessions <- function(nr_credentials, session_ids, limit=750) {
    sessions <- list()

    if (limit > 1000) stop("Limit may not be greater than 1000")

    for(session in session_ids) {
        events <- nrdb_query(nr_credentials,
                             paste("select * from PageView",
                                   paste("where session='",session,"'",sep=''),
                                   'since 36 hours ago',
                                   'limit', limit))
        if (nrow(events) < limit && nrow(events) > 1) {
            message(paste("Session:", session, "-", nrow(events), "events"))
            sessions[[session]] <- postprocess(events)
        } else {
            message(paste("Skipped",session, "because there were", nrow(events), "events."))
        }
    }
    sessions
}

## Utility functions

unpack <- function(l) {
    if (class(l) != 'list') return(l)
    if (length(l) != 1) return(l)
    # Return the first element of the list and convert to a vector
    return(sapply(unpack(l[[1]]), identity))
}
nrdb_timestamp <- function(t) {
    as.POSIXct(t/1000, origin="1970-01-01")
}
postprocess <- function(events) {
    v <- mutate(events,
               timestamp=nrdb_timestamp(timestamp),
               name=gsub('^(WebTransaction/(JSP/|Servlet/)|Controller/)', '', name)) %>%
        # Sort by timestamp
        arrange(timestamp)
    if (nrow(v) > 1) {
        for (i in 1:(nrow(v)-1)) {
            s <- v[i+1,'timestamp'] - v[i,'timestamp']
            d <- v[i, 'duration']
            if (s > d) {
                v[i,'think'] <- (s - d)
            }
        }
    }
    return(v)
}
