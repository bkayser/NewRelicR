#' Execute NRQL queries on NRDB (Insights).
#'
#' This function is a facade on the NRDB REST API.  It allows you to execute queries
#' and analyze the results in a data frame.
#'
#' @param account_id your New Relic account ID
#' @param api_key your New Relic NRDB (Insights) API key
#' @param nrql_query the NRQL query to execute
#'
#' @return a data frame with the results
#' @seealso \href{https://docs.newrelic.com/docs/insights/new-relic-insights/adding-querying-data/querying-your-data-remotely}{New Relic NRQL REST API}
#' @seealso \href{https://docs.newrelic.com/docs/insights/new-relic-insights/using-new-relic-query-language/using-nrql}{NRQL Query Reference}
#' @seealso \href{https://docs.newrelic.com/docs/insights/new-relic-insights/adding-querying-data/querying-your-data-remotely#register}{How to get a NRDB API key}
#' @export
#'
#' @examples
#'     nrdb_query(account_id=-1, api_key='your_nrdb_api_license_key_here',
#'               nrql_query="select count(*) from PageView facet name")
nrdb_query <- function(account_id, api_key, nrql_query) {
    message(paste("Query:", nrql_query))
    if (account_id > 0) {
        url <- paste("https://insights-api.newrelic.com/v1/accounts/",
                     account_id, "/query", sep = '')
    } else {
        url <- 'http://mockbin.org/bin/b1db81d0-a699-44ae-87e0-ed9092d1e017'
    }
    response <- httr::POST(url,
                          body=list(nrql=nrql_query),
                          encode='json',
                          httr::accept("application/json"),
                          httr::add_headers('X-Query-Key'=api_key))
    result <- httr::content(response, type='application/json')

        if (!is.null(result$error)) {
        stop("Error in response: ", result$error)
    }
    if (!is.null(result$facets)) {
        dplyr::tbl_df(dplyr::bind_rows(lapply(result$facets, as.data.frame, stringsAsFactors=F)))
    } else if (!is.null(result$timeSeries)) {
        timeseries <- as.data.frame(t(sapply(result$timeSeries, unlist)))
        # Strip leading 'results.' part
        names(timeseries) <- stri_replace(names(timeseries), "", regex='^results\\.')
        dplyr::tbl_df(timeseries)
    } else if (names(result$results[[1]])[1] == 'events') {
        df <- dplyr::bind_rows(lapply(result$results[[1]]$events, as.data.frame, stringsAsFactors=F))
        dplyr::tbl_df(df)
    } else if (!is.null(result$results)) {
        dplyr::tbl_df(return(unpack(result$results[[1]])))
    } else {
        stop("Unsupported result type; only facets, timeseries and events supported now.")
    }
}

#' Get User Session IDs.
#'
#' This looks in the given time range for the top sessions based on either
#' the size of the session (diverse=F) or the number of distinct pages called
#' during the session (diverse=T).
#'
#' @param account_id your New Relic account ID
#' @param api_key your New Relic NRDB (Insights) API key
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
nrdb_session_ids <- function(account_id, api_key, app_id, from=10, to=6, size=50, diverse=T) {
    if (diverse) {
        # Get the users with their pages:
        df <- nrdb_query(account_id, api_key,
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
            v <- nrdb_query(account_id, api_key,
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

#' Get the Page Views for a list of session Ids.
#'
#' @param account_id your New Relic account ID
#' @param api_key your New Relic NRDB (Insights) API key
#' @param session_ids the list of session ids obtained using \code{\link{nrdb_session_ids}}
#' @param limit the limit on the number of PageViews in a session to this number; if there are
#' more than this number of pages the session is skipped.  Default limit is 750 and the max limit
#' is 1000.
#'
#' @return list of data frames for each session that contain a page view in each row
#'   and the union of all attributes as columns
#' @export
#'
nrdb_sessions <- function(account_id, api_key, session_ids, limit=750) {
    sessions <- list()

    if (limit > 1000) stop("Limit may not be greater than 1000")

    for(session in session_ids) {
        events <- nrdb_query(account_id, api_key,
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

#' Get the top transactions.
#'
#' This will return a list of transaction names and their call count for the last 30 minutes
#' ordered according to call count.
#'
#' @param account_id your New Relic account ID
#' @param api_key your New Relic NRDB (Insights) API key
#' @param app_id the application with the transactions
#' @param limit to limit the number of transactions returned
#' @param event_type the event table to use, either \code{PageView} or (default) \code{Transaction}
#'
#' @return a data frame with two variables, \code{name} and \code{count}
#' @export
get_top_transactions <- function(api_key, app_id, limit=10, event_type='Transaction') {
    nrdb_query(account_id, api_key, paste("select count(*) from ', event_type, ' where appId=", app_id, " facet name limit ",limit, sep=''),
         verbose=F)
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
    v <- dplyr::mutate(events,
                       timestamp=nrdb_timestamp(timestamp),
                       name=gsub('^(WebTransaction/(JSP/|Servlet/)|Controller/)', '', name))
    v <- dplyr::arrange(v, timestamp)
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
