#' Execute NRQL queries on NRDB (Insights).
#'
#' This function is a facade on the NRDB REST API.  It allows you to execute queries
#' and analyze the results in a data frame.
#'
#' @param account_id your New Relic account ID
#' @param api_key your New Relic NRDB (Insights) API key
#' @param nrql_query the NRQL query to execute
#' @param verbose indicates status information to be printed out
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
nrdb_query <- function(account_id, api_key, nrql_query, verbose=F) {
    if (verbose) message(paste("Query:", nrql_query))
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
        stop("Error in response: ", result$error, "\nQuery: ", nrql_query)
    }
    if (!is.null(result$facets)) {
        dplyr::tbl_df(dplyr::bind_rows(lapply(result$facets, as.data.frame, stringsAsFactors=F)))
    } else if (!is.null(result$timeSeries)) {
        timeseries <- as.data.frame(t(sapply(result$timeSeries, unlist)))
        # Strip leading 'results.' part
        names(timeseries) <- stringi::stri_replace(names(timeseries), "", regex='^results\\.')
        dplyr::tbl_df(timeseries)
    } else if (names(result$results[[1]])[1] == 'events') {
        rows <- lapply(result$results[[1]]$events, as.data.frame, stringsAsFactors=F)
        if (length(rows) > 0) {
            df <- plyr::rbind.fill(rows)
            dplyr::tbl_df(df)
        } else {
            return(NULL)
        }
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
#' @param max_length applies a filter with a maximum session length in pages
#' @param diverse a boolean indicating that the session ids returned will be prioritized according to
#' how many distinct pages were accessed during the session. This is important if you have sessions from
#' robots or monitors that you want to ignore.
#'
#' @return a character vector of session ids
#' @export
#'
nrdb_session_ids <- function(account_id,
                             api_key,
                             app_id,
                             from=48,
                             to=6,
                             size=50,
                             max_length=NULL,
                             diverse=T) {

    since_minutes_ago <- from * 60
    limit <- min(999, max(size * 4, 60))
    until_minutes_ago <- since_minutes_ago - 30
    v <- nrdb_query(account_id, api_key,
                    paste('select uniqueCount(name), count(*) from PageView ',
                          'where appId =', app_id,
                          "and name != 'public_access/charts/show' ",
                          'since', since_minutes_ago, "minutes ago",
                          'until', until_minutes_ago, "minutes ago",
                          'facet session',
                          'limit', limit))
    v <- data.frame(session=as.character(v$name), length=v$results.count)
    if (!missing(max_length)) {
        v <- dplyr::filter(v, length <= max_length)
    }
    return(dplyr::tbl_df(head(v, size)))
}

#' Get the user interactions for a list of session Ids, including Page Views and Page Actions.
#'
#' @param account_id your New Relic account ID
#' @param api_key your New Relic NRDB (Insights) API key
#' @param session_ids the list of session ids obtained using \code{\link{nrdb_session_ids}}
#' @param app_id internal artifact--not needed in general
#' @param limit the limit on the number of PageViews in a session to this number; if there are
#' more than this number of pages the session is skipped.  Default limit is 750 and the max limit
#' is 1000.
#' @param page_views_only if false (default) will not get BrowserInteractions
#'
#' @return list of data frames for each session that contain a page view in each row
#'   and the union of all attributes as columns
#' @export
#'
nrdb_sessions <- function(account_id, api_key, session_ids, app_id=NULL, limit=NULL, page_views_only=F) {
    if (missing(limit) || limit > 1000) warning("A maximum of 1000 pages per session will be captured")
    sessions <- list()
    for(session in session_ids) {
        pages <- process_session(account_id, api_key, session, limit, page_views_only, 24 * 3, app_id)
        if (!plyr::empty(pages)) {
            sessions[[session]] <- pages
        }
    }
    sessions
}

#' Get the top transactions.
#'
#' This will return a list of transaction names and their call count for the last 30 minutes
#' ordered according to call count.  If an end_time is provided then a 30 minute period just
#' prior to the end_time is examined.
#'
#' @param account_id your New Relic account ID
#' @param api_key your New Relic NRDB (Insights) API key
#' @param app_id the application with the transactions
#' @param limit to limit the number of transactions returned
#' @param event_type the event table to use, either \code{PageView} or (default) \code{Transaction}
#'
#' @return a data frame with two variables, \code{name} and \code{count}
#' @export
get_top_transactions <- function(account_id,
                                 api_key,
                                 app_id,
                                 limit=10,
                                 event_type='Transaction',
                                 end_time) {
    q <- paste0('select count(*) from ', event_type, ' where appId=', app_id, ' facet name limit ',limit)
    if (!missing(end_time)) q <- paste(q, 'until', as.numeric(end_time))
    nrdb_query(account_id, api_key, paste('select count(*) from ', event_type, ' where appId=', app_id, ' facet name limit ',limit, sep=''))
}

#' Retrieve a sample of events for an application.
#'
#' This returns a sample of events of event_type of the given length (limit) for the given application.
#' The events are fetched in chunks going backwards from the end_time until there are a total of limit
#' events.  There may be gaps in between the chunks.  You aren't guaranteed to get the complete set of
#' contiguous events in a given time range, hence the idea that you are only getting a "sample."
#'
#' @param account_id your New Relic account ID
#' @param api_key your New Relic NRDB (Insights) API key
#' @param app_id the application with the transactions
#' @param end_time the upper bound on the time range for getting events
#' @param limit the number of events to retrieve
#' @param event_type
#'
#' @return a dataframe with limit rows and the union of all attributes in all sampled events.
#' @export
#'
sample_events <- function(account_id,
                          api_key,
                          app_id,
                          end_time=Sys.time(),
                          limit=100,
                          event_type='Transaction') {
    c <- nrdb_query(account_id, api_key, paste('select(count(*)) from',
                                               event_type,
                                               'where appId=',
                                               app_id,
                                               'since 30 minutes ago'))
    rate <- c / ( 30 * 60  )
    batch_duration_ms <- 1000 * (800 / rate)

    begin_time <- end_time - lubridate::seconds(batch_duration)
    chunks <- list()
    count <- 0
    until <- 1000*as.numeric(end_time)
    while (count < limit) {
        since <- until - batch_duration_ms
        q <- paste('select * from',
                   event_type,
                   'where appId=', app_id,
                   'since ',round(since),
                   'until', round(until),
                   'limit', round(min(1.1*(limit-count), 1000)))
        chunk <- nrdb_query(account_id, api_key, q)
        chunks[[length(chunks)+1]] <- chunk
        count <- count + nrow(chunk)
        until <- floor(min(chunk$timestamp)) - 1
    }
    df <- plyr::rbind.fill(chunks)
    top(postprocess(df, add_think_time=F), limit)
}

## Unexported helper functions

process_session <- function(account_id,
                            api_key,
                            session_id,
                            limit,
                            page_views_only,
                            history_hours,
                            app_id) {

    events_count <- nrdb_query(account_id, api_key,
                               event_query("count(*)", "PageView", session_id, history_hours, app_id))

    if (events_count <= 1 || (!is.null(limit) && events_count > limit)) {
        message("Skipped ",session_id, " because there were ", events_count, " events.")
        return(NULL)
    }

    if (is.null(limit))
        limit <- 1000
    else
        limit <- min(limit, 1000)

    events <- nrdb_query(account_id, api_key,
                         event_query("*", "PageView", session_id, history_hours, app_id, limit))

    if (plyr::empty(events)) {
        message("No events found in session ", session_id)
        return(NULL)
    }

    events['type'] <- 'PageView'

    if (page_views_only) {
        message(paste("Session:", session_id, "-", nrow(events), "page views"))
    } else {
        actions <- nrdb_query(account_id, api_key,
                              event_query("*", "BrowserInteraction", session_id, history_hours, app_id, limit))
        if (is.null(actions) || nrow(actions) == 0) {
            message(paste("Session:", session_id, "-", nrow(events), "page views"))
        } else {
            message(paste("Session:", session_id, "-", nrow(events), "page views, ", nrow(actions), " page actions"))
            actions <- actions %>%
                mutate(name=backendTransactionName,
                       duration=0,
                       type='BrowserInteraction') %>%
                select(timestamp,
                       name,
                       duration,
                       type,
                       name,
                       session)
            events <- bind_rows(events, actions)
        }
    }
    return(postprocess(events))
    return(tryCatch(postprocess(events),
                    error=function(e) {
                        warning("Error post-processing session ", session_id, ": ", e)
                        NULL
                    }))

}

event_query <- function(select, event_type, session_id, history_hours, app_id, limit=1) {
    q <-  paste0("select ",select," from ", event_type, " where session='",session_id,"'")
    if (!is.null(app_id)) q <- paste(q, "and appId=",app_id)
    paste(q, 'since', history_hours, 'hours ago limit', format(limit, scientific = F))
}

unpack <- function(l) {
    if (class(l) != 'list') return(l)
    if (length(l) != 1) return(l)
    # Return the first element of the list and convert to a vector
    return(sapply(unpack(l[[1]]), identity))
}
nrdb_timestamp <- function(t) {
    as.POSIXct(t/1000, origin="1970-01-01")
}
postprocess <- function(events, add_think_time=T) {
    if (is.null(events)) {
        return(NULL)
    }
    # Strip off the transaction category to save space
    v <- dplyr::mutate(events,
                       timestamp=nrdb_timestamp(timestamp),
                       name=gsub('^(WebTransaction/(JSP/|Servlet/)|Controller/)', '', name))

    # Convert id fields to strings.  If an id field is all decimal digits then it
    # is transformed to a number and that won't bind with the values from other queries.
    for (colname in grep('_id(_|$)', names(v))) {
        v[colname] <- sapply(v[[colname]], as.character)
    }
    v <- dplyr::arrange(v, timestamp)
    if (add_think_time & nrow(v) > 1) {
        for (i in 1:(nrow(v)-1)) {
            s <- difftime(v$timestamp[i+1], v$timestamp[i], units='secs') %>% as.numeric
            d <- v$duration[i]
            if (s > d) {
                v[i,'think'] <- (s - d)
            }
        }
    }
    return(v)
}
