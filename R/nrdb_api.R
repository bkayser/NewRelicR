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
        for (i in seq_along(result$metadata$timeSeries$contents)) {
            attrs <- result$metadata$timeSeries$contents[[i]]
            if (!is.null(attrs$attribute)) {
                names(timeseries)[i] <- paste0(attrs$`function`, '_', attrs$attribute)
            }
        }
        dplyr::tbl_df(timeseries)
    } else if (names(result$results[[1]])[1] == 'events') {
        rows <- lapply(result$results[[1]]$events, function(event) {
            # Strip out nulls that appear when you select explicit attributes.
            as.data.frame(event[!sapply(event,is.null)], stringsAsFactors=F)
        })
        if (length(rows) > 0) {
            df <- plyr::rbind.fill(rows)
            dplyr::tbl_df(df)
        } else {
            return(NULL)
        }
        
    } else if (!is.null(result$results)) {
        dplyr::tbl_df(return(unlist(result$results)))
    } else {
        stop("Unsupported result type; only facets, timeseries and events supported now.")
    }
}

#' Get User Session IDs.
#'
#' This looks in the given time range for the top sessions based
#' on the number of distinct pages called
#' during the session.
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
#' @param verbose TRUE to print out the queries
#' 
#' @return a character vector of session ids
#' @export
#'
nrdb_session_ids <- function(account_id,
                             api_key,
                             app_id,
                             from=48,
                             to=1,
                             size=50,
                             min_length=10,
                             max_length=NULL,
                             min_unique=1,
                             event_type='PageView',
                             verbose=T) {

    limit <- 1000
    name_attr = if (event_type=='PageView') 'name' else 'backendTransactionName'
    q <- paste0('select uniqueCount(', name_attr, '),',
                ' count(*), min(timestamp), max(timestamp) from ', event_type,
                ' where appId = ', app_id,
                ' until ', to, " hours ago",
                ' since ', from, " hours ago",
                ' facet session',
                ' limit ', limit)

    v <- nrdb_query(account_id, api_key,q, verbose=verbose)

    sessions <- data.frame(session=as.character(v[[1]]),
                           length=v[[3]],
                           uniques=v[[2]],
                           start=as.POSIXct(v[[4]]/1000, origin='1970-01-01'),
                           end=as.POSIXct(v[[5]]/1000, origin='1970-01-01'),
                           stringsAsFactors = F) %>%
        dplyr::arrange(desc(uniques)) %>%
        dplyr::filter(length >= min_length & uniques >= min_unique)

    if (!missing(max_length)) {
        sessions <- dplyr::filter(sessions, length <= max_length)
    }
    return(dplyr::tbl_df(head(sessions, size)))
}

#' Get the user interactions for a list of session Ids, including Page Views and Page Actions.
#'
#' @param account_id your New Relic account ID
#' @param api_key your New Relic NRDB (Insights) API key
#' @param session_ids the list of session ids obtained using \code{\link{nrdb_session_ids}}
#' @param limit the limit on the number of PageViews in a session to this number; if there are
#' more than this number of pages the session is skipped.  Default limit is 750 and the max limit
#' is 1000.
#' @param from the starting point in hours past to search for events
#' @param page_views_only if true (default) will not get PageActions
#' @param verbose TRUE to print out queries
#' @return list of data frames for each session that contain a page view in each row
#'   and the union of all attributes as columns
#' @export
#'
nrdb_sessions <- function(account_id,
                          api_key,
                          session_ids,
                          app_id=NULL,
                          limit=NULL,
                          from=24*3,
                          event_type='PageView',
                          verbose=F) {
    if (!is.null(limit) && limit > 1000) warning("A maximum of 1000 pages per session will be captured")
    sessions <- list()
    for(session in session_ids) {
        pages <- process_session(account_id, api_key, session,
                                 limit=limit,
                                 event_type=event_type,
                                 from=from,
                                 app_id = app_id,
                                 verbose = verbose)
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
nrdb_top_transactions <- function(account_id,
                                  api_key,
                                  app_id,
                                  limit=10,
                                  event_type='Transaction',
                                  end_time) {
    q <- paste0('select count(*) from ', event_type, ' where appId=', app_id, ' facet name limit ',limit)
    if (!missing(end_time)) q <- paste(q, 'until', as.numeric(end_time))
    nrdb_query(account_id, api_key, paste('select count(*) from ', event_type, ' where appId=', app_id, ' facet name limit ',limit, sep=''))
}

#' Retrieve a contiguous chunk of raw events.
#'
#' This returns a set of events making a best effort to get a contguous chunk.
#' So if you give a limit of 5000 events it will make successive queries staying
#' under the limit of 1000 until it has all 5000 events.  You are guaranteed no duplicates
#' but not guaranteed you won't miss some events if they are sparse or bursty.
#'
#' @param account_id your New Relic account ID
#' @param api_key your New Relic NRDB (Insights) API key
#' @param app_id the application with the transactions, required unless a where clause is provided
#' @param attrs an optional array of attributes to fetch; default is everything (*)
#' @param where a clause to use to qualify the events fetched; must specify either app_id or where.
#' @param start_time the timestamp where to start looking.
#' @param limit the number of events to retrieve
#' @param event_type the given transaction type; default is 'Transaction'
#'
#' @return a dataframe with limit rows and the union of all attributes in all sampled events.
#' @export
#'
nrdb_events <- function(account_id,
                        api_key,
                        app_id=NULL,
                        attrs='*',
                        where=NULL,
                        start_time=Sys.time()-lubridate::dminutes(60),
                        limit=1000,
                        event_type='Transaction') {
    if (!is.null(app_id)) {
        where <- paste0('appId=',app_id)
    } else if (is.null(where)) {
        stop("must supply app_id or where clause")
    }

    est.rate <- nrdb_query(account_id, api_key, paste0('select(count(*))/(60*10) from ',
                                                       event_type,
                                                       ' where ', where,
                                                       ' since ', nrql.timestamp(start_time),
                                                       ' until ', nrql.timestamp(start_time+lubridate::dminutes(10))))
    if (est.rate <= 0.0) {
        stop('Cannot find enough events in that time range--try changing the start_time')
    }
    est.period <- 850.0 / est.rate

    period.start <- as.numeric(start_time, unit='secs')
    period.end <- period.start + est.period

    chunks <- list()
    count <- 0
    now <- as.numeric(Sys.time(), units='sec')
    while (count < limit && now > period.start) {
        q <- paste0('select ', paste0(attrs, collapse=','),
                    ' from ', event_type,
                    ' where ', where,
                    ' since ', nrql.timestamp(period.start),
                    ' until ', nrql.timestamp(period.end),
                    ' limit ', round(min(1.1*(limit-count), 1000)))
        chunk <- nrdb_query(account_id, api_key, q)
        message('fetched ',nrow(chunk), ' rows around ', as.POSIXct(period.start, origin='1970-01-01'),
                ' using a time range of ', signif(est.period, 3), ' seconds')
        count <- count + nrow(chunk)
        chunks[[length(chunks)+1]] <- chunk
        if (plyr::empty(chunk)) {
            break;
        } else if (nrow(chunk) == 1000) {
            # Use an aggressive backoff if we hit the 1000 event limit
            est.rate <- 1.5 * est.rate
        } else {
            # Else move 20% towards the target rate of 850
            est.rate <- (0.8 * est.rate) + (0.2 * nrow(chunk) / as.numeric(period.end - period.start, unit='secs'))
        }
        est.period <- 850 / est.rate
        period.start <- period.end
        period.end <- period.start + est.period
    }
    df <- plyr::rbind.fill(chunks)
    head(df, limit)
}

## Unexported helper functions

# If the timestamp is numeric assume a float value in seconds and return an epoch timestamp in milliseconds.
# Otherwise assume a posix time and return a string timestamp
nrql.timestamp <- function(ts) {
    if (is.numeric(ts)) {
        round(ts * 1000)
    } else {
        paste0("'", strftime(ts, '%Y-%m-%d %H:%M:%S', tz='UTC'), "'")
    }
}

process_session <- function(account_id,
                            api_key,
                            session_id,
                            limit,
                            event_type,
                            from,
                            app_id,
                            verbose) {

    events <- NULL
    tryCatch({
        events <- nrdb_query(account_id, 
                             api_key,
                             event_query("*", event_type, session_id, from, app_id, limit),
                             verbose=verbose)
    },
    error=function(e) {
        warning("Error returned getting session: ", e)
        return(NULL)
    })

    if (plyr::empty(events)) {
        message("No events found in session ", session_id)
        return(NULL)
    }

    events['type'] <- event_type
    message("Session: ", session_id, " - ", nrow(events), " ", event_type, " events")
    return(postprocess(events))
}

event_query <- function(select, event_type, session_id, from, app_id, limit=100) {
    q <-  paste0("select ",select," from ", event_type, " where session='",session_id,"'")
    if (!is.null(app_id)) q <- paste0(q, " and appId=",app_id)
    paste(q, 'since', from, 'hours ago',
          'limit', if (is.null(limit)) 1000 else min(1000, limit))
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
    if ('backendTransactionName' %in% names(events)) {
        events <- rename(events, name=backendTransactionName)
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
