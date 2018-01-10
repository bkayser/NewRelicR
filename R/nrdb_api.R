#' Execute NRQL queries on NRDB (Insights).
#'
#' This function is a facade on the NRDB REST API.  It allows you to execute queries
#' and analyze the results in a data frame.
#'
#' @param account_id your New Relic account ID
#' @param api_key your New Relic NRDB (Insights) API key
#' @param nrql_query the NRQL query to execute
#' @param verbose indicates status information to be printed out
#' @param timeout the max time in milliseconds to wait for a response (default: 10,000)
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
nrdb_query <- function(account_id, api_key, nrql_query, verbose=F,
                       timeout=10000) {
    if (verbose) message(paste("Query:", nrql_query))
    if (account_id > 0) {
        url <- paste("https://insights-api.newrelic.com/v1/accounts/",
                     account_id, "/query", sep = '')
    } else {
        url <- 'http://mockbin.org/bin/b1db81d0-a699-44ae-87e0-ed9092d1e017'
    }
    body <- list(account=account_id,
                 format='json',
                 query=nrql_query,
                 restrictions=list(isNewRelicAdmin=T, externalTimeoutMilliseconds=timeout),
                 jsonVersion=1,
                 metadata=list(hostUser=Sys.getenv('USER'),
                               hostname=system('hostname', intern=T),
                               origin='NewRelicR libray'))
    headers <- c('X-Dirac-Client-Origin'='NewRelicR library',
                    "Content-Type"="application/json")
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
        if (rlang::is_empty(result$facets)) {
            process_facets(result)
        } else if (!is.null(result$facets[[1]]$timeSeries)) {
            process_faceted_timeseries(result)
        } else if (!is.null(result$metadata$contents$contents) && result$metadata$contents$contents[[1]][['function']] == 'uniques') {
            process_faceted_uniques(result)
        } else {
            process_facets(result)
        }
    } else if (!is.null(result$timeSeries)) {
        process_timeseries(result)
    } else if (names(result$results[[1]])[1] == 'events') {
        process_events(result)
    } else if (!is.null(result$results)) {
        process_aggregates(result)
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
#' @param min_length applies a filter with a minimum length of a session by number of pages
#' @param max_length applies a filter with a maximum session length in pages
#' @param min_unique applies a filter with a minimum number of unique pages in a session to avoid sessions that only have one unique page
#' @param event_type to override the PageView event type with another event type
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

    sessions <-
        dplyr::filter(
            dplyr::arrange(
                data.frame(session=as.character(v[[1]]),
                           length=v[[3]],
                           uniques=v[[2]],
                           start=as.POSIXct(v[[4]]/1000, origin='1970-01-01'),
                           end=as.POSIXct(v[[5]]/1000, origin='1970-01-01'),
                           stringsAsFactors = F),
                dplyr::desc(uniques)),
            length >= min_length & uniques >= min_unique)

    if (!missing(max_length)) {
        sessions <- dplyr::filter(sessions, length <= max_length)
    }
    return(dplyr::tbl_df(utils::head(sessions, size)))
}

#' Get the user interactions for a list of session Ids, including Page Views and Page Actions.
#'
#' @param account_id your New Relic account ID
#' @param api_key your New Relic NRDB (Insights) API key
#' @param session_ids the list of session ids obtained using \code{\link{nrdb_session_ids}}
#' @param app_id the id of the app to focus on
#' @param limit the limit on the number of PageViews in a session to this number; if there are
#' more than this number of pages the session is skipped.  Default limit is 750 and the max limit
#' is 1000.
#' @param from the starting point in hours past to search for events
#' @param event_type the type of event if other than 'PageView' such as 'PageAction'
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
        if (!rlang::is_empty(pages)) {
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
#' @param end_time an optional upper limit on the time to look for transactions
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
#' There are a lot of transient errors in the NRDB api.  If an error occurs getting a chunk, it will retry 3 times and then give up.
#'
#' @param account_id your New Relic account ID
#' @param api_key your New Relic NRDB (Insights) API key
#' @param app_id the application with the transactions, required unless a where clause is provided
#' @param attrs an optional array of attributes to fetch; default is everything (*)
#' @param where a clause to use to qualify the events fetched; must specify either app_id or where
#' @param start_time the timestamp where to start looking
#' @param end_time the end bounds of the timerange
#' @param limit the number of events to retrieve
#' @param event_type the given transaction type; default is 'Transaction'
#' @param verbose for detailed logging
#' @param timeout value in ms to wait for a response
#' @param sampling_rate the rate of events to use, if sampling.  Value between 0 and 1
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
                        end_time=Sys.time(),
                        limit=1000,
                        event_type='Transaction',
                        verbose=F,
                        timeout=1000,
                        sampling_rate=NULL) {
    period.start <- as.numeric(start_time, unit='secs')

    if (!is.null(app_id)) {
        where.list <- c(paste0('appId=',app_id))
    } else {
        where.list <- vector('character')
    }
    if (!is.null(where)) {
        where.list <- c(where.list, where)
    }
    if (!is.null(end_time)) {
        where.list <- c(where.list, paste0('timestamp <= ', format(as.numeric(end_time) * 1000, scientific = F)))
    }
    if (!is.null(sampling_rate)) {
        where.list <- c(where.list, paste0('random() < ', format(sampling_rate, scientific = F)))
    }
    where <- stringi::stri_join(where.list, collapse=' and ' )
    if (length(where) == 0) stop("provide either an app id or a where clause")

    if (limit <= 1000) {
        q <- paste0('select ', paste0(attrs, collapse=','),
                    ' from ', event_type,
                    ' where ', where,
                    ' since ', nrql.timestamp(period.start),
                    ' limit ', limit)
        return(nrdb_query(account_id, api_key, q, verbose=verbose, timeout=timeout))
    }

    est.rate <- nrdb_query(account_id,
                           api_key,
                           paste0('select(count(*))/(60*10) from ',
                                                       event_type,
                                                       ' where ', where,
                                                       ' since ', nrql.timestamp(start_time),
                                                       ' until ', nrql.timestamp(start_time+lubridate::dminutes(10))),
                           verbose=verbose,
                           timeout=timeout)
    if (est.rate <= 0.0) {
        stop('Cannot find enough events in that time range--try changing the start_time')
    }
    est.period <- 850.0 / est.rate

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
        chunk <- NULL
        errors <- 0
        while (errors < 5) {
            tryCatch({chunk <- nrdb_query(account_id, api_key, q, verbose=verbose, timeout=timeout); errors <- 0 },
                              error=function(msg) {
                                  message("Error getting chunk at ", nrql.timestamp(period.start), ", attempt ", errors,": ",msg)
                                  if (stringi::stri_detect_fixed(msg, 'NRQL Syntax Error')) {
                                      errors <<- 5
                                  } else {
                                      errors <<- errors + 1
                                      Sys.sleep(2**errors)
                                  }
                              })
           if (errors == 0) break
        }
        if (errors > 0) {
            warning("Failed to get chunk.  Stopping early.")
            break
        }
        if (rlang::is_empty(chunk)) {
            message('no more data in time range after ', as.POSIXct(period.start, origin='1970-01-01'))
            break
        } else if (nrow(chunk) == 1000) {
            # Use an aggressive backoff if we hit the 1000 event limit
            est.rate <- 1.5 * est.rate
        } else {
            # Else move 20% towards the target rate of 850
            est.rate <- (0.8 * est.rate) + (0.2 * nrow(chunk) / as.numeric(period.end - period.start, unit='secs'))
        }
        message('fetched ',nrow(chunk), ' rows around ', as.POSIXct(period.start, origin='1970-01-01'),
                ' using a time range of ', signif(est.period, 3), ' seconds')
        count <- count + nrow(chunk)
        chunks[[length(chunks)+1]] <- chunk
        est.period <- 850 / est.rate
        period.start <- period.end
        period.end <- period.start + est.period
    }
    df <- plyr::rbind.fill(chunks)
    utils::head(df, limit)
}

## Unexported helper functions

# If the timestamp is numeric assume a float value in seconds and return an epoch timestamp in milliseconds.
# Otherwise assume a posix time and return a string timestamp
nrql.timestamp <- function(ts) {
    if (is.numeric(ts)) {
        format(ts * 1000, scientific=F, nsmall=0)
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

    if (rlang::is_empty(events)) {
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
        events <- dplyr::rename(events, name='backendTransactionName')
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
            s <- as.numeric(difftime(v$timestamp[i+1], v$timestamp[i], units='secs'))
            d <- v$duration[i]
            if (s > d) {
                v[i,'think'] <- (s - d)
            }
        }
    }
    return(v)
}

# Each of these functions handles a different type of result from the NRDB query.
# They only cover the most common cases.  I continue to expand them.

process_faceted_uniques <- function(result) {
    # This block deals with "uniques(...)" queries and may not handle every
    # case
    facets <- lapply(result$facets, function(l) {
        values <- unlist(l$results[[1]]$members)
        df <- dplyr::bind_cols(data.frame(name=l[[1]], stringsAsFactors = F),
                               as.data.frame(matrix(T, ncol=length(values))))
        names(df)[2:length(df)] <- values
        df
    })
    dplyr::tbl_df(dplyr::bind_rows(facets))
}

process_facets <- function(result) {
    facet_names <- unlist(result$metadata$facet)
    facets <- dplyr::bind_rows(lapply(result$facets, function(facet) {
        # Flatten
        cols <- purrr::flatten(purrr::flatten(facet))
        # Remove nulls
        cols[sapply(cols, is.null)] <- NA
        names(cols)[1:length(facet_names)] <- facet_names
        as.data.frame(cols, stringsAsFactors=F)
    }))
    if (!rlang::is_empty(facets)) {
        names(facets) <- c(facet_names, 
                           process_colnames(result$metadata))
        dplyr::tbl_df(facets)
    } else if (!is.null(result$totalResult$timeSeries)) {
        df <- dplyr::bind_rows(lapply(result$totalResult$timeSeries, as.data.frame))
        df <- dplyr::mutate(df,
                            begin_time = as.POSIXct(beginTimeSeconds, origin='1970-01-01'),
                            end_time = as.POSIXct(endTimeSeconds, origin='1970-01-01'))
        dplyr::select(df, begin_time, end_time)
    }
}

process_timeseries <- function(result) {
    # Complicated loop to replace NULL with NA!
    converted <- sapply(result$timeSeries, function(timeslice) {
        for (i in seq_along(timeslice$results)) {
            # One result column could contain more than one value for apdex (and maybe others)
            for (part in seq_along(timeslice$results[[i]])) {
                if (is.null(timeslice$results[[i]][[part]])) timeslice$results[[i]][[part]] <- 0
            }
        }
        unlist(timeslice)
    })
    timeseries <- as.data.frame(t(converted))
    # Strip leading 'results.' part
    colnames <- process_colnames(result$metadata)
    names(timeseries)[1:length(colnames)] <- colnames
    tryCatch(dplyr::tbl_df(timeseries),
             error=function(m) {
                 stop('problem with the timeseries: ', names(timeseries))
             })
}

process_colnames <- function(metadata) {
    colnames <- c()
    contents <- metadata$contents$contents
    if (is.null(contents)) contents <- metadata$contents$timeSeries$contents
    if (is.null(contents)) contents <- metadata$timeSeries$contents
    if (is.null(contents)) contents <- metadata$contents
    for (attr in contents) {
        if (!is.null(attr$contents) && attr$contents$`function` == 'apdex') {
            if (is.null(attr$alias))
                base <- 'apdex'
            else
                base <- attr$alias
            colnames[length(colnames) + 1] <- paste0(base,'_s')
            colnames[length(colnames) + 1] <- paste0(base,'_t')
            colnames[length(colnames) + 1] <- paste0(base,'_f')
            colnames[length(colnames) + 1] <- paste0(base,'_score')
            colnames[length(colnames) + 1] <- paste0(base,'_count')
        } else if (!is.null(attr$alias)) {
            colnames[length(colnames)+1] <- attr$alias
        } else if (!is.null(attr$attribute)) {
            colnames[length(colnames)+1] <- paste0(attr$`function`, '_', attr$attribute)
        } else if (!is.null(attr$`function`)) {
            colnames[length(colnames)+1] <- attr$`function`
        } else {
            colnames[length(colnames)+1] <- 'UNKNOWN'
            warning("Unable to identify name of column: ", utils::str(attr))
        }
    }
    colnames
}
process_events <- function(result) {
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
}
process_aggregates <-  function(result) {
    values <- unlist(result$results)
    names(values) <- process_colnames(result$metadata)
    return(values)
}

process_faceted_timeseries <- function(result) {
    timeseries <- dplyr::tibble()
    valnames <- process_colnames(result$metadata)
    for (facetIndex in seq_along(result$facets)) {
        facet <- result$facets[[facetIndex]]
        colnames <- paste(facet$name, valnames, sep='.')
        for (timeslice in facet$timeSeries) {
            begin_time <- as.POSIXct(timeslice$beginTimeSeconds, origin='1970-01-01')
            end_time <- as.POSIXct(timeslice$endTimeSeconds, origin='1970-01-01')
            for (valIndex in seq_along(timeslice$results)) {
                name <- paste0(valnames[valIndex], '.', facet$name)
                val <- timeslice$results[[valIndex]][[1]]
                timeseries <- dplyr::bind_rows(timeseries,
                                               data.frame(stringsAsFactors = F,
                                                          list(begin_time=begin_time,
                                                               end_time=end_time,
                                                               name=name,
                                                               value=val)))
            }
        }
    }
    reshape2::dcast(timeseries, begin_time + end_time ~ name, fill=0 )
}
