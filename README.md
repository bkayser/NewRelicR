# R Package for New Relic Data APIs

This package contains utility code for accessing NRDB and Metric Data collected
by [New Relic](http://www.newrelic.com).


## Getting Started

Load the `NewRelicR` package with the following code:

```r
install.packages("devtools")
devtools::install_github("newrelic/NewRelicR")
```

Refer to the help documentation in the package for details on how to use the methods.

```r
??newrelic
```

Please report any bugs or suggestions [here](https://github.com/newrelic/NewRelicR/issues).

## Acknowledgements

This package was developed by [Bill Kayser](https://github.com/bkayser) and New Relic, Inc.


## History

### Version 0.5.0.1000

* Change the naming of some of the results to be more intelligent, using aliases and attribute names for facets instead of "facet" as well as properly naming the values of non-faceted, non-timeseries results.
* Support multi-facet queries  

### Version 0.4.0.1005

* Fixed processing of facets
* Improved error handling by adding early stop
* Fixed misc API problems and errors found by the `check()` routine

### Version 0.4.0.1004

* Added sampling_rate and end_time to nrdb_events()

### Version 0.4.0.1003

* Better handling of edge case when there is no data

### Version 0.4.0.1002

* Fixed a problem processing percentage() functions.

### Version 0.4.0.1001

* Added rlang library
* Removed some invalid references to %>% operator
* Added some more boundary case coverage for nrdb queries

### Version 0.4.0.1000

* Refactored nrdb_query code: WARNING -- may return different names for columns
* Better support for faceted timeseries

### Version 0.3.0.1107

* Better support for "uniques()" queries in nrdb.

### Version 0.3.0.1106

* Add timeout parameter to events calls
* Add retry loop to nrdb_events so its more robust for big queries

### Version 0.3.0.1105

* Improve column labels with multi-column timeseries.

### Version 0.3.0.1104

* Fix edge case in nrdb_query when you are getting multiple summary values.

### Version 0.3.0.1003

* Add verbose option for nrdb_events
* Fix bug in nrdb_events where WHERE was ignored

### Version 0.3.0.1001

* Added verbose option for several calls to see the NRQL.

### Version 0.3.0.1000

* Removed `sample_events()` since it didn't do a good job of sampling.
* Renamed `get_top_transactions` to `nrdb_top_transactions` for consistency.
* Improved queries when the attributes were specified explicitly

### Version 0.2.0.1001

* Renamed `get_events` to `nrdb_events`.

### Version 0.2.0.1000

* Changed license from GPL2 to BSD
* Added `get_events` method to get contiguous chunks of events for a given criteria.

### Version 0.1.0.1011

* Fixed some parameters in the session calls in the nrdb API

### Version 0.1.0.1010

* Fixed problem calculating think time
* Switched to include BrowserInteractions now available with SPA monitoring

### Version 0.1.0.1009

* Updated the session functions in the NRDB API.  Added some new options.

### Version 0.1.0.1008

* Add support for loading PageActions in session data

### Version 0.1.0.1007

* Add end_time parameter to get top applications

### Version 0.1.0.1005

* Made verbose output an option; quiet by default
* Fix bug in managing query cache

### Version 0.1.0.1004

* Fix bug in `get_top_transactions()`

### Version 0.1.0.1003

* Bug fixes and warnings

### Version 0.1.0.1000

* Add `rpm_applications()` function
* Fixes and documentation
* Bumped feature version to 1.

### Version 0.0.0.9006

* Add batch fetching to the `rpm_query()` call.  This means it will respect the period
  parameter and simply fetch data in chunks to get the resolution you need.
* New method for nrdb, `sample_events()` which selects a requested number of events near a point in time
  which may not be contiguous (hence "sample").  

### Version 0.0.0.9003

* Support for multi column timeseries queries, like `select average(duration), count(*) from Transaction timeseries AUTO`

### Version 0.0.0.9002

* Removed dependency on plyr
* Removed `%>%` usage

### Version 0.0.0.9001

* Removed `newrelic_api` helper in favor of passing api keys directly to functions
* Fixed many bugs
* Added some examples using pastebin
* Improved docs

### Version 0.0.0.9000

* Initial release
