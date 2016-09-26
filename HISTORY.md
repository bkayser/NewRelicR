## Version 0.1.0.1007

* Add support for loading PageActions in session data

## Version 0.1.0.1007

* Add end_time parameter to get top applications

## Version 0.1.0.1005

* Made verbose output an option; quiet by default
* Fix bug in managing query cache

## Version 0.1.0.1004

* Fix bug in `get_top_transactions()`

## Version 0.1.0.1003

* Bug fixes and warnings

## Version 0.1.0.1000

* Add `rpm_applications()` function
* Fixes and documentation
* Bumped feature version to 1.

## Version 0.0.0.9006

* Add batch fetching to the `rpm_query()` call.  This means it will respect the period
  parameter and simply fetch data in chunks to get the resolution you need.
* New method for nrdb, `sample_events()` which selects a requested number of events near a point in time
  which may not be contiguous (hence "sample").  

## Version 0.0.0.9003

* Support for multi column timeseries queries, like `select average(duration), count(*) from Transaction timeseries AUTO`

## Version 0.0.0.9002

* Removed dependency on plyr
* Removed `%>%` usage

## Version 0.0.0.9001

* Removed `newrelic_api` helper in favor of passing api keys directly to functions
* Fixed many bugs
* Added some examples using pastebin
* Improved docs

## Version 0.0.0.9000

* Initial release

