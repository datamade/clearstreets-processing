# ClearStreets - Processing

ClearStreets is built with several different scripts. They work in the following order:
* Plow Tracker site is scraped and data is saved locally
* Point data is converted in to GPX format
* GPX data is 'snapped to grid' for city streets and converted to OSM (Open Street Maps)
* OSM data is converted in to KML and uploaded to Google Fusion Tables
* Fusion Table data is read by a front end site and [shown on a map](http://clearstreets.org)
  
## Dependencies

* Python 3
* [scipy](http://www.scipy.org/)

## Still can't figure it out or more detail needed?

Email us! 

[Forest Gregg](mailto:fgregg+git@gmail.com)
[Derek Eder](mailto:derek.eder+git@gmail.com)

## Note on Patches/Pull Requests
 
* Fork the project.
* Make your feature addition or bug fix.
* Commit and send me a pull request.

== Copyright

Copyright (c) 2015 Open City. Released under the MIT License.

See [LICENSE](https://github.com/open-city/clearstreets-processing/wiki/License) for details 
