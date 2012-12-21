# ClearStreets - Processing

ClearStreets is built with several different scripts. They work in the following order:
1. Plow Tracker site is scraped and data is saved locally
1. Point data is converted in to GPX format
1. GPX data is 'snapped to grid' for city streets and converted to OSM (Open Street Maps)
1. OSM data is converted in to KML and uploaded to Google Fusion Tables
1. Fusion Table data is read by a front end site and [shown on a map](http://clearstreets.org)
  
## Dependencies

* [Python]
* [PHP]
* [scipy](http://www.scipy.org/)
* [gdal](http://trac.osgeo.org/gdal/wiki/GdalOgrInPython) for ogr
* [mono](http://www.mono-project.com/Main_Page) (for linux)

## Still can't figure it out or more detail needed?

Email us! 

[Forest Gregg](mailto:fgregg+git@gmail.com)
[Derek Eder](mailto:derek.eder+git@gmail.com)

## Note on Patches/Pull Requests
 
* Fork the project.
* Make your feature addition or bug fix.
* Commit and send me a pull request.

== Copyright

Copyright (c) 2012 Open City. Released under the MIT License.

See [LICENSE](https://github.com/open-city/clearstreets-processing/wiki/License) for details 