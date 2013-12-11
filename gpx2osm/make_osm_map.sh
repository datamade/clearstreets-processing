# Get OSM extract
wget --output-document=test1.xml 'http://overpass-api.de/api/interpreter?data=<osm-script output="xml"> <union> <query type="way"> <has-kv k="highway" regv="motorway|motorway-link|trunk|trunk-link|primary|primary-link|secondary|secondary-link|tertiary|residential|roundabout"/> <bbox-query w="-88.50500" s="41.33900" e="-87.06600" n="42.29700"/> </query> </union> <print mode="body"/> <recurse type="down"/> <print mode="skeleton"/> </osm-script>'
mono OSM2Routing.exe --osm=test.xml --config=chicago.config --output=chicago-r.osm
