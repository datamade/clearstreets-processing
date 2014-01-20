from os import listdir, rename
from os.path import isfile, join

gpx_files = [ f for f in listdir('../gpx') if isfile(join('../gpx',f)) and '.gpx' in f]
osm_files = [ f for f in listdir('../osm') if isfile(join('../osm',f)) and '.osm' in f]

for gpx_file in gpx_files:
  asset_name = gpx_file.replace(".gpx", "")
  for osm_file in osm_files:
    if asset_name in osm_file:
      print "moving %s" % gpx_file
      rename("../gpx/%s" % gpx_file, "../gpx_processed/%s" % gpx_file)