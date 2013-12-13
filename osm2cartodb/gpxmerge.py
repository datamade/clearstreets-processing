from os import listdir
from os.path import isfile, join

gpx_files = [ f for f in listdir('../gpx') if isfile(join('../gpx',f)) and '.gpx' in f]

with open("gpx_all.gpx", "a") as outfile:
  outfile.write('<?xml version="1.0"?>\n')
  outfile.write('<gpx version="1.1" creator="GDAL 1.9.2" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://www.topografix.com/GPX/1/1" xsi:schemaLocation="http://www.topografix.com/GPX/1/1 http://www.topografix.com/GPX/1/1/gpx.xsd">\n')

  for gpx_file in gpx_files:
    with open("../gpx/%s" % gpx_file) as f:
      for line in f:
        if not '<?xml' in line and not '<gpx' in line and not '<metadata' in line and not '</gpx>' in line:
          outfile.write(line)

  outfile.write('</gpx>\n')