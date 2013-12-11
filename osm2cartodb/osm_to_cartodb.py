from os import listdir
from os.path import isfile, join
import datetime

from cartodb_settings import CARTODB_SETTINGS
from cartodb import CartoDBAPIKey, CartoDBException
import xml.etree.ElementTree as ET

def get_latest_insert(plow_id, carto):
  latest = datetime.datetime(1990, 1, 1)
  try:
    query = carto.sql("select datestamp from %s where id = '%s'" % (CARTODB_SETTINGS['table'], plow_id))

    if 'datestamp' in query.keys():
      latest = query['datestamp']
    return latest

  except CartoDBException as e:
    print ("some error ocurred", e)

def insert_into_cartodb(plow_id, datestamp, the_geom, carto, insert_batch):

  # print plow_id, '-', datestamp

  the_geom_wkt = ''
  for point in the_geom:
    the_geom_wkt += point[1] + " " + point[0] + ","

  the_geom_wkt = the_geom_wkt[:-1]

  if len(insert_batch) < 1000:
    insert_batch.append([plow_id, datestamp, the_geom_wkt])

  else:
    insert_sql = "INSERT INTO %s (id, datestamp, the_geom) VALUES " % (CARTODB_SETTINGS['table'])

    for item in insert_batch:
      insert_sql += " ('%s', '%s', ST_GeomFromText('LINESTRING(%s)', 4326))," % (item[0], item[1], item[2])

    insert_sql = insert_sql[:-1]  
    # print 'insert_sql', insert_sql

    try:
      carto.sql(insert_sql)
      insert_batch = []
    except CartoDBException as e:
      print ("some error ocurred", e)

  return insert_batch

def clear_out_table(carto):
  try:
    carto.sql("delete from %s" % CARTODB_SETTINGS['table'])
  except CartoDBException as e:
    print ("some error ocurred", e)

# setup
user =  CARTODB_SETTINGS['user']
API_KEY = CARTODB_SETTINGS['api_key']
cartodb_domain = CARTODB_SETTINGS['domain']
carto = CartoDBAPIKey(API_KEY, cartodb_domain)

clear_out_table(carto)

insert_batch = []
insert_count = 0
osm_files = [ f for f in listdir('../osm') if isfile(join('../osm',f)) and '.osm' in f]

for osm_file in osm_files:

  plow_id = osm_file.split("_")[0]
  datestamp = ''
  the_geom = []

  print 'importing', plow_id

  latest_insert = get_latest_insert(plow_id, carto)
  current_segment_datestamp = datetime.datetime(1990, 1, 2)
  # print 'latest_insert', latest_insert

  # read OSM file
  tree = ET.parse("../osm/%s" % osm_file)
  for node in tree.getroot().iter('node'):
    # print current_segment_datestamp
    # print node.attrib
    for child in node:
      # print child.attrib
      if child.attrib['k'] == 'time':
        if len(the_geom) > 0 and current_segment_datestamp > latest_insert:
          the_geom.append([node.attrib['lat'], node.attrib['lon']])
          insert_batch = insert_into_cartodb(plow_id, datestamp, the_geom, carto, insert_batch)
          insert_count = insert_count + 1

          if insert_count % 1000 == 0:
            print "inserted %s so far" % insert_count;
        the_geom = []
        current_segment_datestamp = datetime.datetime.strptime(child.attrib['v'], "%m/%d/%Y %I:%M:%S %p")

      datestamp = current_segment_datestamp
      the_geom.append([node.attrib['lat'], node.attrib['lon']])