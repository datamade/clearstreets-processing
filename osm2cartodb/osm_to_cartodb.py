from os import listdir
from os.path import isfile, join
import datetime
import time

from cartodb_settings import CARTODB_SETTINGS
from cartodb import CartoDBAPIKey, CartoDBException
import xml.etree.ElementTree as ET

def get_latest_insert(plow_id, carto):
  latest = datetime.datetime(1990, 1, 1)
  try:
    query = carto.sql("select date_stamp from %s where id = '%s' ORDER BY date_stamp DESC LIMIT 1" % (CARTODB_SETTINGS['table'], plow_id))
    
    if len(query['rows']):
      row = query['rows'][0]

      if 'date_stamp' in row.keys():
        latest = datetime.datetime.strptime(row['date_stamp'], "%Y-%m-%d %H:%M:%S")

    return latest

  except CartoDBException as e:
    print ("some error ocurred", e)

def add_to_insert_batch(plow_id, datestamp, the_geom, carto, insert_batch, batch_size):

  # print plow_id, '-', datestamp

  the_geom_wkt = ''
  for point in the_geom:
    the_geom_wkt += point[1] + " " + point[0] + ","

  the_geom_wkt = the_geom_wkt[:-1]

  if len(insert_batch) < batch_size:
    insert_batch.append([plow_id, datestamp, the_geom_wkt])

  else:
    commit_insert_batch(carto, insert_batch)

  return insert_batch

def commit_insert_batch(carto, insert_batch):
  insert_sql = "INSERT INTO %s (id, date_stamp, the_geom) VALUES " % (CARTODB_SETTINGS['table'])

  for item in insert_batch:
    insert_sql += " ('%s', '%s', ST_GeomFromText('LINESTRING(%s)', 4326))," % (item[0], item[1], item[2])

  insert_sql = insert_sql[:-1]  
  # print 'insert_sql', insert_sql

  try:
    carto.sql(insert_sql)
    insert_batch = []
  except CartoDBException as e:
    print ("some error ocurred", e)

def clear_out_table(carto):
  print "clearing out table"
  try:
    carto.sql("delete from %s" % CARTODB_SETTINGS['table'])
  except CartoDBException as e:
    print ("some error ocurred", e)

# setup
user =  CARTODB_SETTINGS['user']
API_KEY = CARTODB_SETTINGS['api_key']
cartodb_domain = CARTODB_SETTINGS['domain']
carto = CartoDBAPIKey(API_KEY, cartodb_domain)

# clear_out_table(carto)

while True:

  insert_batch = []
  batch_size = 1000
  insert_count = 0
  osm_files = [ f for f in listdir('../osm') if isfile(join('../osm',f)) and '.osm' in f]

  print 'OSM files:'
  print osm_files

  for osm_file in osm_files:

    plow_id = osm_file.split("_")[0]
    datestamp = ''
    the_geom = []

    latest_insert = get_latest_insert(plow_id, carto)
    current_segment_datestamp = datetime.datetime(1990, 1, 2)

    # read OSM file
    tree = ET.parse("../osm/%s" % osm_file)
    for node in tree.getroot().iter('node'):
      # print current_segment_datestamp
      # print node.attrib
      for child in node:
        # print child.attrib
        if child.attrib['k'] == 'time':
          if len(the_geom) > 0 and current_segment_datestamp > latest_insert:
            # print current_segment_datestamp, '>', latest_insert
            the_geom.append([node.attrib['lat'], node.attrib['lon']])
            insert_batch = add_to_insert_batch(plow_id, datestamp, the_geom, carto, insert_batch, batch_size)
            insert_count = insert_count + 1

            if insert_count % batch_size == 0:
              commit_insert_batch(carto, insert_batch)
              insert_batch = []
              print "inserted %s so far" % insert_count;
          the_geom = []
          current_segment_datestamp = datetime.datetime.strptime(child.attrib['v'], "%m/%d/%Y %I:%M:%S %p")

        datestamp = current_segment_datestamp
        the_geom.append([node.attrib['lat'], node.attrib['lon']])

  # do one final insert for the remainder
  if len(insert_batch) > 0:
    commit_insert_batch(carto, insert_batch)
    print "inserted the remaining %s" % len(insert_batch);

  print 'sleeping for 5 min'
  time.sleep(300)