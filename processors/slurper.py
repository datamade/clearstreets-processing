import os
import json
import datetime
import time
import requests
import sqlalchemy as sa
import logging
import boto
from boto.s3.key import Key
from io import BytesIO

from processors.config import DB_CONN, AWS_KEY, AWS_SECRET, S3_BUCKET, \
    CARTODB_SETTINGS
from processors.poll import poll

class Slurper(object):
    
    def __init__(self):
        self.engine = sa.create_engine(DB_CONN)
        self.time_format = "%a %b %d %H:%M:%S %Z %Y"

        self.gps_data_url = "https://gisapps.cityofchicago.org/PlowTrackerWeb/services/plowtrackerservice/getTrackingData"
        
        self.route_points_table = sa.Table('route_points', sa.MetaData(),
                                      sa.Column('id', 
                                                sa.Integer, 
                                                primary_key=True),
                                      sa.Column('object_id', sa.Integer),
                                      sa.Column('posting_time', sa.DateTime),
                                      sa.Column('direction', sa.Integer),
                                      sa.Column('x', sa.Float),
                                      sa.Column('y', sa.Float),
                                      sa.Column('lat', sa.Float),
                                      sa.Column('lon', sa.Float),
                                      sa.Column('inserted', sa.Boolean, 
                                                server_default=sa.text('FALSE')),
                                      sa.UniqueConstraint('object_id', 
                                                          'posting_time'))

        
        self.assets_table = sa.Table('assets', sa.MetaData(),
                                     sa.Column('object_id', 
                                               sa.Integer, 
                                               primary_key=True),
                                     sa.Column('asset_name', sa.String),
                                     sa.Column('asset_type', sa.String))
    
    def initializeDB(self, recreate=False):

        if recreate:
            self.backup()
            self.deleteFromCartoDB()
            self.route_points_table.drop(bind=self.engine, checkfirst=True)
            self.assets_table.drop(bind=self.engine, checkfirst=True)

        self.route_points_table.create(bind=self.engine, checkfirst=True)
        self.assets_table.create(bind=self.engine, checkfirst=True)

    def fetchData(self):
        
        def data() :
            payload = {"TrackingDataInput":{"envelope":{"minX":0,
                                                        "minY":0,
                                                        "maxX":0,
                                                        "maxY":0}}}

            while True:
                try:
                    response = requests.post(self.gps_data_url, 
                                             data=json.dumps(payload))
                except Exception as e :
                    logging.warn(e)
                    time.sleep(10)
                    continue

                yield response.json()

        for locations in poll(data()) :
            try:
                yield locations['TrackingDataResponse']['locationList']
            except KeyError :
                logging.warn("Expected 'TrackingResponse' and 'locationList' not in response")


    def insertPoints(self, route_points):
        # This is inside the loop as an act of perhaps irrational
        # defensive programming, as the script stopped updating the db for
        # no apparent reasons and without throwing an error.
        
        for route_point in route_points:
            
            point = {}
            
            (point['object_id'],
             asset_name,
             asset_type,
             point['posting_time'],
             point['direction'],
             point['x'],
             point['y'],
             point['lat'],
             point['lon']) = (int(route_point['assetName'].replace("S","")), # cast the assetName to an integer
                   route_point['assetName'],
                   route_point['assetType'],
                   route_point['postingTimeFormatted'],
                   route_point['directionDegrees'],
                   route_point['XCoord'],
                   route_point['YCoord'],
                   route_point['latitude'],
                   route_point['longitude'])
                

            point['posting_time'] = self.formatTime(point['posting_time'])
            
            conn = self.engine.connect()
            trans = conn.begin()

            try:

                conn.execute(self.route_points_table.insert(), **point)
                trans.commit()

            except sa.exc.IntegrityError:
                trans.rollback()
                trans = conn.begin()

                update_stmt = self.route_points_table.update()\
                                  .where(self.route_points_table.c.object_id == point['object_id'])\
                                  .where(self.route_points_table.c.posting_time == point['posting_time'])

                conn.execute(update_stmt.values(**point))
                trans.commit()

            trans = conn.begin()

            try:

                asset_info = {
                    'object_id': point['object_id'],
                    'asset_name': asset_name,
                    'asset_type': asset_type
                }

                conn.execute(self.assets_table.insert(), **asset_info)
                trans.commit()

            except sa.exc.IntegrityError:
                trans.rollback()
            
            conn.close()
            self.engine.dispose()

    def formatTime(self, s) :
        return datetime.datetime(*time.strptime(s, self.time_format)[:6])

    def run(self, recreate=False):

        self.initializeDB(recreate=recreate)

        for route_point in self.fetchData():
            self.insertPoints(route_point)

    def backup(self):

        conn = self.engine.raw_connection()

        s3conn = boto.connect_s3(AWS_KEY, AWS_SECRET)
        bucket = s3conn.get_bucket(S3_BUCKET)
        
        now = datetime.datetime.now().strftime('%m-%d-%Y_%H:%M')

        for table in ['route_points', 'assets']:
            copy = ''' 
                COPY (SELECT * FROM {table})
                TO STDOUT WITH CSV HEADER DELIMITER ','
            '''.format(table=table)
            
            fname = 'backups/{now}_{table}.csv'.format(now=now, 
                                                       table=table)
            
            with open(fname, 'w') as f:
                curs = conn.cursor()
                curs.copy_expert(copy, f)
            
            key = Key(bucket)
            key.key = fname
            key.set_contents_from_filename(fname)
            key.set_acl('public-read')

        conn.close()
        
        params = {
            'q': 'SELECT * FROM {table}'.format(table=CARTODB_SETTINGS['table']),
            'format': 'geojson',
        }
        
        url = 'https://{user}.cartodb.com/api/v2/sql'.format(user=CARTODB_SETTINGS['user'])
        
        geojson_dump = requests.get(url, params=params)
        
        key.key = 'backups/cartodb_{now}.geojson'.format(now=now)
        key.set_contents_from_file(BytesIO(geojson_dump.content))
        key.set_acl('public-read')

        s3conn.close()
    
    def deleteFromCartoDB(self):
        
        params = {
            'q': 'DELETE * FROM {table}'.format(CARTODB_SETTINGS['table']),
            'api_key': CARTODB_SETTINGS['api_key'],
        }
        
        url = 'https://{user}.cartodb.com/api/v2/sql'.format(user=CARTODB_SETTINGS['user'])

        delete = requests.get(url, params=params)


class TestSlurper(Slurper) :
    def fetchData(self):
        from os.path import abspath, join, dirname

        test_feed_dir = abspath(join(dirname(__file__), '..', 'test_data'))

        for test_file in sorted(os.listdir(test_feed_dir)):
                
            test_file_path = abspath(join(test_feed_dir, test_file))
            test_feed = json.load(open(test_file_path))
                
            yield test_feed['TrackingDataResponse']['locationList']

    def writeRawResponse(self):
        now = int(datetime.datetime.now().timestamp())
        
        response = next(self.fetchData())
        with open('%s.json' % now, 'w') as f:
            f.write(json.dumps(response))




