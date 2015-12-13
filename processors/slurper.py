import os
import json
import datetime
import time
import requests
import sqlalchemy as sa

from processors.config import DB_CONN
from processors.poller import poll

class Slurper(object):
    
    def __init__(self, test_mode=False):
        self.engine = sa.create_engine(DB_CONN)
        self.time_format = "%a %b %d %H:%M:%S %Z %Y"
        self.test_mode = test_mode

        self.gps_data_url = "https://gisapps.cityofchicago.org/PlowTrackerWeb/services/plowtrackerservice/getTrackingData"
        
        self.fault_sleep = 10
        
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
    
    def run(self, recreate=False):

        self.initializeDB(recreate=recreate)

        for route_point in self.fetchData():
            
            if route_point:
                self.insertPoints(route_point)
            

    
    def initializeDB(self, recreate=False):

        if recreate:
            self.route_points_table.drop(bind=self.engine, checkfirst=True)
            self.assets_table.drop(bind=self.engine, checkfirst=True)

        self.route_points_table.create(bind=self.engine, checkfirst=True)
        self.assets_table.create(bind=self.engine, checkfirst=True)

    def formatTime(self, s, time_format) :
        return datetime.datetime(*time.strptime(s, time_format)[:6])
    
    def writeRawResponse(self):
        # Used only to get raw responses for testing / debugging
        now = int(datetime.datetime.now().timestamp())
        
        response = next(self.fetchData())
        with open('%s.json' % now, 'w') as f:
            f.write(json.dumps(response))


    def fetchData(self):
        if self.test_mode:
            from os.path import abspath, join, dirname

            test_feed_dir = abspath(join(dirname(__file__), '..', 'test_data'))

            for test_file in sorted(os.listdir(test_feed_dir)):
                
                test_file_path = abspath(join(test_feed_dir, test_file))
                test_feed = json.load(open(test_file_path))
                
                yield test_feed['TrackingDataResponse']['locationList']
        
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
                    print(e)
                    time.sleep(self.fault_sleep)
                    continue

                try:
                    feed = response.json()
                    locations = feed['TrackingDataResponse']['locationList']
                    yield locations
                except KeyError :
                    print("Expected 'TrackingResponse' and 'locationList' not in response")
                    time.sleep(self.fault_sleep)
                    continue

        for locations in poll(data()) :
            yield locations

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
                

            point['posting_time'] = self.formatTime(point['posting_time'], 
                                                    self.time_format)
            
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

