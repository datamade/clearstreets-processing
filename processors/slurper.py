import json
import time
import datetime
import requests
from time import sleep
from collections import defaultdict, deque
from math import exp
from scipy.optimize import fsolve
import sqlalchemy as sa

from processors.config import DB_CONN

class Slurper(object):
    
    def __init__(self, test_mode=False):
        self.engine = sa.create_engine(DB_CONN)
        self.time_format = "%a %b %d %H:%M:%S %Z %Y"
        self.test_mode = test_mode

        self.gps_data_url = "https://gisapps.cityofchicago.org/PlowTrackerWeb/services/plowtrackerservice/getTrackingData"
        
        # We'll use these variables to keep track of whether we observe a new
        # plow position. 30 observations should be sufficient for a reasonable
        # estimate.
        self.previous_posting_time = datetime.datetime(1,1,1)
        self.last_posting_time = datetime.datetime(1,1,1)
        self.update_history = defaultdict( lambda: deque([], 30))
        self.intervals = deque([],30)

        # We want adjust our sampling intervals depending upon our estimated
        # rate of updates for the plows
        self.sampling_frequency = 10
        self.updates = 0

        # Even if we are unable to get any data, we need to keep track of the
        # time we spent on that attempt
        self.fault_sleep = 60
        self.faults = 0
        
        self.route_points_table = sa.Table('route_points', sa.MetaData(),
                                      sa.Column('object_id', sa.Integer),
                                      sa.Column('posting_time', sa.DateTime),
                                      sa.Column('direction', sa.Integer),
                                      sa.Column('x', sa.Float),
                                      sa.Column('y', sa.Float),
                                      sa.UniqueConstraint('object_id', 'posting_time'))

        
        self.assets_table = sa.Table('assets', sa.MetaData(),
                                     sa.Column('object_id', 
                                               sa.Integer, 
                                               primary_key=True),
                                     sa.Column('asset_name', sa.String),
                                     sa.Column('asset_type', sa.String))
    
    def run(self, recreate=False):

        self.initializeDB(recreate=recreate)

        while True:
            self.insertData()
            self.incrementSamplingInterval()

    
    def initializeDB(self, recreate=False):

        if recreate:
            self.route_points_table.drop(bind=self.engine, checkfirst=True)
            self.assets_table.drop(bind=self.engine, checkfirst=True)

        self.route_points_table.create(bind=self.engine, checkfirst=True)
        self.assets_table.create(bind=self.engine, checkfirst=True)

    def formatTime(self, s, time_format) :
        return datetime.datetime(*time.strptime(s, time_format)[:6])
    
    def irregularCGM(self, intervals, Z) :
        # Rate of change estimator for irregularly sampled data
        # we will want to solve for x
        #
        # Cho and Garcia Molina, 2003, Estimating Frequency of Change
        # http://dl.acm.org/citation.cfm?id=857170
        def iCGM(x) :
            L = 0
            K = 0
            
            for i, z in enumerate(Z) :
                if z == 1 :
                    L = L + intervals[i]/(exp(x*intervals[i])-1)
                else :
                    K = K + intervals[i]
            return L - K
        
        return iCGM
    
    def fetchData(self):
        if self.test_mode:
            from os.path import abspath, join, dirname

            test_feed_file = abspath(join(dirname(__file__), 'test_feed.json'))
            test_feed = json.load(open(test_feed_file))
            
            return test_feed['TrackingDataResponse']['locationList']

        try:
            payload = {"TrackingDataInput":{"envelope":{"minX":0,"minY":0,"maxX":0,"maxY":0}}}
            response = requests.post(self.gps_data_url, data=json.dumps(payload))
        except Exception as e :
            print e
            sleep(fault_sleep)
            self.faults += 1
        
        try:
            read_data = response.json()['TrackingDataResponse']['locationList']
        except Exception as e :
            print "Expected 'TrackingResponse' and 'locationList' not in response"
            sleep(fault_sleep)
            self.faults += 1
        
        return read_data

    def insertPoints(self):
        # This is inside the loop as an act of perhaps irrational
        # defensive programming, as the script stopped updating the db for
        # no apparent reasons and without throwing an error.

        for route_point in self.fetchData():
            
            point = {}
            
            try: 
                (point['object_id'],
                 asset_name,
                 asset_type,
                 point['posting_time'],
                 point['direction'],
                 point['x'],
                 point['y']) = (int(route_point['assetName'].replace("S","")), # cast the assetName to an integer
                       route_point['assetName'],
                       route_point['assetType'],
                       route_point['postingTimeFormatted'],
                       route_point['directionDegrees'],
                       route_point['XCoord'],
                       route_point['YCoord'])
            except TypeError:
                continue

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

            if point['posting_time'] > self.last_posting_time :
                self.last_posting_time = point['posting_time']
                
            if point['posting_time'] > self.previous_posting_time :
                self.update_history[point['object_id']].append(1)
                self.updates += 1
            else :
                self.update_history[point['object_id']].append(0)

    def incrementSamplingInterval(self):

        # Add the sampling interval
        previous_posting_time = last_posting_time
        if self.faults :
            self.intervals.append(self.sampling_frequency
                             + self.faults*self.fault_sleep)
            self.faults = 0
        else :
            self.intervals.append(self.sampling_frequency)

        # Estimate the update rate
        r = []
        try: 
            for object_id in self.update_history :
                z = sum(self.update_history[object_id])
                if z > 0 :
                    icgm = self.irregularCGM(intervals, self.update_history[object_id])
                    r.append(fsolve(icgm, .01))
        except Exception as e :
            print e

        # Assuming that updates are drawn from a Poisson distribution,
        # then with some probability, we will observe LESS than 2 events
        # in this period. This does not mean the probability that we will
        # observe 1 update, as it is very likely that we will observe no
        # update.
        #
        # We also do not allow the interval to get too small so we don't
        # slam the city's servers.
        #
        # .95 : .355362
        # .90 : .531812
        # .80 : .824388
        #
        # http://www.wolframalpha.com/input/?i=exp(-bx)%2Bbxexp(-bx)%3D.8

        if len(r) > 0:
            self.sampling_frequency = max(.824388/max(r), 10)
            print "Estimated Average Update Interval: " + str(int(1/max(r))) + " seconds"
            print "Sampling Interval:                 " + str(int(self.sampling_frequency)) + " seconds"
            print "Updates:                           " + str(self.updates)
            print ""
        else:
            print "Sampling Interval:                 " + str(int(self.sampling_frequency)) + " seconds"
        
        sleep(self.sampling_frequency)    
