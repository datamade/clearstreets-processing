import json
import sqlite3
import time
import datetime
import urllib2
from time import sleep
from collections import defaultdict, deque
import numpy
from scipy.optimize import fsolve

# Helper function for dealing with time stamps
time_format = "%a %b %d %H:%M:%S %Z %Y"
def formatTime(s, time_format) :
    return datetime.datetime(*time.strptime(s, time_format)[:6])


# Rate of change estimator for irregularly sampled data
# we will want to solve for x
#
# Cho and Garcia Molina, 2003, Estimating Frequency of Change
# http://dl.acm.org/citation.cfm?id=857170
def irregularCGM(intervals, Z) :
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


# Set up DB
con = sqlite3.connect("plow.db")
cur = con.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS route_points (object_id INTEGER, posting_time DATETIME, X DOUBLE, Y DOUBLE, UNIQUE(object_id, posting_time) ON CONFLICT REPLACE)")
cur.execute("CREATE TABLE IF NOT EXISTS assets (object_id INTEGER, asset_name TEXT, asset_type TEXT, PRIMARY KEY(object_id) ON CONFLICT REPLACE)")

# The feed for City Of Chicago's Plow Data
gps_data_url = "https://gisapps.cityofchicago.org/ArcGISRest/services/ExternalApps/operational/MapServer/38/query?where=POSTING_TIME+>+SYSDATE-1+&returnGeometry=true&outSR=4326&outFields=ADDRESS,POSTING_TIME,ASSET_NAME,ASSET_TYPE,OBJECTID&f=pjson"


# We'll use these variables to keep track of whether we observe a
# new plow position
previous_posting_time = datetime.datetime(1,1,1)
last_posting_time = datetime.datetime(1,1,1)
update_history = defaultdict( lambda: deque([], 30))

# We want adjust our sampling intervals depending upon our estimated
# rate of updates for the plows
intervals = deque([],30)
sampling_frequency = 10
fault_sleep = 60
fault = False

while True:
    query = urllib2.Request(gps_data_url)

    # Try to handle anything besides a well-formed json response
    try:
        response = urllib2.urlopen(query).read()
    except Exception as e :
        print e
        sleep(fault_sleep)
        fault = True
        continue
    if "Sorry, servers are currently down" in response:
        print "Sorry, servers are currently down"
        sleep(fault_sleep)
        fault = True
        continue

    read_data = json.loads(response)

    for route_point in read_data['features'] :
        
        (object_id,
         asset_name,
         asset_type,
         posting_time,
         x,
         y) = (route_point['attributes']['OBJECTID'],
               route_point['attributes']['ASSET_NAME'],
               route_point['attributes']['ASSET_TYPE'],
               route_point['attributes']['POSTING_TIME'],
               route_point['geometry']['x'],
               route_point['geometry']['y'])

        posting_time = formatTime(posting_time, time_format)

        # Insert Data into DB
        cur.execute("""insert into route_points (object_id, posting_time, X, Y)
                       values (?, ?, ?, ?)""",
                    (object_id, posting_time, x, y))
        cur.execute("""insert into assets (object_id, asset_name, asset_type)
                       values (?, ?, ?)""",
                    (object_id, asset_name, asset_type))

        # Update whether or not we observed a new position for every
        # plow
        if posting_time > last_posting_time :
            last_posting_time = posting_time
            
        if posting_time > previous_posting_time :
            update_history[object_id].append(1)
        else :
            update_history[object_id].append(0)

    # Add the sampling interval
    previous_posting_time = last_posting_time
    if fault :
        intervals.append(sampling_frequency + fault_sleep)
        fault = False
    else :
        intervals.append(sampling_frequency)

    # Calculate a new sampling_frequency that we belive will capture
    # each atomic plow update 95% of the time
    r = [] 
    for object_id in update_history :
        z = sum(update_history[object_id])
        if z > 0 :
            icgm = irregularCGM(intervals, update_history[object_id])
            r.append(fsolve(icgm, .01))
            print z
    # Assuming that updates are drawn from a poisson distribution,
    # then with .95% probability, we will observe LESS than 2 events
    # in this period. This does not mean the probability that we will
    # observe 1 update, as it is very likely that we will observe no
    # update.
    #
    # We do not allow the interval to get too small so we don't slam
    # the city's servers.
    sampling_frequency = .355362/(min(max(r), .1))
    print "Sampling Interval: " + str(int(sampling_frequency)) + " seconds"
    
    con.commit()
    sleep(sampling_frequency)    

con.close()

