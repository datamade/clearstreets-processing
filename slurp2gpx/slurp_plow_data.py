import json
import sqlite3
import time
import datetime
import urllib2
from time import sleep

gps_data_url = "https://gisapps.cityofchicago.org/ArcGISRest/services/ExternalApps/operational/MapServer/38/query?f=json&where=OBJECTID%20is%20not%20null%20AND%20POSTING_TIME%20%3E%20SYSDATE-1%2F96&returnGeometry=true&spatialRel=esriSpatialRelIntersects&outFields=ADDRESS%2CPOSTING_TIME%2CASSET_NAME%2CASSET_TYPE%2COBJECTID"

time_format = "%a %b %d %H:%M:%S %Z %Y"

con = sqlite3.connect("plow.db")
cur = con.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS route_points (object_id INTEGER, posting_time DATETIME, X DOUBLE, Y DOUBLE, UNIQUE(object_id, posting_time) ON CONFLICT REPLACE)")
cur.execute("CREATE TABLE IF NOT EXISTS assets (object_id INTEGER, asset_name TEXT, asset_type TEXT, PRIMARY KEY(object_id) ON CONFLICT REPLACE)")

while True:
    query = urllib2.Request(gps_data_url)
    try:
        response = urllib2.urlopen(query).read()
    except :
        sleep(60)
        continue
    if "Sorry, servers are currently down" not in response:
        read_data = json.loads(response)
        for route_point in read_data['features'] :
            cur.execute("""insert into route_points (object_id, posting_time, X, Y)
            values (?, ?, ?, ?)""",
                        (route_point['attributes']['OBJECTID'],
                         datetime.datetime(*time.strptime(route_point['attributes']['POSTING_TIME'], time_format)[:6]),
                         route_point['geometry']['x'],
                         route_point['geometry']['y']))
            cur.execute("""insert into assets (object_id, asset_name, asset_type) values (?, ?, ?)""",
                        (route_point['attributes']['OBJECTID'],
                         route_point['attributes']['ASSET_NAME'],
                         route_point['attributes']['ASSET_TYPE']))
    con.commit()
    print cur.execute("select count(*) from route_points").fetchall()
    sleep(60)    

con.close()
