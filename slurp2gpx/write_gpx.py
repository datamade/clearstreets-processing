import sqlite3
from osgeo import ogr
import time, datetime
import os
import glob

def testWindow(l, test_time) :
    times = []
    for point in l :
        times.append(datetime.datetime.strptime(point[1], "%Y-%m-%d %H:%M:%S"))
    return all([time > test_time for time in times])
        

path = "./"

####################################################
## Set up the driver to write GPX traces
driverName = "GPX"
drv = ogr.GetDriverByName(driverName)

count = 0

while True:
    plow_count = 0
    enough_traces = 0

    con = sqlite3.connect("plow.db")
    cur = con.cursor()
    ## Create trace for each asset
    plows = cur.execute("select asset_name, object_id from assets").fetchall()
    
    for plow in plows:
        plow_count += 1
        previous_period = datetime.datetime.now() - datetime.timedelta(hours=1)
        plow_name = plow[0]
        object_id = plow[1]

        # Return the last N points, in order 
        plow_track = cur.execute("""
        select * from (select * from route_points where object_id == ? order by posting_time desc limit ?) order by posting_time asc""",
                                 (object_id, 40)).fetchall()



        if len(plow_track) < 20:
            continue
        else:
	    enough_traces += 1


        if testWindow(plow_track, previous_period) is False :
            continue



        ds = drv.CreateDataSource("../gpx/" + plow_name + ".gpx")
        if ds is None:
            os.remove("../gpx/" + plow_name + ".gpx")
            ds = drv.CreateDataSource("../gpx/" + plow_name + ".gpx")
        
        layer = ds.CreateLayer("track_points", None, ogr.wkbPoint)

        for point in plow_track :
            feature = ogr.Feature( layer.GetLayerDefn())

            wpt_time = datetime.datetime.strptime(point[1], "%Y-%m-%d %H:%M:%S")
            feature.SetField("time",
                             wpt_time.year,
                             wpt_time.month,
                             wpt_time.day,
                             wpt_time.hour,
                             wpt_time.minute,
                             wpt_time.second,
                             0)

            feature.SetField("track_seg_id", 1 )
            feature.SetField("track_fid", 1)

            (long, lat) = (point[2], point[3])
            pt = ogr.Geometry(ogr.wkbPoint)
            pt.SetPoint_2D(0, long, lat)
            feature.SetGeometry(pt)
                
            layer.CreateFeature(feature)

    
            feature.Destroy()

        layer.SyncToDisk()
        ds = None

    con.close()
    count += 1
    print 'iteration', count, ':', enough_traces, 'traces /', plow_count, 'plows'
    time.sleep(10)        




