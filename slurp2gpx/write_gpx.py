import sqlite3
from osgeo import osr
from osgeo import ogr
import time, datetime
import os
import glob

path = "./"

####################################################
## Define our Coordinate Reprojections

# This the projection of the original data
# http://spatialreference.org/ref/esri/102671/
utm_srs = osr.SpatialReference()
utm_srs.SetUTM(11)
utm_srs.ImportFromEPSG(102671)

# Same area but in in lat, long
ll_srs = utm_srs.CloneGeogCS()

xform = osr.CoordinateTransformation(utm_srs, ll_srs)

####################################################
## Set up the driver to write GPX traces
driverName = "GPX"
drv = ogr.GetDriverByName(driverName)

####################################################
## Setup the SQL db
con = sqlite3.connect("plow.db")
cur = con.cursor()

## Create trace for each asset
plows = cur.execute("select asset_name, object_id from assets").fetchall()

while True:
    for plow in plows:
        previous_period = datetime.datetime.now() - datetime.timedelta(hours=1)
        plow_name = plow[0]
        object_id = plow[1]

        plow_track = cur.execute("""
        select * from route_points where object_id == ? and posting_time > ? order by posting_time""",
                                 (object_id, previous_period)).fetchall()

        if len(plow_track) < 2:
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

            (long, lat, z) = xform.TransformPoint(point[2], point[3])
            pt = ogr.Geometry(ogr.wkbPoint)
            pt.SetPoint_2D(0, long, lat)
            feature.SetGeometry(pt)
                
            layer.CreateFeature(feature)

    
            feature.Destroy()

        layer.SyncToDisk()
        ds = None
    time.sleep(10)        


con.close()

