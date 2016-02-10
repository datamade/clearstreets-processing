import sqlalchemy as sa
from processors.config import OSRM_ENDPOINT, DB_CONN, CARTODB_SETTINGS
import requests
from requests.exceptions import ConnectionError
import json
import os
from datetime import datetime
from .slurper import Slurper
import time

class Tracer(object):

    def __init__(self, plow_ids=[], test_mode=False):
        self.osrm_endpoint = OSRM_ENDPOINT
        self.engine = sa.create_engine(DB_CONN)
        self.test_mode = test_mode

        self.point_limit = 40
        self.matching_beta = 5
        self.gps_precision = 10
        self.plow_ids = plow_ids
        
        self.overlap = 10

    def run(self):
        for asset in self.iterAssets():
            points = [dict(zip(r.keys(), r.values())) for r in self.getRecentPoints(asset)]

            if points:
                trace_resp = self.getTrace(points)
            
            else:
                continue
            if trace_resp.json()['status'] == 200:
                asset_geojson, error = self.createTraceGeoJSON(trace_resp.json())
                
                if not error:
                    last_posting_time = max([p['posting_time'] for p in points])
                    inserted = self.insertCartoDB(asset.object_id, asset_geojson, last_posting_time)
                

                    if inserted:
                        self.updateLocalTable([p['id'] for p in points])
                else:
                    print(error, asset.object_id, last_posting_time)
            elif trace_resp.json()['status'] == 208:
                if points:
                    earliest_point = min(points, key=lambda x: x['posting_time'])
                    self.markUnmatchable(earliest_point['id'])

            else:
                print(trace_resp.url, asset.object_id)
                print(trace_resp.json())

    def dumpGeoJSON(self):
        for asset in self.iterAssets():
            points = [dict(zip(r.keys(), r.values())) for r in self.getRecentPoints(asset)]
            trace_resp = self.getTrace(points)
            
            asset_collection = {
                'type': 'FeatureCollection',
                'features': []
            }

            if trace_resp.status_code == 200:
                asset_geojson, error = self.createTraceGeoJSON(trace_resp)
                
                if not error:
                    feature = {
                        'type': 'Feature',
                        'geometry': asset_geojson,
                        'properties': {}
                    }
                    asset_collection['features'].append(feature)
                    
                    self.updateLocalTable(point_ids)
            
            dirname = 'output_{sigma}_{beta}'.format(sigma=self.gps_precision, 
                                                     beta=self.matching_beta)
            try:
                os.mkdir(dirname)
            except FileExistsError:
                pass
           
            filename = '{0}/{1}.geojson'.format(dirname, asset.object_id) 
            if os.path.exists(filename):
                contents = json.load(open(filename))
                contents['features'].extend(asset_collection['features'])
                asset_collection = contents

            with open(filename, 'w') as f:
                f.write(json.dumps(asset_collection))


    def iterAssets(self):
        
        assets = 'SELECT * FROM assets'
        query_kwargs = {}

        if self.plow_ids:
            assets = sa.text('SELECT * FROM assets WHERE object_id IN :plow_ids')
            query_kwargs['plow_ids'] = tuple(self.plow_ids)
            

        assets = self.engine.execute(assets, **query_kwargs)

        for asset in assets:
            yield asset
    
    def pointQuery(self):
        # Find max posting time where inserted is true
        # Return all points newer than that
        # Return max inserted point and N previous points
        
        return ''' 
            SELECT * FROM (
              SELECT * FROM route_points
              WHERE object_id = :object_id
                AND unmatchable = FALSE
                AND posting_time > COALESCE((
                  SELECT 
                    MAX(posting_time)
                  FROM route_points
                  WHERE inserted = TRUE
                    AND object_id = :object_id
              ), '1900-01-01'::timestamp)
              
              UNION
             
              SELECT * FROM route_points
              WHERE object_id = :object_id
                AND unmatchable = FALSE
                AND posting_time <= COALESCE((
                  SELECT 
                    MAX(posting_time)
                  FROM route_points
                  WHERE inserted = TRUE
                    AND object_id = :object_id
              ), '1900-01-01'::timestamp)
              ORDER BY posting_time DESC
              LIMIT {overlap}
              ) AS subq
            ORDER BY posting_time
            LIMIT 50

        '''.format(overlap=self.overlap)
        

    def testPointQuery(self):
        return ''' 
            SELECT * FROM route_points
            WHERE inserted = FALSE
              AND object_id = :object_id
            LIMIT 10
        '''

    def getRecentPoints(self, asset):
        query = self.pointQuery()

        if self.test_mode:
            query = self.testPointQuery()

        recent_points = self.engine.execute(sa.text(query), 
                                            object_id=asset.object_id)
        
        return recent_points

    def getTrace(self, points):
        
        point_fmt = 'loc={lat},{lon}&t={timestamp}&matching_beta={matching_beta}&gps_precision={gps_precision}'
        
        query = []
        posting_times = []
        point_ids = []

        for point in points:
            posting_timestamp = int(point['posting_time'].timestamp())
            posting_times.append(point['posting_time'])

            point_query = point_fmt.format(lat=point['lat'], 
                                           lon=point['lon'], 
                                           timestamp=posting_timestamp,
                                           matching_beta=self.matching_beta,
                                           gps_precision=self.gps_precision)
            query.append(point_query)
            point_ids.append(point['id'])
        
            
        query = '&'.join(query)
        query_url = '{0}?compression=false&{1}'.format(self.osrm_endpoint, query)
        
        points = sorted(points, 
                        key=lambda x: x['posting_time'], 
                        reverse=True)

        try:
            trace_resp = requests.get(query_url)

                # geojson = {'type': 'FeatureCollection', 'features': []}

                # for point in points:
                #     feat = {
                #         'type': 'Feature', 
                #         'geometry': {
                #             'type': 'Point', 
                #             'coordinates': [point['lon'], point['lat']]
                #         },
                #         'properties': {
                #             'timestamp': point['posting_time'].isoformat(),
                #             'object_id': point['object_id']
                #         }
                #     }
                #     geojson['features'].append(feat)
                # print(json.dumps(geojson))
                # 
                # print('point count', len(points))
                
        except ConnectionError:
            return None
        
        return trace_resp

    
    def createTraceGeoJSON(self, trace_resp):
        
        try:
            matchings = trace_resp['matchings']
        except KeyError:
            return None, trace_resp
        
        flipped_geometry = []

        for lat, lon in matchings[0]['geometry']:
            flipped_geometry.append([lon, lat])

        feature =  {
            'type': 'LineString',
            'coordinates': flipped_geometry,
            'crs': {"type": "name", "properties": {"name": "EPSG:4326"}},
        }
        
        return feature, None

    def insertCartoDB(self, asset_id, geojson, date_stamp):
        # After inserting update local table with inserted flag
        
        if geojson:
            
            insert = ''' 
                INSERT INTO {table}
                  (id, date_stamp, the_geom)
                VALUES ('{id}', '{date_stamp}', ST_GeomFromGeoJSON('{geojson}'))
            '''.format(table=CARTODB_SETTINGS['table'],
                       id=asset_id,
                       date_stamp=date_stamp,
                       geojson=json.dumps(geojson))
            
            user =  CARTODB_SETTINGS['user']
            api_key = CARTODB_SETTINGS['api_key']
            
            params = {
                'q': insert,
                'api_key': api_key,
            }
            
            url = 'https://{user}.cartodb.com/api/v2/sql'.format(user=user)

            carto = requests.post(url, data=params)
            
            if carto.status_code != 200:
                print('CartoDB returned an error', carto.content)
                return False
            
            return True
        
        return False
    
    def markUnmatchable(self, point_id):
        mark = ''' 
            UPDATE route_points SET
              unmatchable = TRUE
            WHERE id = :point_id
        '''

        with self.engine.begin() as conn:
            conn.execute(sa.text(mark), point_id=point_id)
    
    def updateLocalTable(self, points):
        update = ''' 
            UPDATE route_points SET
              inserted = TRUE
            WHERE id IN :ids
        '''

        ids = tuple([r for r in points])
        
        if ids:
            
            with self.engine.begin() as conn:
                conn.execute(sa.text(update), ids=ids)
