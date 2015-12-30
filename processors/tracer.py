import sqlalchemy as sa
from processors.config import OSRM_ENDPOINT, DB_CONN, CARTODB_SETTINGS
import requests
import json

class Tracer(object):

    def __init__(self):
        self.osrm_endpoint = OSRM_ENDPOINT
        self.engine = sa.create_engine(DB_CONN)
        
        self.point_limit = 40
        self.matching_beta = 5

    def run(self):
        for asset in self.iterAssets():
            points = self.getRecentPoints(asset)
            trace_resp, last_posting_time, point_ids = self.getTrace(points)
            
            if trace_resp:
                asset_geojson, error = self.createTraceGeoJSON(trace_resp)
                
                if not error:
                    inserted = self.insertCartoDB(asset.object_id, asset_geojson, last_posting_time)
                

                    if inserted:
                        self.updateLocalTable(point_ids)
                else:
                    print(error, asset.object_id, last_posting_time)


    def iterAssets(self):
        assets = self.engine.execute('SELECT * FROM assets')

        for asset in assets:
            yield asset
    
    def getRecentPoints(self, asset):
        recent_points = ''' 
            (
              SELECT * FROM (
                SELECT * 
                FROM route_points
                WHERE object_id = :object_id
                  AND inserted = FALSE
                ORDER BY posting_time DESC
              ) AS s
              ORDER BY posting_time ASC
              LIMIT :limit
            ) UNION (
              SELECT *
                FROM route_points
              WHERE object_id = :object_id
                AND inserted = TRUE
              ORDER BY posting_time DESC
              LIMIT 1
            )
            ORDER BY posting_time ASC
        '''

        recent_points = self.engine.execute(sa.text(recent_points), 
                                            object_id=asset.object_id,
                                            limit=self.point_limit)
        
        return recent_points

    def getTrace(self, points):
        
        point_fmt = 'loc={lat},{lon}&t={timestamp}&matching_beta={matching_beta}'
        
        query = []
        posting_times = []
        point_ids = []

        for point in points:
            posting_timestamp = int(point.posting_time.timestamp())
            posting_times.append(point.posting_time)

            point_query = point_fmt.format(lat=point.lat, 
                                           lon=point.lon, 
                                           timestamp=posting_timestamp,
                                           matching_beta=self.matching_beta)
            query.append(point_query)
            point_ids.append(point.id)
        
        if len(point_ids) > 10:
            
            query = '&'.join(query)
            
            query_url = '{0}?compression=false&{1}'.format(self.osrm_endpoint, query)

            trace_resp = requests.get(query_url)
            
            return trace_resp.json(), max(posting_times), point_ids

        return None, None, None
    
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
                print('CartoDB returned an error', cartodb.content)
                return False
            
            return True
        
        return False

    
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
