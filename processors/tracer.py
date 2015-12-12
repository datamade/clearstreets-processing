import sqlalchemy as sa
from processors.config import OSRM_ENDPOINT, DB_CONN, CARTODB_SETTINGS
import requests

class Tracer(object):

    def __init__(self):
        self.osrm_endpoint = OSRM_ENDPOINT
        self.engine = sa.create_engine(DB_CONN)

        self.point_limit = 40

    def run(self):
        for asset in self.iterAssets():
            points = self.getRecentPoints(asset)
            trace_resp, last_posting_time = self.getTrace(points)
            if trace_resp:
                asset_geojson = self.createTraceGeoJSON(trace_resp)
                self.insertCartoDB(asset.object_id, asset_geojson, last_posting_time)
                self.updateLocalTable(points)

    def iterAssets(self):
        assets = self.engine.execute('SELECT * FROM assets')

        for asset in assets:
            yield asset
    
    def getRecentPoints(self, asset):
        recent_points = ''' 
            SELECT * FROM (
                SELECT * 
                FROM route_points
                WHERE object_id = :object_id
                  AND inserted = FALSE
                ORDER BY posting_time DESC
            ) AS s
            ORDER BY posting_time ASC
        '''

        recent_points = self.engine.execute(sa.text(recent_points), 
                                            object_id=asset.object_id,
                                            limit=self.point_limit)
        
        return recent_points

    def getTrace(self, points):
        
        point_fmt = 'loc={lat},{lon}&t={timestamp}'
        
        query = []
        posting_times = []

        for point in points:
            posting_timestamp = int(point.posting_time.timestamp())
            posting_times.append(point.posting_time)

            point_query = point_fmt.format(lat=point.lat, 
                                           lon=point.lon, 
                                           timestamp=posting_timestamp)
            query.append(point_query)

        query = '&'.join(query)
        
        query_url = '{0}?compression=false&{1}'.format(self.osrm_endpoint, query)

        trace_resp = requests.get(query_url)

        return trace_resp.json(), max(posting_times)
    
    def createTraceGeoJSON(self, trace_resp):
        
        try:
            matchings = trace_resp['matchings']
        except KeyError:
            return None

        feature =  {
            'type': 'LineString',
            'coordinates': matchings[0]['geometry'],
            'crs': {"type": "name", "properties": {"name": "EPSG:4326"}},
        }
        
        return feature

    def insertCartoDB(self, asset_id, geojson, date_stamp):
        # After inserting update local table with inserted flag
        insert = ''' 
            INSERT INTO {table}
              (id, date_stamp, the_geom)
            VALUES ('{id}', '{date_stamp}', ST_GeomFromGeoJSON('{geojson}')
        '''.format(table=CARTODB_SETTINGS['table'],
                   id=asset_id,
                   date_stamp=date_stamp,
                   geojson=geojson)
        
        user =  CARTODB_SETTINGS['user']
        API_KEY = CARTODB_SETTINGS['api_key']
        cartodb_domain = CARTODB_SETTINGS['domain']
        
        carto = CartoDBAPIKey(API_KEY, cartodb_domain)
        
        carto.sql(insert)
    
    def updateLocalTable(self, points):
        update = ''' 
            UPDATE route_points SET
              inserted = TRUE
            WHERE id IN :ids
        '''

        ids = tuple([r.id for r in points])
        
        with self.engine.begin() as conn:
            conn.execute(sa.text(update), ids=ids)
