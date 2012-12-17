-- DROP TABLE IF EXISTS assets;
-- DROP TABLE IF EXISTS route_points;

CREATE TABLE assets (object_id INTEGER, asset_name VARCHAR, asset_type VARCHAR);
CREATE TABLE route_points (object_id INTEGER, posting_time TIMESTAMP, X FLOAT, Y FLOAT);

COPY assets FROM '/Users/samarthbhaskar/Desktop/Clear_streets_v2/raw_gps/assets1.csv' DELIMITERS ',' CSV;
COPY assets FROM '/Users/samarthbhaskar/Desktop/Clear_streets_v2/raw_gps/assets2.csv' DELIMITERS '|' CSV;

COPY route_points FROM '/Users/samarthbhaskar/Desktop/Clear_streets_v2/raw_gps/route_points1.csv' DELIMITERS ',' CSV;
COPY route_points FROM '/Users/samarthbhaskar/Desktop/Clear_streets_v2/raw_gps/route_points2.csv' DELIMITERS ',' CSV;

-- select * from assets order by random() limit 10;
-- select * from route_points order by random() limit 10;

alter table route_points add column geo GEOGRAPHY;

UPDATE route_points SET geo = st_geogfromtext('POINT(' || X || ' ' || Y || ')') where x between -180 and 180 and y between -180 and 180;
CREATE INDEX route_points_geog_gist on route_points using gist(geo);
VACUUM ANALYZE route_points ;
ALTER TABLE route_points add id serial primary key;
SELECT addgeometrycolumn('public', 'route_points', 'geometry', 4326, 'Point', 2);
UPDATE route_points SET geometry = geo::geometry;

-- alter table route_points drop column line;
ALTER TABLE route_points add column line geometry;
UPDATE route_points SET line = (select st_MakeLine(geometry order by posting_time) from route_points where geometry is not null);

--- routing ---
--- installed pgrouting and followed this tutorial (http://underdark.wordpress.com/2011/02/07/a-beginners-guide-to-pgrouting/) ---
--- however this needs debugging ---

CREATE OR REPLACE VIEW road_ext AS 
   SELECT *, startpoint(geo), endpoint(geo)
   FROM route_points;
   
CREATE TABLE node AS 
   SELECT row_number() OVER (ORDER BY foo.p)::integer AS id, 
          foo.p AS the_geom
   FROM (         
      SELECT DISTINCT road_ext.startpoint AS p FROM road_ext
      UNION 
      SELECT DISTINCT road_ext.endpoint AS p FROM road_ext
   ) foo
   GROUP BY foo.p;

CREATE TABLE network AS
   SELECT a.*, b.id as start_id, c.id as end_id
   FROM road_ext AS a
      JOIN node AS b ON a.startpoint = b.the_geom
      JOIN node AS c ON a.endpoint = c.the_geom;
      
SELECT * FROM shortest_path('
   SELECT gid AS id, 
          start_id::int4 AS source, 
          end_id::int4 AS target, 
          shape_leng::float8 AS cost
   FROM network',
1,
5110,
false,
false);

--- this can then be visualized in QGis using the RT Sql Layer method --



