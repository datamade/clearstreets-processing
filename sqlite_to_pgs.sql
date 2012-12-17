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

CREATE OR REPLACE VIEW road_ext AS 
   SELECT *, startpoint(geo), endpoint(geo)
   FROM route_points;
