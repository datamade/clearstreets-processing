cd ./slurp2gpx/
echo 'launching slurp_plow_data.py'
nohup python slurp_plow_data.py > ~/logs/slurp_plow_data.txt &
sleep 30
echo 'launching write_gpx.py'
nohup python write_gpx.py > ~/logs/write_gpx.txt &
cd ../gpx2osm
echo 'launching gpx2osm.sh'
nohup bash gpx2osm.sh > ~/logs/gpx2osm.txt &
cd ../osm2cartodb/
echo 'launching osm_to_cartodb.php'
nohup python osm_to_cartodb.py > ~/logs/osm_to_cartodb.txt &
