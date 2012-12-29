cd ./slurp2gpx/
echo 'launching slurp_plow_data.py'
nohup python slurp_plow_data.py > ~/logs/slurp_plow_data.txt &
echo 'launching write_gpx.py'
nohup python write_gpx.py > ~/logs/write_gpx.txt &
cd ../gpx2osm
echo 'launching gpx2osm.sh'
nohup bash gpx2osm.sh > ~/logs/gpx2osm.txt &
cd ../osm2ft/
echo 'launching osm_to_ft.php'
nohup php osm_to_ft.php > ~/logs/osm_to_ft.txt &