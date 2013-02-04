echo 'clearing out osm and gpx folders'
rm ./osm/*
rm ./gpx/*
cd ./slurp2gpx/
echo 'launching write_gpx.py'
python write_gpx_all.py > ~/logs/write_gpx_all.txt
cd ../gpx2osm
echo 'launching gpx2osm.sh'
bash gpx2osm.sh > ~/logs/gpx2osm.txt
cd ../osm2ft/
echo 'launching osm_to_ft.php'
php osm_to_csv.php > ~/logs/osm_to_csv.txt
