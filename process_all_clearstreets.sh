echo 'clearing out osm and gpx folders'
rm ./osm/*
rm ./gpx/*
cd ./slurp2gpx/
echo 'launching write_gpx.py'
python write_gpx_all.py > ~/logs/write_gpx_all.txt
cd ../gpx2osm
echo 'launching gpx2osm.sh'
mono MatchGPX2OSM.exe --osm=chicago-r.osm --gpx=../gpx --output=../osm > ~/logs/gpx2osm.txt
cd ../osm2cartodb/
echo 'launching osm_to_cartodb.php'
python osm_to_cartodb > ~/logs/osm_to_cartodb.txt
