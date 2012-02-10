while :
do
    mono MatchGPX2OSM.exe --osm=chicago-r.osm --gpx=../gpx --output=../osm
    scp ../gpx/* fgregg@bunkum.us:www/gpx
    scp ../osm/* fgregg@bunkum.us:www/osm
    sleep 10
done