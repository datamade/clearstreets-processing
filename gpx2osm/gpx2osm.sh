while :
do
    mono MatchGPX2OSM.exe --osm=chicago-r.osm --gpx=../gpx --output=../osm
    sleep 300
done

scp ../osm/* fgregg@bunkum.us:www/osm
