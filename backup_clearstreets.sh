DATE=`/bin/date +%m-%d-%Y`
echo "$DATE"
echo 'clearing out osm and gpx folders'
rm ./osm/*
rm ./gpx/*
cd ./slurp2gpx/
echo 'writing out all GPX files'
python write_gpx_all.py > ~/logs/write_gpx_all.txt
echo 'moving plow.db'
cd ..
mv ./slurp2gpx/plow.db ./backups/plow-${DATE}.db
echo 'zipping up GPX'
tar -zcvf backups/gpx-${DATE}.tar.gz gpx
echo 'uploading to s3'
s3cmd put --acl-public --guess-mime-type backups/plow-${DATE}.db s3://clearstreets-data/${DATE}/
s3cmd put --acl-public --guess-mime-type backups/gpx-${DATE}.tar.gz s3://clearstreets-data/${DATE}/
echo 'deleting files'
rm ./gpx/*
echo 'done'
