DATE=`/bin/date +%m-%d-%Y`
echo "$DATE"
echo 'moving plow.db'
mv ./slurp2gpx/plow.db ./backups/plow-${DATE}.db
echo 'zipping up gpx and osm'
tar -zcvf backups/gpx-${DATE}.tar.gz gpx
tar -zcvf backups/osm-${DATE}.tar.gz osm
echo 'uploading to s3'
s3cmd put --acl-public --guess-mime-type backups/plow-${DATE}.db s3://clearstreets/${DATE}/
s3cmd put --acl-public --guess-mime-type backups/gpx-${DATE}.tar.gz s3://clearstreets/${DATE}/
s3cmd put --acl-public --guess-mime-type backups/osm-${DATE}.tar.gz s3://clearstreets/${DATE}/
echo 'deleting files'
rm ./slurp2pgx/plow.db
rm ./gpx/*
rm ./osm/*