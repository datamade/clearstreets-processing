#!/bin/bash

#cd /home/datamade/osrm-backend/build
#rm chicago_illinois.*
#wget https://s3.amazonaws.com/metro-extracts.mapzen.com/chicago_illinois.osm.pbf
#/home/datamade/osrm-backend/build/osrm-extract -p /home/datamade/clearstreets-processing/clearstreets.lua chicago_illinois.osm.pbf
#/home/datamade/osrm-backend/build/osrm-prepare -p /home/datamade/clearstreets-processing/clearstreets.lua chicago_illinois.osrm
/home/datamade/osrm-backend/build/osrm-routed /home/datamade/osrm-backend/build/chicago_illinois.osrm
