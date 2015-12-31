#!/bin/bash

wget https://s3.amazonaws.com/metro-extracts.mapzen.com/chicago_illinois.osm.pbf
$HOME/osrm-backend/build/osrm-extract chicago_illinois.osm.pbf
$HOME/osrm-backend/build/osrm-prepare chicago_illinois.osrm
$HOME/osrm-backend/build/osrm-routed chicago_illinois.osrm
