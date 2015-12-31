#!/bin/bash

wget https://s3.amazonaws.com/metro-extracts.mapzen.com/chicago_illinois.osm.pbf
osrm-extract chicago_illinois.pdf
osrm-prepare chicago_illinois.osrm
osrm-routed chicago_illinois.osrm
