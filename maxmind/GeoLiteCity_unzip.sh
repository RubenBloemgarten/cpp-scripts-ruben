#!/bin/sh
# Very very simple script to unzip the GeoLiteCity csv file, just run once a day, if there is a new file it will be done. Not very elegant, but it works.

ZIP_DIR=/DATA/maxmind/GeoLiteCity_CSV/
UNZIPPED_DIR=/DATA/maxmind/GeoLiteCity_CSV/unzipped
LOG_DIR=/var/log/cpp_logs/maxmind

unzip -uo $ZIP_DIR\*.zip -d $UNZIPPED_DIR >> $LOG_DIR/GeoLiteCityUnzip.log 2>&1 &&
# Temporary placeholder for directory filelist for db import 
cd $UNZIPPED_DIR
ls -d * > $LOG_DIR/GeoLiteCityDirs.txt 
