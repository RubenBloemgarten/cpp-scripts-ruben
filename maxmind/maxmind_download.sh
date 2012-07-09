#!/bin/sh
wget -r --reject "index.html*" -b -nH --cut-dirs=3 --no-parent -e robots=off -nc -A.zip -P /DATA/maxmind/ http://www.maxmind.com/download/geoip/database/GeoLiteCity_CSV/
