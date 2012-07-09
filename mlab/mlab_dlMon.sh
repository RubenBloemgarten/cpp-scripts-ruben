#!/bin/sh
SERVICE='mlab_downloader.py'
DIR='/DATA/scripts/mlab' 
if ps ax | grep -v grep | grep $SERVICE > /dev/null
then
    echo "$SERVICE service running, everything is fine" > /var/log/sqzMon.log
else
     
    echo "`date +"%Y-%m-%d-%H-%M"` $SERVICE is not running" >> /var/log/sqzMonError.log
    echo "starting $SERVICE now" >> /var/log/mlab_dl_mon_error.log 
    python $DIR/$SERVICE
fi
