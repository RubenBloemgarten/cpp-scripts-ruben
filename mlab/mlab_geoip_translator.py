#!/usr/bin/python
#
# Description : This script adds geoip information to a testsuite connection row from the maxmind table where the date is relative to the connection date. 
# Notes       : The script contains a lot of definitions from mlab_mysql_import.py which probably should not be here
# Initials:
#               AX  Axel Roest
#		RB  Ruben Bloemgarten
#
# Version history
# 20120629		AX	first version
# 20120630		RB      very minor changes: changing the scriptname, logfilename etc
# 20120630              RB      Added verbosity in descriptions. Corrected type where 'locId' was 'locID'
# 20120701              RB      Removing a bunch of extraneous and therefore confusing leftovers taken from mlab_mysql_import.py

import sys
import re
import os
from optparse import OptionParser
from datetime import datetime
import dateutil.parser as dparser
from dateutil.relativedelta import relativedelta
import MySQLdb
import shutil

#################################################################
#                                                               #
#           settings                                            #
#                                                               #
#################################################################

# PLEASE UPDATE THESE SETTINGS
db_host = "BLA" # your host, usually localhost
db_user = "bLA" # your username
db_passwd = "BLA" # your password
db_name = "BLA" # name of the database
db_tables = {"glasnost": "glasnost_test", "ndt": "ndt_test"} # a mapping from testname to tablename
db_filetable = 'files'

# directories
baseDir     = '/DATA/mlab/'
logDir      = baseDir + 'logs/'

#files
errorLog    = "error.log"
processLog  = "mlab_geoip_translator_processed.log"

#################################################################
#                                                               #
#           functions                                           #
#                                                               #
#################################################################

def usage():
  print "Usage: mlab_geoip_translator.py maxmind_table"
  sys.exit(1)

# Extract the datestring from the date in the table name
# Blocks_GeoLiteCity_20090601
def extract_datestring(string):
  ''' Returns the datetime contained in string '''
  # Extract the date
  date_match = re.match('Blocks_GeoLiteCity_(\d{4}\d{2}\d{2})$', string)
  if not date_match:
    raise Exception('Error in argument "', string, '" does not contain a valid date.')
  return date_match.group(1)

# Turn the datestring into datetime format
def extract_date(string):
  ''' Returns the datetime contained in string '''
  # Extract the date
  date_match = re.match('.*(\d{4})(\d{2})(\d{2})$', string)
  if not date_match:
    raise Exception('Error in argument "', string, '" does not contain a valid date.')
  date = datetime(int(date_match.group(1)),int(date_match.group(2)),int(date_match.group(3)))
  return date

# Verify if the connection string to be inserted already exists in the testsuite mlab table.
def exists_dbentry(cur, file_id, db_table, test_datetime, destination, source_ip):
    sql = "SELECT COUNT(*) FROM " + db_table + " WHERE date = '" + test_datetime.isoformat() + "' AND destination = '" + destination +  "' AND  source = '" + source_ip + "' AND file_id = " + str(file_id) 
    cur.execute(sql)

    if cur.fetchone()[0] < 1:
        return False
    else:
        return True

# Insert a testsuite connection string without testing if it exists.
def blunt_insert_dbentry(cur, file_id, db_table, test_datetime, destination, source_ip):
    columns = ', '.join(['date', 'destination', 'source', 'file_id'])
    values = '"' + '", "'.join([test_datetime.isoformat(), destination, source_ip, str(file_id)]) + '"'
    sql = "INSERT INTO  " + db_table + " (" + columns + ") VALUES(" + values + ") "
    cur.execute(sql)
    
# Insert a testsuite connection string after having tested if it exists
def insert_dbentry(cur, file_id, db_table, test_datetime, destination, source_ip):
    ''' Insert a test connection to the database, if it not already exists '''
    # Check if the entry exists already 
    sql = "SELECT COUNT(*) FROM " + db_table + " WHERE date = '" + test_datetime.isoformat() + "' AND destination = '" + destination +  "' AND  source = '" + source_ip + "' AND file_id = " + str(file_id) 
    cur.execute(sql)

    # If not, then insert it
    if cur.fetchone()[0] < 1:
        print 'Found new test performed on the', test_datetime, 'from ' + destination + ' -> ' + source_ip + '.' 
        blunt_insert_dbentry(cur, file_id, db_table, test_datetime, destination, source_ip)

# return True if the maxmind geoip table exists in the maxmind database
def check_maxmind_exist(cur, table):
    sql = "select * FROM maxmind.`" + table + "` LIMIT 1"
    cur.execute(sql)
    if cur.fetchone()[0] < 1:
        return False
    else:
        return True
      
# Retrieve the names of all tables in the maxmind database and filter the "Blocks_[DATE]" tables.
def get_maxmind_dates(cur):
    datehash = {}
    sql = "SHOW TABLES FROM `maxmind`"
    cur.execute(sql)
    rows = cur.fetchall()
    rows2 = []
    # filter rows
    for item in rows:
        m = re.search('Blocks_GeoLiteCity_(\d+)$', item[0])
        if (m):
            rows2.append(m.group(0))

    # print rows2
    
    skipfirst = True
    for table in rows2:
        date = extract_datestring(table)
        if (skipfirst):
            skipfirst = False
        else:
            datehash[olddate] = date
        olddate = date
    # last one is 6 months in the future
    lastdate = extract_date(date)
    print 'date=' + date + " = " + str(lastdate)
    # futuredate = lastdate + datetime.timedelta(365 * 6/12)     
    futuredate = lastdate + relativedelta(months = +6)
    datehash[olddate] = futuredate.strftime('%Y%m%d')
    return datehash

#  update the rows in the glasnost table to add the maxmind geoip information where the used maxmind table is relative to the date of the connection.
def update_mlab_glasnost(cur,table):
    start_datum = extract_datestring(table)
    end_datum   = maxmind_dates[start_datum]
    print 'updating between' + start_datum + ' AND ' + end_datum
    try:
        sql = 'UPDATE mlab.glasnost SET locId = M.`locId` FROM mlab.glasnost L , maxmind.' + table + ' M WHERE  L.`source` BETWEEN M.`startnumip` AND M.`endnumip` AND L.`date` BETWEEN "' + start_datum + '" AND "' + end_datum + '" AND L.`locId` = 0'
        print sql
        cur.execute(sql)
    except MySQLdb.Error, e:
        print "An error has been passed. %s" %e
        
#################################################################
#                                                               #
#           start of initialisation                             #
#           Read command line options                           #
#                                                               #
#################################################################

parser = OptionParser()
parser.add_option("-q", "--quiet", action="store_false", dest="verbose", default=False, help="don't print status messages to stdout")
(options, args) = parser.parse_args()
if len(args) == 0:
  usage()

# create file if necessary, as open by itself doesn't cut it
f = open(logDir + processLog, 'a')
f.write("\nNew mlab_maxmind_processed job on " + str(datetime.now()))
f.close

#################################################################
#                                                               #
#           start of main program                               #
#                                                               #
#################################################################
global_start_time = datetime.now()

try:
    # Connect to the mysql database
    db = MySQLdb.connect(host = db_host, 
                         user = db_user, 
                         passwd = db_passwd, 
                         db = db_name) 
    cur = db.cursor()
    
except:
    sys.stderr.write('Error, cannot connect to database' + db_name + '\n')

# contains hash with key = start_date, value = enddate (= startdate of next table, except for the last one)
maxmind_dates = get_maxmind_dates(cur)
print maxmind_dates

# sys.exit(1)
# Iterate over ALL filenames
for table in args:
    if (check_maxmind_exist(cur,table)):
        update_mlab_glasnost(cur,table)

cur.close()
global_end_time = datetime.now()

print '=====================================\nAll Done. ' + str(len(args)) + ' file(s) in ' + str(global_end_time - global_start_time)
