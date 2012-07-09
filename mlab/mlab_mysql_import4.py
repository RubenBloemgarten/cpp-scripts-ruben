#!/usr/bin/python
#
# Initials:     SF  Simon Funke
#               RB  Ruben Bloemgarten
#               AX  Axel Roest
#
# Version history
# 2012xxxx      SF  first version
# 20120628      AX  removed testing for every line, added timing code, 
# 20120629      AX  added loop over all arguments, exception handling, restructured code, moved processed files to archive or error folder
# 20120708      RB  cleaning some names and spelling, also we don't want processed_files.log to clobber the downloaders processed_files.log. So we should use overly descriptive names
# test: 
# cd /DATA
# python scripts/mlab/mlab_mysql_import2.py mlab/clean/glasnost/20090128T000000Z-batch-batch-glasnost-0002.tgz.csv
# 
# ToDO: v loop over all arguments in sys.argv[0]
#       v deduplication toevoegen (put in hash, test on hash, clear hash for each file, but keep last entry
#       v move files naar archive directory
#       v move error files naar error directory
#       v log process and errors
#		skip empty ip lines instead or error message

import sys
import re
import os
from optparse import OptionParser
from datetime import datetime
import dateutil.parser as dparser
import MySQLdb
import shutil

#################################################################
#                                                               #
#           settings                                            #
#                                                               #
#################################################################

# PLEASE UPDATE THESE SETTINGS
db_host = "localhost" # your host, usually localhost
db_user = "root" # your username
db_passwd = "" # your password
db_name = "mlab" # name of the database
db_tables = {"glasnost": "glasnost", "ndt": "ndt_test"} # a mapping from testname to tablename
db_filetable = 'files'

# directories
baseDir     = '/DATA/mlab/'
#baseDir     = '/home/axel/mlab/'
scratchDir  = baseDir + 'scratch/'
workDir     = baseDir + 'work/'
archiveDir  = baseDir + 'archive/'
errorDir    = baseDir + 'error/'
logDir      = baseDir + 'logs/'
cleanDir    = baseDir + 'clean/'

#files
errorLog    = "mlab_mysql_import_error.log"
processLog  = "mlab_mysql_import_processed_files.log"

#################################################################
#                                                               #
#           functions                                           #
#                                                               #
#################################################################

def usage():
  print "Usage: mlab_mysql_import3.py mlab_file1.csv [mlab_files.csv ...]"
  sys.exit(1)
  

def extract_destination(filename):
# This routine extracts the destination server of the mlab file. 
# It assumes that the filename has the form like 20100210T000000Z-mlab3-dfw01-ndt-0000.tgz.csv
#  
  # Split the filename and perform some tests if it conforms to our standard
  f_split = filename.split('-')
  if len(f_split) < 3:
    raise Exception("The specified filename (", filename, ") should contain at least two '-' characters that delimit the data, destination and the suffix.")
    
  if '.tgz.csv' not in f_split[-1]:
    print "The specified filename (", filename, ") should end with '.tgz.csv'."

  return '.'.join(filename.split('-')[1:-1])

def extract_datetime(string):
# Returns the datetime contained in string.
  # Extract the date
  date_match = re.search(r'\d{4}/\d{2}/\d{2}', string)
  if not date_match:
    raise Exception('Error in import: line "', string, '" does not contain a valid date.')
  # Extract the time
  time_match = re.search(r'\d{2}:\d{2}:\d{2}', string)
  if not time_match:
    raise Exception('Error in import: line "', string, '" does not contain a valid time.')

  try:
    return dparser.parse(date_match.group(0) + ' ' + time_match.group(0), fuzzy=True) 
  except ValueError:
    raise ValueError, 'Error in import: line "' + string + '" does not contain a valid date and time.'

def extract_ip(string):
# Returns the first valid ip address contained in string.
  # return with 0 or empty string when we encounter cputime
  # Extract the date
  match = re.search(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', string)
  if not match:
  	# ignore file
    raise Exception ('Error in import: line "', string, '" does not contain a valid ip address.')
  return match.group(0)

def exists_dbentry(cur, file_id, db_table, test_datetime, destination, source_ip):
# Test if the entry already exists in the database
    # Check if the entry exists already 
    sql = "SELECT COUNT(*) FROM " + db_table + " WHERE date = '" + test_datetime.isoformat() + "' AND destination = '" + destination +  "' AND  source = '" + source_ip + "' AND file_id = " + str(file_id) 
    cur.execute(sql)

    if cur.fetchone()[0] < 1:
        return False
    else:
        return True

def blunt_insert_dbentry(cur, file_id, db_table, test_datetime, destination, source_ip):
# Insert a connection to the database without testing.
    columns = ', '.join(['date', 'destination', 'source', 'file_id'])
    values = '"' + '", "'.join([test_datetime.isoformat(), destination, source_ip, str(file_id)]) + '"'
    sql = "INSERT INTO  " + db_table + " (" + columns + ") VALUES(" + values + ") "
    cur.execute(sql)

def insert_dbentry(cur, file_id, db_table, test_datetime, destination, source_ip):
# Insert a test connection to the database, if it not already exists
    # Check if the entry exists already 
    sql = "SELECT COUNT(*) FROM " + db_table + " WHERE date = '" + test_datetime.isoformat() + "' AND destination = '" + destination +  "' AND  source = '" + source_ip + "' AND file_id = " + str(file_id) 
    cur.execute(sql)

    # If not, then insert it
    if cur.fetchone()[0] < 1:
        print 'Found new test performed on the', test_datetime, 'from ' + destination + ' -> ' + source_ip + '.' 
        blunt_insert_dbentry(cur, file_id, db_table, test_datetime, destination, source_ip)

def get_file_id(cur, filename):
# Returns the id of a filename in the filename table. Creates a new row if the filename does not exist. 
    sql = "SELECT id FROM " + db_filetable + " WHERE filename ='" + filename + "'"
    cur.execute(sql)
    id = cur.fetchone()
    # If the entry does not exist, we add it in
    if not id:
        sql = "INSERT INTO  " + db_filetable + " (filename) VALUES('" + filename + "')"
        cur.execute(sql)
        return get_file_id(cur, filename)
    return id[0]

def dedup(file_id, table, test_datetime, destination, source_ip):
# do deduplucation of connection strings
    key = str(file_id) + table + str(test_datetime) + destination + source_ip
    if key in deduplookup:
        return False
    else:
        deduplookup[key] = True
        return True
        
# returns True on error, False on correct processing
def process_file(f, filename):
    start_time = datetime.now()
    failure = True
    try:
        # Connect to the mysql database
        db = MySQLdb.connect(host = db_host, 
                             user = db_user, 
                             passwd = db_passwd, 
                             db = db_name) 
        cur = db.cursor() 
    
        # Find the destination server by investigating the filename
        destination = extract_destination(filename)
        print 'Destination: ', destination,
    
        # Get the filename id from the files table
        file_id = get_file_id(cur, filename) 
        db.commit()
    
        # Find the testsuite by investigating the filename
        try:
            test = [test for test in db_tables.keys() if test in filename][0]
        except IndexError:
            sys.stderr.write('The filename ' + filename + ' does not contain a valid testname.')
            return 1
        # print "Found test suite " + test 
    
        # The filetest ALONE, takes 3 seconds with a 9 million records database, without indexes
        # But falls back to less than half a second when indexing is turned on on the db
        filetest=True
        # Read the file line by line and import it into the database
        for line in f:
          line = line.strip()
          source_ip = extract_ip(line)
          test_datetime = extract_datetime(line)
          if (filetest):
            if (exists_dbentry(cur, file_id, db_tables[test], test_datetime, destination, source_ip)):
                # this file has already been read: ABORT WITH ERROR
                raise Exception('File entry already exist in db; the file has already been read: ' + filename)
            filetest=False
          # test if we have already done it in this or last filetest
          if (dedup(file_id, db_tables[test], test_datetime, destination, source_ip)):
              blunt_insert_dbentry(cur, file_id, db_tables[test], test_datetime, destination, source_ip)
        end_time = datetime.now()
        print 'File done in ' + str(end_time - start_time)
        failure = False
    except Exception as inst:
        sys.stderr.write('Exception: '+str(inst.args)  + '\n')
        with open(logDir + errorLog, 'a') as f:
            f.write(pathname + '\n')
            f.write('Exception: '+str(inst.args)  + '\n')
        print
    except IOError as e:
        sys.stderr.write('Error handling file ' + filename + ' (' + str(e.args) + ')\n')
        with open(logDir + errorLog, 'a') as f:
            f.write(pathname + '\n')
            f.write('Error handling file ' + filename + ' (' + str(e.args) + ')\n')
        print
# This bit should probably be cleaned up.        
#    except:
#        sys.stderr.write('Process error ' + '\n')
    finally:
        # Commit and finish up
        sys.stderr.flush()
       # db.commit()
       # disconnect from server
        #db.close()
    
    return failure
    
def extract_archive_date(filename):
      m = re.match('^(\d{4})(\d{2})(\d{2})', filename)
      return (m.group(1),m.group(2))

def create_archive_dir(ym):
# test if archive directory exist, and create it if necessary
    if (not os.path.exists(ym)):
        os.makedirs(ym)
    return ym

def move_archive(pathname):
# move processed file to archive folder
    fname = os.path.basename(pathname)
    (year,month) = extract_archive_date(fname)
    aDir = create_archive_dir(archiveDir + year +'/'+ month)
    shutil.move(pathname,aDir)
    with open(logDir + processLog, 'a') as f:
        f.write(pathname + '\n')


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
f.write("\nNew batchjob on " + str(datetime.now()))
f.close

# deduplookup is a hash we use for de-duplication of input lines
# maybe it is necessary to purge parts of it during the duration of the import
# but then we have to carefully monitor tests that appear in multiple files
# OR store the last test in a separate global (dirty? yeah, I know)
deduplookup = {}

#################################################################
#                                                               #
#           start of main program                               #
#                                                               #
#################################################################
global_start_time = datetime.now()

# Iterate over ALL filenames
for pathname in args:
    try:
        with open(pathname, 'r') as f:
            # Extract the basename of the filename, as the path is not of interest after this point
            filename = os.path.basename(pathname)
            print "processing file " + filename,
            if (process_file(f, filename)):
                shutil.move(pathname,errorDir)
            else:
                move_archive(pathname)
    # file is automatically closed if needed
    except IOError as e:
         print 'Could not open file ' + pathname + '\nError: ' + str(e.args)

global_end_time = datetime.now()

print '=====================================\nAll Done. ' + str(len(args)) + ' file(s) in ' + str(global_end_time - global_start_time)
