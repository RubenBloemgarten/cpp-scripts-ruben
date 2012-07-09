#!/usr/bin/python
#
# Initials:     SF  Simon Funke
#               RB  Ruben Bloemgarten
#               AX  Axel Roest
#
# Version history
# 2012xxxx      RB  first version
# 20120708      RB  added indexes at after load infile


import sys
import os
import MySQLdb
import shutil
# 
# PLEASE UPDATE THESE SETTINGS
db_host = "BLA" # your host, usually localhost
db_user = "BLA" # your username
db_passwd = "BLA" # your password
db_name = "BLA" # name of the database
db_group = "mysql"
unzipped_directory = "/DATA/maxmind/GeoLiteCity_CSV/unzipped/"
csv_file_blocks = """GeoLiteCity-Blocks.csv"""
csv_file_location = """GeoLiteCity-Location.csv"""
log_dir = "/var/log/cpp_logs/maxmind/"
tmp_dir = "/tmp/"

with open(log_dir + 'GeoLiteCityDirs.txt', 'r') as unzipped_file_list, open(log_dir + 'GeoLiteCityLoad.log', 'a') as logFile:
    for eachLine in unzipped_file_list:
        strippedLine=eachLine.rstrip('\n')
        with open(log_dir +'GeoLiteCityLoadProcessed.log', 'r') as processedLog:
            if eachLine in processedLog:
                logFile.write('Already processed, skipping: ' + eachLine)
            else:
                logFile.write('Start loading: ' + eachLine)
                # Let's copy the files to /tmp first to avoid apparmor issues
                shutil.copyfile(unzipped_directory + strippedLine + '/' + csv_file_blocks, tmp_dir + csv_file_blocks)
                shutil.copyfile(unzipped_directory + strippedLine + '/' + csv_file_location, tmp_dir + csv_file_location)
                # Create Location table if not exist
                try: 
                    db = MySQLdb.connect(host = db_host, user = db_user, passwd = db_passwd, db = db_name, read_default_group = db_group)
                    #db.autocommit(True)
                    cursor = db.cursor()
                    query = ("""
                        CREATE TABLE IF NOT EXISTS """ + """`""" + """Location_""" + strippedLine + """` """ +
                             """(`locId` int(11) NOT NULL,
                                 `country` varchar(128) NOT NULL,
                                 `region` varchar(128) NOT NULL,
                                 `city` varchar(128) NOT NULL,
                                 `postalCode` varchar(128) NOT NULL,
                                 `latitude` varchar(128) NOT NULL,
                                 `longitude` varchar(128) NOT NULL,
                                 `metroCode` varchar(128) NOT NULL,
                                 `areaCode` varchar(128) NOT NULL)
                                  ENGINE=MyISAM DEFAULT CHARSET=latin1;
                             """)
                    cursor.execute( query )
                    db.commit()
                    # Create Blocks Table if not exist
                    query = ("""
                        CREATE TABLE IF NOT EXISTS""" + """`""" + """Blocks_""" + strippedLine +"""` """ +
                             """(`startIpNum` bigint(11) NOT NULL,
                                 `endIpNum` bigint(11) NOT NULL,
                                 `locId` int(11) NOT NULL)
                                 ENGINE=MyISAM DEFAULT CHARSET=latin1;
                             """)
                    cursor.execute( query )
                    db.commit()
                    # set global local_infile = 1
                    query = ("""
                        set global local_infile = 1;
                            """)
                    cursor.execute( query )
                    db.commit()
                    # Load the csv_file_location file
                    query = ("""
                        LOAD DATA LOCAL INFILE '""" + tmp_dir + csv_file_location + """' IGNORE INTO TABLE `Location_""" + strippedLine + """`""" + 
                             """ FIELDS TERMINATED BY ','
                                 ENCLOSED BY '"'
                                 LINES TERMINATED BY '\n'
                                 IGNORE 2 LINES;
                                 ALTER TABLE INDEX on maxmind.Location_""" + strippedLine +
                                  """ add primary key (locId);
                             """)
                    cursor.execute( query )
                    db.commit()
                    # Load the csv_file_blocks file
                    query = ("""
                        LOAD DATA LOCAL INFILE '""" + tmp_dir + csv_file_blocks + """' IGNORE INTO TABLE `Blocks_""" + strippedLine + """`""" +
                             """ FIELDS TERMINATED BY ','
                                 ENCLOSED BY '"'
                                 LINES TERMINATED BY '\n'
                                 IGNORE 2 LINES;
                                 CREATE INDEX startEndIp on maxmind.Blocks_""" + strippedLine + """(startIpNum,endIpNum);
                             """)
                    cursor.execute( query )
                    db.commit()
                    db.close()
                except MySQLdb.Error as e:
                    print(e)
                except Exception as e:
                    print(e)
                finally: 
                    print("Done loading " + strippedLine)
                    cursor.close()
                    
                    # Write the processedLog
                    with open(log_dir +'GeoLiteCityLoadProcessed.log', 'a') as processedLog:
                        os.remove(tmp_dir + csv_file_blocks)
                        os.remove(tmp_dir + csv_file_location)
                        processedLog.write(eachLine)
                    
