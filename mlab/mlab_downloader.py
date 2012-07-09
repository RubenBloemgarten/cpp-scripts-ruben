# mlab downloader 0.10 20120421 by Ruben Bloemgarten (ruben@abubble.nl)
# the mlab_downloader downloads all tarballs from mlab using gsutil and writes the tarball's filelist to a file and then removes the tarball
# Still need to add download a fresh all_mlab_tarfiles file
# and it would be better to diff it with the previous one and write a new current file to avoid skipping through the entire processed log.

import subprocess
import os
import tarfile
import logging
import fileinput
import sys

try:
    # Set some stuff
    # scratchDir : Tarfiles are downloaded to this location for processing and removed afterwards
    # csvDir :  stores some preprocessed files from mlab, including the all_mlab_tarfiles.txt
    # logDir : logs directory, there should probably be a symlink mlab -> /DATA/mlab/logs in /var/log
    # cleanDir : The processed csv files live here
    scratchDir = '/DATA/mlab/scratch/'
    csvDir = '/DATA/mlab/csv/'
    logDir = '/DATA/mlab/logs/'
    cleanDir = '/DATA/mlab/clean/'
    # Don't grab a file we've already grabbed, process that data list, strip those linebreaks and download that tarball.
    # logFile is not readable until after a break occurs. I should probably learn how logging is supposed to be done, oh well. 
    with open('/DATA/mlab/logs/mlab_downloader.log', 'a') as logFile, open('/DATA/mlab/csv/all_mlab_tarfiles.txt', 'r') as fileMlab:
        for eachLine in fileMlab:
            strippedLine=eachLine.rstrip('\n')
            with open('/DATA/mlab/logs/processed_lines.log', 'r') as processedLog:
                if eachLine in processedLog:
                    logFile.write('Already processed, skipping: ' + eachLine)
                else:
                    logFile.write('start downloading: ' + eachLine)
                    subprocess.call(['gsutil', 'cp', strippedLine, scratchDir])
                    logFile.write('finished downloading: ' + eachLine)
                    # read that tarry ball to a listy file, dump the leftovers and write a report
                    listDir = os.listdir(scratchDir)
                    if listDir ==[]:
                        logFile.write('No file in directory, no file to process' + '\n')
                    else:
                        for tarName in listDir:
                            logFile.write('Start processing file: ' + tarName + '\n')
                            with tarfile.open(scratchDir + tarName , 'r|gz') as tar:
                                tarList = tar.getnames()
                            with open(cleanDir + tarName + '.csv','a') as csvFile:
                                cleanDirList = os.listdir(cleanDir)
                                if csvFile in cleanDirList:
                                    # If a break occurs during the writing of the csv file, its still there and will get appended, that's bad.
                                    # I should probably write to a temp file first, sigh, more stuff.
                                    # for now, just remove it and hope removing a still opened file does not cause an exception.
                                    logFile.write(csvFile + 'has already been proccessed, a break must have occured during writing of the file, removing it now' + '\n')
                                    os.remove(csvFile)
                                    logFile.write(csvFile + 'has been removed' + '\n')
                                else:
                                    for tarLine in tarList:
                                        csvFile.write(tarLine +',' + '\n')                        
                            logFile.write('file: ' + tarName + ' has been processed' + '\n')
                            with open('/DATA/mlab/logs/processed_lines.log', 'a') as processedLog:
                                processedLog.write(eachLine)
                                os.remove(scratchDir +tarName)
                            logFile.write(tarName +' has been removed, proceeding with next line' + '\n')
    

except (IOError, NameError, ValueError, OSError) as err:
    logFile.write(' Bad things error:' + str(err) + '\n')
