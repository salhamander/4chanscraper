#!/home/salhagen/opt/python-3.6.3/bin/python3
import sys
from apscheduler.schedulers.blocking import BlockingScheduler
from chanscraper import *
from colocation import *
from startwordanalysis import *

print('Content-type: text/html\n\n')
print('<html><body>ma domain running python ' + sys.version + '<body></html>')

def startcode():
    # quit()
    global filetime
    global outputfolder
    global outputcsv
    print('Creating snapshot csv')
    createNewSnapshot()
    print('Snapshot csv created')
    print('Writing metadata...')
    writeMetaResults()
    print('Started word analysis...')
    startWordAnalysis(outputcsv)
    print('Resetting variables')
    resetVariables()
    print('Code finished!')
    print('Waiting 3 hours before next update...')

# startcode()
scheduler = BlockingScheduler()
scheduler.add_job(startcode, 'interval', minutes=180, start_date='2018-01-078 09:00:00') #add , start_date='2018-01-05 17:21:00' later
scheduler.start()