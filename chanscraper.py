import time as theendisnigh
import threading
import urllib.request, json
import requests
import pandas as pd
import re
import html2text
import io
import os
from requests.exceptions import HTTPError
from PIL import Image
from nltk.text import Text
from nltk.tokenize import word_tokenize
from colocation import *
from wordfrequencies import *

print('code started!\n')

li_no = []
li_opno = []
li_replies = []
li_comments = []
li_subjects = []
li_time = [] 
li_name = []
li_uniqueips = []
li_id = []
li_now = []
li_country = []
li_tim = []
li_allnos = []

columns = ['opno','no','replies','time','comment', 'now', 'country', 'subject', 'tim']
df = pd.DataFrame(columns=columns)

global filetime
global outputfolder
global outputcsv

counter = 0
time = 0
firstpost = 0
lasttimeactive = 0

li_activethreads = []           #the pivotal list with the active threads
li_inactivethreads = []

di_metadata = {}
di_metadata['threadsmetadata'] = {}
di_threadmetadata = {}
di_threadsmetadata = {}
li_repliesarchived = []
li_uniqueipsarchived = []
li_timarchived = []
li_countriesarchived = []
li_allimages = []


def createNewSnapshot():
    print('createNewSnapshot() called')
    global li_activethreads
    global filetime
    global outputfolder
    global outputcsv

    del li_activethreads[:]     #start with a clean threads list for a new snapshot
    del li_inactivethreads[:]
    del li_repliesarchived[:]

    filetime = ""
    outputfolder = ""
    outputcsv = ""
    filetime = str(theendisnigh.strftime("%d-%m-%Y-%H-%M-%S"))
    outputfolder = 'snapshots/snapshot-' + filetime
    outputcsv = outputfolder + '/' + filetime + '.csv'

    os.makedirs(outputfolder)
    os.makedirs(outputfolder + '/img/')

    print('Opening catalog.json to register active threads')
    url_handle_all = urllib.request.urlopen("http://a.4cdn.org/pol/catalog.json")   #json with all the ops, page 1-10
    data = url_handle_all.read().decode('latin-1', 'ignore')
    allpages = json.loads(data)

    for pages in allpages:                         #loop through pages.   Put [x:] after allpages for testing.
        for threads in pages['threads']:           #loop through threads. Put [x:] after pages['threads'] for testing.
            if threads['no'] == 124205675 or threads['no'] == 143684145:  #filter out the sticky posts
                print('sticky thread')
            else:
                li_activethreads.append(threads['no'])      #fill active threads list with all catalog.json threads
    print(li_activethreads)    

    # while(len(li_activethreads) > 0):                   #update if there is still an active thread 
    loopbool = True
    while(loopbool):
        print('threads still active: ' + str(len(li_activethreads)))
        
        fetchPosts(li_activethreads)
        loopbool = False

        timecounter = 1
        timetowait = 20                 # * 10 is the amount of seconds wait for snapshot
        # if len(li_activethreads) > 0:
        #     print('fetched/updated posts, sleeping for a minute before running the next thread update')
        #     while timecounter < timetowait:
        #         timecounter = timecounter + 1
        #         theendisnigh.sleep(10)
        #     timecounter = 1

def fetchPosts(activethreads):
    global li_activethreads
    global counter
    global opno
    global di_metadata
    global di_threadmetadata
    global di_threadsmetadata

    li_imagestofetch = []

    threadnumbers = activethreads
    print('updating/fetching threads ' + str(threadnumbers))

    count_postshandled = 0

    li_threadslooped = threadnumbers

    for index, threadnumber in enumerate(reversed(li_threadslooped)):        #loop through all current threads (first two are ignored)
        threadactive = True
        try:                                            #check if the thread is still active on the site
            urllib.request.urlretrieve("http://a.4cdn.org/pol/thread/" + str(threadnumber) + ".json")
            # theendisnigh.sleep(1)
        except urllib.error.HTTPError as error:                       #some threads get deleted and return a 404
            if error.code == 404:
                print('Thread deleted from website\nAdded thread ' + str(threadnumber) + ' to inactive threads')
                if threadnumber not in li_inactivethreads:
                    li_inactivethreads.append(threadnumber)                 #append the thread number to the inactive threads
                
            if error.code == 524:
                print('HTTP 524 Error: Origin Time-out\nSleeping 30 seconds and trying again.')
                theendisnigh.sleep(20)
                
        except ConnectionResetError:
            print('Connection refused by the remote host...\nSleeping for 200 seconds.')
            if threadnumber not in li_inactivethreads:
                li_inactivethreads.append(threadnumber)
            theendisnigh.sleep(200)
            
        else:
            url_handle_thread = urllib.request.urlopen("http://a.4cdn.org/pol/thread/" + str(threadnumber) + ".json")
            threadtimestamp = int(theendisnigh.time())
            lastactivity = threadtimestamp
            threaddata = url_handle_thread.read().decode('latin-1', 'ignore')
            threadjson = json.loads(threaddata)

            del li_no[:]                          #remove all the previous data - the csv is written per thread
            del li_opno[:]
            del li_replies[:]
            del li_uniqueips[:]
            del li_comments[:]
            del li_time[:] 
            del li_now[:]
            del li_country[:]
            del li_subjects[:]
            del li_name[:]
            del li_id[:]
            del li_tim[:]

            postoffset = 0                        #if the thread is already registered, determine an offset for posts
            nokey = threadjson['posts'][0]['no']
            startpost = threadjson['posts'][0]

            if nokey in di_metadata['threadsmetadata']:
                postoffset = di_metadata['threadsmetadata'][nokey]['lasttimeactive']
                firstpost = di_metadata['threadsmetadata'][nokey]['firstpost']

            archivedkey = 'archived'              #check if entry has a subject
            closedkey = 'closed'                  #check if entry has a subject
            if archivedkey in startpost:
                if startpost['archived'] == 1:    #check whether post was closed or archived
                    if threadnumber not in li_inactivethreads:
                        li_inactivethreads.append(threadnumber)
                        archivedonkey = 'archived_on'                  #check if entry has a subject
                        if archivedonkey in startpost:
                            lastactivity = startpost['archived_on']
                        print('Thread archived\nAdded thread ' + str(threadnumber) + ' to inactive threads')
                
            elif closedkey in startpost:
                if startpost['closed'] == 1:
                    if threadnumber not in li_inactivethreads:
                        li_inactivethreads.append(threadnumber)
                        archivedonkey = 'archived_on'                  #check if entry has a subject
                        if archivedonkey in startpost:
                            lastactivity = startpost['archived_on']
                        print('Thread closede\nAdded thread ' + str(threadnumber) + ' to inactive threads')
                

            for post in threadjson['posts']:        #loop through posts in thread
                # if post['time'] > postoffset:       #only fetch post when it is posted later than last known post in thread
                if True:
                    comkey = 'com'
                    subkey = 'sub'
                    timkey = 'tim'
                    countrykey = 'country_name'
                    idkey = 'id'
                    namekey = 'name'
                    uniqueipkey = 'unique_ips'

                    opnumber = ''
                    if post['resto'] == 0:
                        opno = post['no']
                        replies = post['replies']
                        opnumber = opno
                        firstpost = post['time']      #if it's the first post, store the value for metadata
                    else:
                        replies = ''
                        opnumber = opno
                    no = post['no']

                    if uniqueipkey in post:
                        if uniqueipkey is not '':
                            uniqueips = post['unique_ips']
                            li_uniqueipsarchived.append(uniqueips)
                    else:
                        uniqueips = ''
                    
                    if comkey in post:              #check if comment exists (not all posts have coms)
                        comment = post['com']
                        comment = comment.encode('latin-1', 'ignore')
                        comment = comment.decode('latin-1')
                        comment = comment.replace('>', '> ')
                        comment = html2text.html2text(comment)
                        print('removed characters and cleaned html text')
                        # print(comment)
                    else:
                        comment = ''

                    if subkey in post:              #check if entry has a subject
                        subject = post['sub']
                        subject = subject.encode('latin-1', 'ignore')
                        subject = subject.decode('latin-1')
                        subject = subject.replace('>', '> ')
                        subject = html2text.html2text(subject)
                    else:
                        subject = ''

                    if idkey in post:        #check if entry has a subject
                        idstring = post['id']
                    else:
                        idstring = ''

                    if namekey in post:        #check if entry has a subject
                        namestring = post['name']
                        namestring = namestring.encode('latin-1', 'ignore')
                        namestring = namestring.decode('latin-1')
                        namestring = namestring.replace('>', '> ')
                        namestring = html2text.html2text(namestring)
                    else:
                        namestring = '' 

                    time = post['time']
                    lasttimeactive = time
                    now = post['now']

                    if countrykey in post:          #not all posts have countrykeys
                        country = post['country_name']
                    else:
                        country = ''

                    if timkey in post:
                        ext = post['ext']           #download the images
                        tim = post['tim']
                        li_imagestofetch.append(str(opno)+'-'+str(tim)+str(ext))
                    else:
                        tim = ''
                        ext = ''

                    li_no.append(no)                #put info in lists
                    li_opno.append(opnumber)
                    li_replies.append(replies)
                    li_repliesarchived.append(replies)
                    li_comments.append(comment)
                    li_subjects.append(subject)
                    li_id.append(idstring)
                    li_uniqueips.append(uniqueips)
                    li_name.append(namestring)
                    li_time.append(time)
                    li_now.append(now)
                    li_country.append(country)
                    li_countriesarchived.append(country)
                    li_tim.append(tim)
                    li_timarchived.append(tim)

                    count_postshandled = count_postshandled + 1

                else:
                    print('post already registered')
        
        #when one thread is finished:
        if count_postshandled > 0:      #and if there was actually some new posts to process
            di_threadmetadata = {}      #store active times
            di_threadmetadata['firstpost'] = firstpost
            di_threadmetadata['lasttimeactive'] = lasttimeactive        #time of last post
            di_threadmetadata['secondsactive'] = (lastactivity - firstpost)
            if li_uniqueips[0] is not '':
                di_threadmetadata['uniqueips'] = li_uniqueips[0]
            if li_replies[0] is not '':
                di_threadmetadata['posts'] = li_replies[0] + 1
            
            di_threadsmetadata[threadnumber] = di_threadmetadata
            di_metadata['threadsmetadata'] = di_threadsmetadata     #append threads dict to metadata dict
            print(di_threadmetadata)
            writeCSV()

        count_postshandled = 0
        counter = counter + 1
        print('Finished thread ' + str(threadnumber)+ '\n' + str(index + 1) + ' / ' + str(len(li_threadslooped)) + ' threads complete')
        # print('sleeping for 7 seconds')
        # theendisnigh.sleep(7)                  #to not freeze my computer and to save 4chan

    #when all threads are finished:
    li_activethreads = [thread for thread in li_threadslooped if thread not in li_inactivethreads]
    print(str(len(li_inactivethreads)) + ' threads inactive: ' + str(li_inactivethreads))
    print(str(len(li_activethreads)) + ' threads active: ' + str(li_activethreads))
    count_activethreads = len(li_activethreads)
    
    li_imagestofetch = [image for image in li_imagestofetch if image not in li_allimages]
    getImages(li_imagestofetch)
    li_allimages.append(li_imagestofetch)
    del li_imagestofetch[:]

def getImages(imagelist):
    li_images = imagelist
    size = 800, 800

    print('Fetching images...')
    for index, postimage in enumerate(li_images):
        imagename = postimage[10:]
        # print('Getting and resizing image ' + imagename)
        extlist = postimage.split('.')
        extfile = extlist[1]
        ext = '.' + extlist[1]
        while True:
            request = urllib.request.Request('http://i.4cdn.org/pol/' + str(imagename))
            try:                                    #check if the thread is still active on the site
                response = urllib.request.urlopen(request)
                # imageurl = "http://i.4cdn.org/pol/" + str(imagename)

            except urllib.error.HTTPError as httperror:                       #some threads get deleted and return a 404
                print('HTTP error when requesting image')
                print('Reason:', httperror.code)
                if httperror.code != 404:
                    theendisnigh.sleep(120)
                pass
            except ConnectionError:
                print('Connection refused by the remote host...\nSleeping for 120 seconds.')
                theendisnigh.sleep(120)
                pass
            except ConnectionAbortedError:
                print('Connection refused by the remote host...\nSleeping for 120 seconds.')
                theendisnigh.sleep(120)
                pass
            except ConnectionResetError:
                print('Connection refused by the remote host...\nSleeping for 120 seconds.')
                theendisnigh.sleep(120)
                pass
            else:
                if ext == '.webm':                      #check whether the file is an image
                    print('webm file, discarding')
                    # urllib.request.urlretrieve(imageurl, outputfolder+'/img/'+ str(postimage))
                else:
                    #resizing images
                    imagefile = io.BytesIO(response.read())
                    image = Image.open(imagefile)
                    # print('imagesize: ' + str(image.size))
                    imagesize = image.size
                    if imagesize[0] > 800 or imagesize[1] > 800:
                        # print('Resizing...')
                        image.thumbnail(size)
                    image.save(outputfolder + '/img/' + postimage)
                print('Image ' + str(index  + 1) + '/' + str(len(li_images)) + ' downloaded')
            
            # theendisnigh.sleep(1)
            if (index + 1) % 100 == 0:                          #so 4chan doesn't kick me out
                print('sleeping for 10 seconds')
                theendisnigh.sleep(10)
            break

def writeMetaResults():
    global di_metadata
    global di_threadmetadata
    global di_threadsmetadata

    di_metadata['threadmetadata'] = di_threadmetadata
    di_commentsdata = {}
    cumulatedreplies = 0                                    #calculate the total and average amount of comments
    li_replyamount = []
    for reply in li_repliesarchived:
        if reply is not '':
            cumulatedreplies = cumulatedreplies + reply + 1 #add 1 to include OP number
            li_replyamount.append(reply + 1)
    print('cumulatedreplies: ' + str(cumulatedreplies) + '\nlen(li_replyamount): ' + str(len(li_replyamount)))
    averagereplies = cumulatedreplies / len(li_replyamount)
    di_commentsdata['posts_amount'] = cumulatedreplies
    di_commentsdata['posts_average'] = averagereplies

    cumulatedips = 0
    amountofentries = 0
    for ips in li_uniqueipsarchived:
        cumulatedips = cumulatedips + ips
    if amountofentries > 0:                                  #sometimes the averageips key exists in no posts
        averageips = cumulatedips / len(li_uniqueipsarchived)
        di_commentsdata['averageips'] = averageips

    imagecounter = 0                                         #calculate the total amount of images/textposts
    textcounter = 0
    for image in li_timarchived:
        if image is not '':
            imagecounter = imagecounter + 1

    di_commentsdata['images_amount'] = imagecounter
    di_commentsdata['text_posts'] = imagecounter / cumulatedreplies

    di_metadata['commentsdata'] = di_commentsdata

    di_countryflags = {}
    for flag in li_countriesarchived:                            #average country contributions codes
        if flag is not '':
            if flag not in di_countryflags:
                di_countryflags[flag] = 1
            else:
                di_countryflags[flag] += 1
    di_metadata['countrydata'] = di_countryflags

    write_handle = open(outputfolder + '/' + filetime + '-' + 'metadata.txt',"w")
    write_handle.write(str(di_metadata)) 
    write_handle.close()
    print('Meta results: ' + str(di_metadata))
    di_threadmetadata = {}

def writeCSV():
    print('starting to write to csv')
    columns = ['threadnumber','no','now','time','comment', 'subject','replies','uniqueips','name','id','country','imagefile']
    df = pd.DataFrame(columns=columns)
    df['threadnumber'] = li_opno                        #add the lists to the pandas DataFrame
    df['no'] = li_no
    df['now'] = li_now
    df['time'] = li_time
    df['comment'] = li_comments
    df['subject'] = li_subjects
    df['replies'] = li_replies
    df['uniqueips'] = li_uniqueips
    df['name'] = li_name
    df['id'] = li_id
    df['country'] = li_country
    df['imagefile'] = li_tim

    with open(outputcsv, 'a') as f:
        if counter == 0:
            df.to_csv(f, encoding='latin-1', index=False)     #write headers at the first line
        else:
            df.to_csv(f, encoding='latin-1', header=None, index=False)
    print('Finished writing thread to csv')

def resetVariables():                                         #clear all variables for new scheduled run
    del li_no[:]
    del li_opno[:]
    del li_replies[:]
    del li_comments[:]
    del li_subjects[:]
    del li_time[:] 
    del li_name[:]
    del li_uniqueips[:]
    del li_id[:]
    del li_now[:]
    del li_country[:]
    del li_tim[:]
    del li_allnos[:]

    df.iloc[0:0]

    counter = 0
    time = 0
    firstpost = 0
    lasttimeactive = 0

    del li_activethreads[:]
    del li_inactivethreads[:]

    di_metadata.clear()
    di_threadmetadata.clear()
    di_threadsmetadata.clear()
    del li_repliesarchived[:]
    del li_uniqueipsarchived[:]
    del li_timarchived[:]
    del li_countriesarchived[:]
    del li_allimages[:]
    di_metadata['threadsmetadata'] = {}