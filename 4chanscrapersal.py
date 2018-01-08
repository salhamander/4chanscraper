import time as theendisnigh
import threading
import urllib.request, json
import pandas as pd
import re
import html2text
#import colocation.py as colocation
from nltk.text import Text
from nltk.tokenize import word_tokenize

print('code started!\n')

li_no = []
li_opno = []
li_replies = []
li_comments = []
li_time = [] 

counter = 0
commentlist = []
columns = ['opno','no','replies','time','comment']
df = pd.DataFrame(columns=columns)

def startcode():
    global counter
    
    # if counter<1:
    #     counter = counter + 1
    #     threading.Timer(10.0, startcode).start()
    open4chan()
    writeCSV()
    print('Code finished!')
    quit()
        
#        colocation.startTextAnalysis()

def open4chan():
    global filestart
    global imagename
    global counter
    
    print('Opening 4chan json')

    url_handle_all = urllib.request.urlopen("http://a.4cdn.org/pol/catalog.json")   #json with all the ops, page 1-10
    data = url_handle_all.read().decode()
    allthreads = json.loads(data)

    threadnumbers = []                                      #get all the threat numbers and put them in a list
    for pages in allthreads:                                #loop through pages
        for threads in pages['threads']:                    #loop through threads
            threadnumbers.append(threads['no'])
    print(threadnumbers)
    
    for threadnumber in threadnumbers[2:]:      #loop through all current threads (first two are ignored)
                                                #open the thread via new url that includes all comments
        url_handle_thread = urllib.request.urlopen("http://a.4cdn.org/pol/thread/" + str(threadnumber) + ".json")
        threaddata = url_handle_thread.read().decode('utf-8')
        threadjson = json.loads(threaddata)

        del li_no[:]
        del li_opno[:]
        del li_replies[:]
        del li_comments[:]
        del li_time[:] 

        for post in threadjson['posts']:
            # if post['no'] not in commentlist: #check if the comment is already in the datasheet
            if post['resto'] == 0:
                opno = post['no']
                replies = post['replies']
            else:
                replies = ''
            comkey = 'com'
            if comkey in post:              #check if comment exists (not all posts have coms)
                comment = post['com']
                comment = html2text.html2text(comment)
            else:
                comment = ''
            no = post['no']
            commentlist.append(post['no'])
            opnumber = opno
            time = post['time']

            li_no.append(no)                #put info in lists
            li_opno.append(opnumber)
            li_replies.append(replies)
            li_comments.append(comment)
            li_time.append(time)

        counter = counter + 1
        print('Finished thread number ' + str(counter) + '')
        writeCSV()
        print('sleeping for 10 seconds')
        theendisnigh.sleep(7)                  #to not freeze my computer and to save 4chan

def writeCSV():
    print('starting to write to csv')
    columns = ['opno','no','replies','time','comment']
    df = pd.DataFrame(columns=columns)
    df['no'] = li_no                            #add the lists to the pandas DataFrame
    df['threadnumber'] = li_opno
    df['replies'] = li_replies
    df['time'] = li_time
    df['comment'] = li_comments
    print(df)
    with open('csv/4chanfile.csv', 'a') as f:
        df.to_csv(f, encoding='utf-8', header=None, index=False)
    print('finished writing to csv')

startcode()

#IMAGE STUFF
#                    file.write(str(no) + ',' + str(time) + ',' + str(tim) + ',' + str(comment) + '\n')
                    
#                   Download an image
#                    print(tim)
#                    ext = comments['ext']
#                    imageurl = "http://i.4cdn.org/pol/" + str(tim) + str(ext)
#                    print(imageurl)
#                    urllib.request.urlretrieve(imageurl, 'img/' + str(tim) + str(ext))

#        file.write(posts[0]['com'])
#        commentlist.append(posts[0]['com'])
