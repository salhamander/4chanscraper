import re
import operator
import nltk
import pandas as pd
from nltk.collocations import *
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
from createstrings import *
from wordfrequencies import *
from colocation import *


def startWordAnalysis(content):
	colnames = ['threadnumber','no','now','time','comment', 'subject','replies','uniqueips','name','id','country','imagefile']

	df = pd.read_csv(content, encoding='latin-1', names=colnames)

	fullcorpusstring = createNLTKStrings(df,'fullcorpus')
	di_threadstrings = createNLTKStrings(df,'perthread')		#example: {}

	di_threadwords = {}
	li_threadbigrams = []
	li_threadtrigrams = []
	di_fullcorpus = {}

	print('Getting word frequencies for entirety of corpus')
	fullcorpus_frequencylist = getWordFrequencies(fullcorpusstring, 200)
	print(fullcorpus_frequencylist)

	print('Getting collocations for entirety of corpus')            #fetch bigrams
	print('Getting full bigrams...')
	li_fullbigrams = getCollocations(fullcorpusstring, 50, 'bigram')
	print(li_fullbigrams)

	print('Getting full trigrams...')
	li_fulltrigrams = getCollocations(fullcorpusstring, 50, 'trigram')
	print(li_fulltrigrams)

	di_fullcorpus['frequencies'] = fullcorpus_frequencylist
	di_fullcorpus['bigrams'] = li_fullbigrams
	di_fullcorpus['trigrams'] = li_fulltrigrams

	write_handle = open(str(content) + '-textanalysis-fullcorpus.txt',"w")
	write_handle.write(str(di_fullcorpus))
	write_handle.close()
	print('Wrote word analysis for full corpus\nNow starting analysis for words per thread')

	print('Getting word frequencies per thread.')
	for key, value in di_threadstrings.items():
		di_individualthread = {}

		li_threadfrequentwords = getWordFrequencies(value, 20)
		di_individualthread['frequentwords'] = li_threadfrequentwords	#append frequent words per thread
		print(li_threadfrequentwords)

		thread_bigrams = getCollocations(value, 10, 'bigram')			#append bigrams per thread 
		di_individualthread['bigrams'] = thread_bigrams			#append frequent words per thread
		print(thread_bigrams)

		thread_trigrams = getCollocations(value, 10, 'trigram')			#append bigrams per thread 
		di_individualthread['trigrams'] = thread_trigrams			#append frequent words per thread
		print(thread_trigrams)
		
		di_threadwords[key] = di_individualthread

	print(di_threadwords)

	write_handle = open(str(content) + '-textanalysis-perthread.txt',"w")
	write_handle.write(str(di_threadwords))
	write_handle.close()

# startWordAnalysis('snapshots/snapshot-06-01-2018-18-00-00/06-01-2018-18-00-00.csv')