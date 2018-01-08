def createNLTKStrings(df, outputform):
	inputsheet = df
	output = outputform

	li_posts = inputsheet.comment.tolist()
	li_threadnumberlist = inputsheet.threadnumber.tolist()

	li_posts = li_posts[1:]
	li_threadnumberlist = li_threadnumberlist[1:]

	if output == 'fullcorpus':
		longstring = ''
		for comment in li_posts:
			longstring = longstring + str(comment) + ' '
		return longstring

	if output == 'perthread':
		di_threadstrings = {}
		threadstring = ''
		
		currentthreadnumber = li_threadnumberlist[1]
		print('currentthreadnumber: ' + str(currentthreadnumber))
		for index, threadnumber in enumerate(li_threadnumberlist):
			if currentthreadnumber == threadnumber:
				threadstring = str(threadstring) + str(li_posts[index]) + ' '
			else:
				di_threadstrings[currentthreadnumber] = threadstring 		#put a threadnumber value as key with full string
				threadstring = ''
				threadstring = str(li_posts[index])
				currentthreadnumber = threadnumber
		# print('Threadstrings: ' + str(di_threadstrings))
		return di_threadstrings