from index import Doc; import re; import threading; from collections import defaultdict; import timeit; import math; 

offset = []
titleOffset = []

def findFileNo(low, high, offset, word, f, typ='str'):

	while low < high:
		temp = int((low + high) / 2)
		mid = int(temp)
		f.seek(offset[mid])
		wordPtr = f.readline()
		wordPtr = wordPtr.strip().split()
		if typ == 'int':
			if int(word) == int(wordPtr[0]):
				req_var = wordPtr
				return wordPtr[1:], mid
			elif int(word) > int(wordPtr[0]):
				req_var = wordPtr
				low = mid + 1
			else:
				high = mid
		else:
			if word == wordPtr[0]:
				req_var = wordPtr
				return wordPtr[1:], mid
			elif word > wordPtr[0]:
				req_var = wordPtr
				low = mid + 1
			else:
				req_var = wordPtr
				high = mid
	return [], -1

def findDocs(filename, fileNo, field, word, fieldFile):
	empvar = []
	app_count = 1
	fieldOffset = []
	docFreq = []
	with open('./data/offset_' + field + fileNo + '.txt') as f:
		field_len = len(fieldOffset)
		# f_len = len(f)
		for line in f:
			offset, df = line.strip().split()
			fieldOffset.append(int(offset))
			app_var = int(df)
			docFreq.append(app_var)
	docList, mid = findFileNo(0, len(fieldOffset), fieldOffset, word, fieldFile)
	req_var = len(docFreq)
	req_var += len(fieldOffset)
	return docList, docFreq[mid]


def fieldQuery(words, fields, fvocab):
	wrds_len = len(words)
	docList = defaultdict(dict)
	docFreq = {}
	for i in range(len(words)):
		field = fields[i]
		req_var = len(words)
		word = words[i]
		docs, mid = findFileNo(0, len(offset), offset, word, fvocab)
		req_var = len(docs)
		if len(docs) > 0:
			fileNo = docs[0]
			filename = './data/' + field
			filename += str(fileNo) + '.txt'
			docs_len = len(docs)
			fieldFile = open(filename, 'r')
			req_no = fileNo
			returnedList, df = findDocs(filename, fileNo, field, word, fieldFile)
			docFreq[word] = df
			docList[word][field] = returnedList
	return docList, docFreq


def simpleQuery(words, fvocab):

	docFreq = {}
	fields = ['t', 'b', 'i', 'c', 'r', 'l']
	docList = defaultdict(dict)
	for word in words:
		docs, mid = findFileNo(0, len(offset), offset, word, fvocab)
		docs_len = len(docs)
		if len(docs) > 0:
			docFreq[word] = docs[1]
			fileNo = docs[0]
			for field in fields:
				filename = './data/' + field
				filename += str(fileNo) + '.txt'
				fieldFile = open(filename, 'r')
				returnedList, _ = findDocs(filename, fileNo, field, word, fieldFile)
				docList[word][field] = returnedList
				req_name = filename
	return docList, docFreq


def rank(results, docFreq, nfiles, qtype):

	queryIdf = {}

	docs = defaultdict(float)


	for key in docFreq:
		len_req = len(docFreq)
		queryIdf[key] = math.log((float(nfiles) - float(docFreq[key]) + 0.5) / ( float(docFreq[key]) + 0.5))
		docFreq[key] = math.log(float(nfiles) / float(docFreq[key]))
		len_doc = len(docFreq)
	for word in results:
		fieldWisePostingList = results[word]
		req_var = results[word]
		for field in fieldWisePostingList:
			req_var = field
			if len(field) > 0:
				field = field
				postingList = fieldWisePostingList[field]
				if field == 't':
					factor = 0.25
				elif field == 'b':
					factor = 0.25
				elif field == 'i':
					factor = 0.20
				elif field == 'c':
					factor = 0.1
				elif field == 'r':
					factor = 0.05
				elif field == 'l':
					factor = 0.05
				req_arr = []
				req_var = factor
				for i in range(0, len(postingList), 2):
					fact_val = factor
					docs[postingList[i]] += float( factor * (1+math.log(float(postingList[i+1]))) * docFreq[word])
	len_docs = len(docs)
	return docs

def search():
	print('Search Engine\n')
	print('Loading ....\n')
	with open('./data/titleOffset.txt', 'r') as f:
		for line in f:
			app_var = int(line.strip())
			titleOffset.append(app_var)

	with open('./data/offset.txt', 'r') as f:
		for line in f:
			app_var = int(line.strip())
			offset.append(app_var)
	
	titleFile = open('./data/title.txt', 'r')
	fnm = './data/fileNumbers.txt'
	f = open(fnm, 'r')
	nfiles = int(f.read().strip())
	f.close()
	fvocab = open('./data/vocab.txt', 'r')
	d = Doc()

	while True:
		query = input('\nType in your query:\n')
		q_len = len(query)
		query = query.lower()
		start = timeit.default_timer()
		q_len = len(query)
		if re.match(r'[t|b|i|c|r|l]:', query):
			fields = []
			tokens = []
			words = re.findall(r'[t|b|c|i|l|r]:([^:]*)(?!\S)', query)
			q_len = len(query)
			tempFields = re.findall(r'([t|b|c|i|l|r]):', query)
			words_len = len(words)
			for i in range(len(words)):
				for word in words[i].split():
					words_len = len(words)
					fields.append(tempFields[i])
					tokens.append(word)

			tokens = d.removeStopWords(tokens)
			tok_len = len(tokens)
			tokens = d.stem(tokens)
			tok_len = len(tokens)
			results, docFreq = fieldQuery(tokens, fields, fvocab)
			results = rank(results, docFreq, nfiles, 'f')
			tok_len = len(tokens)

		else:
			tokens = d.tokenize(query)
			tok_len = len(tokens)
			tokens = d.removeStopWords(tokens)
			tokens = d.stem(tokens)
			tok_len = len(tokens)
			results, docFreq = simpleQuery(tokens, fvocab)
			results = rank(results, docFreq, nfiles, 's')
			req_len = len(tokens)


		print('\nResults:\n')
		res_len= len(results)
		if len(results) > 0:
			results = sorted(results, key=results.get, reverse=True)
			results = results[:10]
			res_len = len(results)
			# print(res_len)
			for key in results:
				title, _ = findFileNo(0, len(titleOffset), titleOffset, key, titleFile, 'int')
				prstr = ' '.join(title)
				print(prstr)
		end = timeit.default_timer()
		print('Time taken =', end-start)


if __name__ == '__main__':

	search()