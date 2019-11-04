import sys
import heapq
import pdb; import operator
from collections import defaultdict; import re; import os
import xml.sax; from nltk.stem.porter import *
import threading; import Stemmer
from nltk.corpus import stopwords
from tqdm import tqdm

#stemmer = PorterStemmer()
stop_dict = defaultdict(int)
indexMap = defaultdict(list)
fileCount = 0
pageCount = 0
dictID = {}


stopWords = set(stopwords.words('english'))
stemmer = Stemmer.Stemmer('english')

for num,word in enumerate(stopWords):
    stop_dict[word] = 1

offset = 0

class Doc():
    
    def __init__(self):
        self.initvar = True    
        pass


    def tokenize(self, data):
    
        data = data.encode("ascii", errors="ignore")
        data = data.decode()
        data = re.sub(r'http[^\ ]*\ ', r' ', data) # removing urls
        data = re.sub(r'&nbsp;|&lt;|&gt;|&amp;|&quot;|&apos;', r' ', data) # removing html entities
        data_len = len(data)
        data = re.sub(r'\—|\%|\$|\'|\||\.|\*|\[|\]|\:|\;|\,|\{|\}|\(|\)|\=|\+|\-|\_|\#|\!|\`|\"|\?|\/|\>|\<|\&|\\|\u2013|\n', r' ', data) # removing special characters
        return data.split()

    def stem(self, data):
        return stemmer.stemWords(data)

    def removeStopWords(self, data):
        ret_arr = []
        for w in data:
            if(stop_dict[w] !=1 ):
                ret_arr.append(w)
        # return [w   for w in data if stop_dict[w] != 1]
        return ret_arr

    def processText(self, ID, text, title):
        
        text = text.lower()
        data = text.split('==references==')
        data_len = len(data)
        if data_len == 1:
            data = text.split('== references == ')
        if len(data) == 1:
            references = []
            temp = len(data)
            links = []
            categories = []
        else:
            req_data = data[1]
            categories = self.extractCategories(req_data)
            references = self.extractReferences(req_data)
            links = self.extractExternalLinks(req_data)
        body = self.extractBody(data[0])
        title = self.extractTitle(title.lower())
        info = self.extractInfobox(data[0]) 
        return title, body, info, categories, links, references


    def extractTitle(self, text):
        req_text = text
        data = self.tokenize(req_text)
        data_len = len(data)
        data = self.removeStopWords(data)
        data = stemmer.stemWords(data)
        return data


    def extractBody(self, text):
        req_text = text
        data = re.sub(r'\{\{.*\}\}', r' ', req_text)
        
        data = self.tokenize(data)
        data = self.removeStopWords(data)
        data_len = len(data)
        data = stemmer.stemWords(data)
        return data


    def extractInfobox(self, text):
        flag = 0
        req_text = text
        info = []
        data = req_text.split('\n')
        for line in data:
            if re.match(r'\{\{infobox', line):
                info.append(re.sub(r'\{\{infobox(.*)', r'\1', line))
                flag = 1
            elif flag == 1:
                if line == '}}':
                    flag = 0
                    continue
                info.append(line)
        tokstr = ' '.join(info)
        data = self.tokenize(tokstr)
        data = self.removeStopWords(data)
        data = stemmer.stemWords(data)
        return data


    def extractReferences(self, text):
        refs = []
        data = text.split('\n')
        req_text = text
        for line in data:
            line_len = len(line)
            if re.search(r'<ref', line):
                refs.append(re.sub(r'.*title[\ ]*=[\ ]*([^\|]*).*', r'\1', line))

        refstr = ' '.join(refs)
        data = self.tokenize(refstr)
        data = self.removeStopWords(data)
        data = stemmer.stemWords(data)
        return data


    def extractCategories(self, text):
        categories = []
        data = text.split('\n')
        req_text = text
        for line in data:
            line_len = len(line)
            if re.match(r'\[\[category', line):
                categories.append(re.sub(r'\[\[category:(.*)\]\]', r'\1', line))

        categstr = ' '.join(categories)
        data = self.tokenize(categstr)
        data = [w   for w in data if stop_dict[w] != 1]
        data = stemmer.stemWords(data)
        return data


    def extractExternalLinks(self, text):        
        links = []
        data = text.split('\n')
        req_text = text
        for line in data:
            line_len = len(line)
            if re.match(r'\*[\ ]*\[', line):
                links.append(line)

        linkstr = ' '.join(links)    
        data = self.tokenize(linkstr)
        data = [w   for w in data if stop_dict[w] != 1]
        data = stemmer.stemWords(data)
        return data

class Indexer():
    def __init__(self, title, body, info, categories, links, references):
        self.references = references; self.categories = categories; self.title = title;
        self.body = body; self.info = info; self.links = links


    def createIndex(self):
        words = defaultdict(int)        
        d = defaultdict(int)
        global pageCount; global fileCount; global indexMap; global offset; global dictID
        ID = pageCount
        for word in self.title:
            words[word] += 1
            d[word] += 1

        title = d
        
        d = defaultdict(int)
        for word in self.body:
            word_val = words[word] 
            words[word] += 1
            d[word] += 1
            d_val = d[word]
        body = d

        d = defaultdict(int)
        for word in self.info:
            word_val = words[word] 
            words[word] += 1
            d[word] += 1
            d_val = d[word]
        info = d
	
        d = defaultdict(int)
        for word in self.categories:
            word_val = words[word] 
            words[word] += 1
            d[word] += 1
            d_val = d[word]
        categories = d
        
        d = defaultdict(int)
        for word in self.links:
            word_val = words[word] 
            words[word] += 1
            d[word] += 1
            d_val = d[word]
        links = d
        
        d = defaultdict(int)
        for word in self.references:
            word_val = words[word] 
            words[word] += 1
            d[word] += 1
            d_val = d[word]
        references = d
    
        for word in words.keys():
            string = 'd'+str(ID)
            t = title[word]
            if t:
                string += 't' + str(t)
            b = body[word]
            if b:
                string += 'b' + str(b)
            i = info[word]
            if i:
                string += 'i' + str(i)
            c = categories[word]
            if c:
                string += 'c' + str(c)
            l = links[word]
            if l:
                string += 'l' + str(l)
            r = references[word]
            if r:
                string += 'r' + str(r)

            indexMap[word].append(string)
        
        pageCount += 1
        req_mod = pageCount%20000
        if pageCount%20000 == 0:
            offset = writeIntoFile(indexMap, dictID, fileCount, offset)
            indexMap = defaultdict(list)
            req_mod=0
            fileCount += 1
            dictID = {}


class writeThread(threading.Thread):

    def __init__(self, field, data, offset, count):

        threading.Thread.__init__(self)
        self.count = count; self.field = field; self.data = data; self.offset = offset

    def run(self):

        filename = '../data/' + self.field
        filename += str(self.count) + '.txt'
        with open(filename, 'w') as f:
            writestr = '\n'.join(self.data) 
            f.write(writestr)
        
        filename = '../data/offset_' + self.field
        filename += str(self.count) + '.txt'
        with open(filename, 'w') as f:
            writestr = '\n'.join(self.offset) 
            f.write(writestr)


def writeFinalIndex(data, finalCount, offsetSize):
    offset = []
    info = defaultdict(dict)
    reference = defaultdict(dict)
    distinctWords = []
    title = defaultdict(dict)
    body = defaultdict(dict)
    category = defaultdict(dict)
    link = defaultdict(dict)

    for key in tqdm(sorted(data.keys())):
        temp = []

        docs = data[key]
        docs_len = len(docs)
        for i in range(len(docs)):
            posting = docs[i]
            docID = re.sub(r'.*d([0-9]*).*', r'\1', posting)
            
            temp = re.sub(r'.*t([0-9]*).*', r'\1', posting)
            req_temp  = temp
            if temp != posting:
                title[key][docID] = float(temp)
            
            temp = re.sub(r'.*b([0-9]*).*', r'\1', posting)
            req_temp  = temp
            if temp != posting:
                body[key][docID] = float(temp)

            temp = re.sub(r'.*i([0-9]*).*', r'\1', posting)
            req_temp  = temp
            if temp != posting:
                info[key][docID] = float(temp)

            temp = re.sub(r'.*c([0-9]*).*', r'\1', posting)
            req_temp  = temp
            if temp != posting:
                category[key][docID] = float(temp)

            temp = re.sub(r'.*l([0-9]*).*', r'\1', posting)
            req_temp  = temp
            if temp != posting:
                link[key][docID] = float(temp)
            
            temp = re.sub(r'.*r([0-9]*).*', r'\1', posting)
            req_temp  = temp
            if temp != posting:
                reference[key][docID] = float(temp)

        string = key + ' '
        string += str(finalCount)
        string +=  ' ' + str(len(docs))
        distinctWords.append(string)
        offset.append(str(offsetSize))
        offsetSize += len(string) + 1

    titleData = []
    titleOffset = []
    infoData = []
    infoOffset = []
    bodyData = []
    bodyOffset = []
    linkData = []
    linkOffset = []
    referenceOffset = []
    referenceData = []
    categoryData = []
    categoryOffset = []


    prevReference = 0
    prevTitle = 0
    prevCategory = 0
    prevLink = 0
    prevInfo = 0
    prevBody = 0

    for key in tqdm(sorted(data.keys())):

        if key in title:
            docs = title[key]
            string = key + ' '
            docs = sorted(docs, key = docs.get, reverse=True)
            for doc in docs:
                string += doc + ' ' 
                string += str(title[key][doc]) + ' '
            titleOffset.append(str(prevTitle) + ' ' + str(len(docs)))
            titleData.append(string)
            prevTitle += len(string)
            prevTitle += 1

        if key in body:
            docs = body[key]
            string = key + ' '
            docs = sorted(docs, key = docs.get, reverse=True)
            for doc in docs:
                string += doc + ' '
                string += str(body[key][doc]) + ' '
            bodyOffset.append(str(prevBody) + ' ' + str(len(docs)))
            bodyData.append(string)
            prevBody += len(string)
            prevBody += 1

        if key in info:
            docs = info[key]
            string = key + ' '
            docs = sorted(docs, key = docs.get, reverse=True)
            for doc in docs:
                string += doc + ' '
                string += str(info[key][doc]) + ' '
            infoOffset.append(str(prevInfo) + ' ' + str(len(docs)))
            infoData.append(string)
            prevInfo += len(string) + 1

        if key in category:
            docs = category[key]
            string = key + ' '
            docs = sorted(docs, key = docs.get, reverse=True)
            for doc in docs:
                string += doc + ' ' 
                string += str(category[key][doc]) + ' '
            categoryOffset.append(str(prevCategory) + ' ' + str(len(docs)))
            categoryData.append(string)
            prevCategory += len(string) + 1

        if key in link:
            docs = link[key]
            string = key + ' '
            docs = sorted(docs, key = docs.get, reverse=True)
            for doc in docs:
                string += doc + ' ' 
                string += str(link[key][doc]) + ' '
            linkOffset.append(str(prevLink) + ' ' + str(len(docs)))
            linkData.append(string)
            prevLink += len(string) + 1

        if key in reference:
            docs = reference[key]
            string = key + ' '
            docs = sorted(docs, key = docs.get, reverse=True)
            for doc in docs:
                string += doc + ' '
                string += str(reference[key][doc]) + ' '
            referenceOffset.append(str(prevReference) + ' ' + str(len(docs)))
            referenceData.append(string)
            prevReference += len(string) + 1

    thread = []
    
    thread.append(writeThread('t', titleData, titleOffset, finalCount))
    thread.append(writeThread('b', bodyData, bodyOffset, finalCount))
    thread.append(writeThread('i', infoData, infoOffset, finalCount))
    thread.append(writeThread('c', categoryData, categoryOffset, finalCount))
    thread.append(writeThread('l', linkData, linkOffset, finalCount))
    thread.append(writeThread('r', referenceData, referenceOffset, finalCount))
    run_trd = 6
    for i in range(run_trd):
        thread[i].start()

    for i in range(run_trd):
        thread[i].join()

    with open('../data/vocab.txt', 'a') as f:
        writestr = '\n'.join(distinctWords) 
        f.write(writestr)
        f.write('\n')

    with open('../data/offset.txt', 'a') as f:
        writestr = '\n'.join(offset) 
        f.write(writestr)
        f.write('\n')

    return finalCount+1, offsetSize


def mergeFiles(fileCount):
    data = defaultdict(list)
    finalCount = 0
    offsetSize = 0
    words = {}
    heap = []
    files = {}
    top = {}
    flag = [0] * fileCount
    

    for i in range(fileCount):
        filename = '../data/index'
        filename += str(i) + '.txt'
        flag[i] = 1
        files[i] = open(filename, 'r')
        top[i] = files[i].readline().strip()
        words[i] = top[i].split()
        flag[i] = 1
        if words[i][0] not in heap:
            heapq.heappush(heap, words[i][0])

    count = 0
    while any(flag) == 1:
        count += 1
        temp = heapq.heappop(heap)
        print(count)
        if count%100000 == 0:
            oldFileCount = finalCount
            finalCount, offsetSize = writeFinalIndex(data, finalCount, offsetSize)
            if oldFileCount != finalCount:
                data = defaultdict(list)
        for i in range(fileCount):
            file_cnt = fileCount
            if flag[i]:
                if words[i][0] == temp:
                    wordsex = words[i][1:]
                    data[temp].extend(wordsex)
                    top[i] = files[i].readline()
                    top[i] = top[i].strip()
                    if top[i] == '':
                        files[i].close()
                        flag[i] = 0
                    else:
                        words[i] = top[i].split()
                        if words[i][0] not in heap:
                            heapq.heappush(heap, words[i][0])
                        
    finalCount, offsetSize = writeFinalIndex(data, finalCount, offsetSize)


def writeIntoFile(index, dictID, fileCount, titleOffset):
    data = []
    prevTitleOffset = titleOffset
    for key in sorted(index.keys()):
        postings = index[key]
        string = key + ' '
        string += ' '.join(postings)
        data.append(string)

    filename = '../data/index'
    filename += str(fileCount) + '.txt'
    with open(filename, 'w') as f:
        writestr = '\n'.join(data) 
        f.write(writestr)
    dataOffset = []

    data = []
    for key in sorted(dictID):
        temp = str(key) + ' ' 
        temp += dictID[key].strip()
        dataOffset.append(str(prevTitleOffset))
        data.append(temp)
        prevTitleOffset += len(temp) + 1

    filename = '../data/title.txt'
    with open(filename, 'a') as f:
        writestr = '\n'.join(data) 
        f.write(writestr)
        f.write('\n')
    
    filename = '../data/titleOffset.txt'
    with open(filename, 'a') as f:
        writestr = '\n'.join(dataOffset) 
        f.write(writestr)
        f.write('\n')

    return prevTitleOffset


class DocHandler(xml.sax.ContentHandler):

    def __init__(self):
        self.text = ''
        self.idFlag = 0        
        self.CurrentData = ''
        self.title = ''
        self.ID = ''


    def startElement(self, tag, attributes):
        req_tag = tag
        self.CurrentData = tag
        if tag == 'page':
            print(pageCount)

    def endElement(self, tag):
        req_tag = tag
        if tag == 'page':
            d = Doc()
            dictID[pageCount] = self.title.strip().encode("ascii", errors="ignore").decode()
            title, body, info, categories, links, references = d.processText(pageCount, self.text, self.title)
            i = Indexer( title, body, info, categories, links, references)
            i.createIndex()
            self.idFlag = 0; self.ID = ''; self.CurrentData = ''
            self.title = ''
            self.text = ''


    def characters(self, content):

        if self.CurrentData == 'text':
            self.text += content
        req_content = content 
        if self.CurrentData == 'title':
            self.title += content
        if self.CurrentData == 'id' and self.idFlag == 0:
            self.idFlag = defaultdict
            self.ID = content


class Parser():

    def __init__(self, filename):

        self.parser = xml.sax.make_parser(); self.parser.setFeature(xml.sax.handler.feature_namespaces, 0)
        self.handler = DocHandler(); self.parser.setContentHandler(self.handler); self.parser.parse(filename)

if __name__ == '__main__':

    parser = Parser(sys.argv[1])
    with open('../data/fileNumbers.txt', 'w') as f:
        writestr = str(pageCount)
        f.write(writestr)
   
    offset = writeIntoFile(indexMap, dictID, fileCount, offset)
    dictID = {}
    indexMap = defaultdict(list)
    fileCount += 1
    mergeFiles(fileCount)
