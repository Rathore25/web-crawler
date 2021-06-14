# -*- coding: utf-8 -*-
"""
Created on Thu Nov 12 09:13:53 2020

@author: nitish
"""

from os import listdir,path
from os.path import isfile, join
import json
import codecs

class TRECParser:
    def __init__(self, dirPath: str, batchSize = 100):
        print('Path:', dirPath)
        self.dirPath                = dirPath
        self.batchSize              = batchSize
        self.parsingProgressFile    = './ParsingProgress.txt'
        self.iterations             = 0
        
    '''
    Get all file names from a directory
    '''
    def getFilePaths(self, dirPath: str):
        filePaths = []
        
        for item in listdir(dirPath):
            filePath = join(dirPath, item)
        
            if isfile(filePath):
                filePaths.append(filePath)
        
        print('Total files:', len(filePaths))
        
        return filePaths
    
    def getParsedFile(self, filePath: str) -> []:
        fileData     = []
        
        print('Parsing:', filePath)
        #with open(filePath) as file:
        with codecs.open(filePath, encoding = 'utf-8-sig') as file:
            currDoc     = { "docNo":"", "title":"", "text":"", "inlinks":[], "outlinks":[], "wavenumber":0 }
            currText    = str('')
            isTextBody  = False
            for line in file:
                # </DOC> - end of document, push the currDoc to the list and reset currDoc
                if line.startswith('</DOC>'):
                    if currDoc['wavenumber'] == "None":
                        currDoc = { "docNo":"", "title":"", "text":"", "inlinks":[], "outlinks":[], "wavenumber":0 }
                        continue
                    
                    fileData.append({
                        'id': currDoc['docNo'], 
                        'title': currDoc['title'],
                        'content': currDoc['text'], 
                        'inlinks': currDoc['inlinks'],
                        'outlinks': currDoc['outlinks'],
                        'wavenumber': currDoc['wavenumber']
                        })
                    
                    currDoc = { "docNo":"", "title":"", "text":"", "inlinks":[], "outlinks":[], "wavenumber":0 }
                    continue
                
                # <DOCNO> - Document number
                if line.startswith('<DOCNO>'):
                    currDoc['docNo'] = line.replace('<DOCNO>','').replace('</DOCNO>','').strip()
                    continue
                
                # <TITLE> - Document title
                if line.startswith('<TITLE>'):
                    currDoc['title'] = line.replace('<TITLE>','').replace('</TITLE>','').strip()
                    continue
                if line.startswith('</TITLE>'):
                    continue
                
                # <WAVE> - Wave number of the URL
                if line.startswith('<WAVE>'):
                    currDoc['wavenumber'] = line.replace('<WAVE>','').replace('</WAVE>','').strip()
                    continue
                
                # <INLINK> - Inlinks of the URL
                if line.startswith('<INLINK>'):
                    inlinks = line.replace('<INLINK>','').replace('</INLINK>','').strip()
                    currDoc['inlinks'] = list(inlinks.split(";"))
                    continue
                
                # <OUTLINK> - Outlinks of the URL
                if line.startswith('<OUTLINK>'):
                    outlinks = line.replace('<OUTLINK>','').replace('</OUTLINK>','').strip()
                    currDoc['outlinks'] = list(outlinks.split(";"))
                    continue
                
                # <TEXT> - text body starts
                if line.startswith('<TEXT>'):
                    isTextBody = True
                    continue
                
                # </TEXT> - text body ends
                if line.startswith('</TEXT>'):
                    isTextBody      = False
                    currDoc['text'] = currText
                    currText        = ''
                    continue
                
                if isTextBody:
                    currText += str(line) + ' '
            
            # end of for line in file
        # end of open(filePath)
        
        return fileData
    
    def saveParsingProgress(self, data):
        with open(self.parsingProgressFile, 'w') as outfile:
            json.dump(data, outfile)
            
    def retrieveParsingProgress(self):
        jsonData = {"lastFile" : "", "lastDoc" : "" }
        
        if path.exists(self.parsingProgressFile):
            with open(self.parsingProgressFile) as file:
                jsonData = json.load(file)
        
        return jsonData
            
    
    def parse(self):
        print("Parsing batch number:", self.iterations)
        dirPath         = self.dirPath
        filePaths       = self.getFilePaths(dirPath)
        totalData       = []
        parsingProgress = self.retrieveParsingProgress()
        lastFile        = parsingProgress["lastFile"]
        lastDoc         = parsingProgress["lastDoc"]
        
        for filePath in filePaths:
            itemIndex   = 0
            length      = 0
            fileData    = []
            
            # reached the last file which was fully processed
            if lastFile != "" and filePath == lastFile and lastDoc == "":
                lastFile = ""
                continue
            
            # not reached the last file which was partially/fully processed
            if lastFile != "" and filePath != lastFile:
                continue
            
            # reached the last file which was partially processed
            if lastFile != "" and filePath == lastFile and lastDoc != "":
                fileData    = self.getParsedFile(filePath)
                length      = len(fileData)
                lastFile    = ""
                
                for index, item in enumerate(fileData):
                    if item['id'] == lastDoc:
                        itemIndex = index + 1
                        break
                
                lastDoc     = ""
                
                if itemIndex >= length:
                    continue
            
            if fileData is None or len(fileData) == 0:
                fileData    = self.getParsedFile(filePath)
                length      = len(fileData)

            # push the new documents            
            for index in range(itemIndex, length):
                totalData.append(fileData[index])
            
            # trim the total data to batch size
            if len(totalData) > self.batchSize:
                totalData = totalData[:self.batchSize]
                lastDoc   = str(totalData[self.batchSize-1]['id'])
                break
            elif len(totalData) == self.batchSize:
                lastDoc  = ""
                break
        
        lastFile = str(filePath)
        
        print('Completed reading files in batch:', self.iterations, 'Document count:', len(totalData))
        
        self.iterations += 1
        
        jsonData = {"lastFile" : lastFile, "lastDoc" : lastDoc }
        self.saveParsingProgress(jsonData)
        
        return totalData