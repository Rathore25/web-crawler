# -*- coding: utf-8 -*-
"""
Created on Sun Nov  8 07:58:23 2020

@author: nitish
"""
from TRECParser import TRECParser
from RedisManager import RedisManager
from ESManager import ESManager
import logging
import time
import codecs

class Converter():
    def __init__(self):
        self.RMManager          = RedisManager()
        self.ESManager          = ESManager()
        self.batchedESDocPath   = './BatchedDocumentsFromES/'
        self.transformedDocPath = './TransformedBatchDocuments/'
        self.waveInfoPath       = "./waveMap.txt"
        self.indexName          = "crawleddocuments"
    
    # to get all the crawled documents from elastic search and save it in local
    def ESToLocal(self):
        # put the total number of documents in the index
        totalBatches    = 7
        index           = 1
        failedBatches   = []
        startTime       = time.time()
        
        while index <= totalBatches:
            print("Index:",index)
            currBatchId = 'batch_'+str(index)
            try:
                response    = self.ESManager.search(self.indexName, '_id', currBatchId)
                
                if response["hits"]["total"]["value"] == 1:
                    data = response["hits"]["hits"][0]["_source"]["content"]
                
                with codecs.open(self.batchedESDocPath + currBatchId, 'w', 'utf-8-sig') as file:
                    file.write(data)
            except:
                logging.exception("Failed Batch:"+currBatchId)
                failedBatches.append(currBatchId)
            
            index += 1
        
        print("Total time taken:", time.time() - startTime)
        print("Failed batches:", (failedBatches))

    def TransformLocalBatchedDocuments(self):
        startTime       = time.time()
        parser          = TRECParser("")
        batchPaths      = parser.getFilePaths(self.batchedESDocPath)
        batch           = 1
        
        for path in batchPaths:
            print(path)
            
            transformedBatchData    = []
            try:
                # file = { "id":"", "title":"", "content":"", "inlinks":[], "outlinks":[], "wavenumber":"" }
                fileData = parser.getParsedFile(path)    
                
                if fileData is not None and len(fileData) > 0:
                    for file in fileData:
                        url     = file['id']
                        crawled, waveNumber, inlinks, outlinks = self.RMManager.getURLInfo(url)
                        
                        if crawled == False:
                            print("Not crawled: ", url)
                            continue
                        
                        inlinks     = "" if (inlinks is None or len(inlinks) == 0) else ";".join(inlinks)
                        outlinks    = "" if (outlinks is None or len(outlinks) == 0) else ";".join(outlinks)
                        
                        transformedFileData = { 
                            "id"        : url, 
                            "title"     : file["title"], 
                            "content"   : file["content"], 
                            "inlinks"   : inlinks, 
                            "outlinks"  : outlinks, 
                            "wavenumber": str(waveNumber)
                            }
                        
                        transformedBatchData.append(transformedFileData)
            except:
                logging.exception("Error in:" + path)
                print("Error in:", path)
            
            if len(transformedBatchData) > 0:
                strData = ""
                    
                for index, item in enumerate(transformedBatchData):
                    strData += "<DOC>\n"
                    strData += "<DOCNO>" + item["id"] + "</DOCNO>\n"
                    strData += "<TITLE>" + item["title"] + "</TITLE>\n"
                    strData += "<WAVE>" + item["wavenumber"] + "</WAVE>\n"
                    strData += "<INLINK>" + item["inlinks"] + "</INLINK>\n"
                    strData += "<OUTLINK>" + item["outlinks"] + "</OUTLINK>\n"
                    strData += "<TEXT>\n"
                    strData += item["content"]
                    strData += "\n</TEXT>\n"
                    strData += "</DOC>\n"
                
                with codecs.open(self.transformedDocPath + "batch_" + str(batch), 'w', 'utf-8-sig') as file:
                    file.write(strData)
                
                batch += 1
        
        print("Total time:", time.time() - startTime)
    
    def saveWaveInfo(self):
        waveMap = {}
        data    = ""
        
        for index in range(5):
            waveKey = "WN_" + str(index)
            urls    = self.RMManager.getSet(waveKey)
            urls    = list([str(item, 'utf-8-sig') for item in urls])
            data    += waveKey + "|" + ";".join(urls)
            data    += "\n"
        
        with codecs.open(self.waveInfoPath, 'w', 'utf-8-sig') as file:
            file.write(data)
            

cn = Converter()
cn.ESToLocal()
#cn.TransformLocalBatchedDocuments()
#cn.saveWaveInfo()