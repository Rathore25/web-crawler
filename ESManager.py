# -*- coding: utf-8 -*-
"""
Created on Wed Nov  4 17:42:05 2020

@author: nitish
"""
from URLHelper import URLHelper
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import json

class ESManager():
    def __init__(self):
        self.URLHelper      = URLHelper()
        self.robotIndexName = "robotinfo"
        self.batchDocIndex  = "crawleddocuments"
        self.mainIndexName  = "websitecontents"
    
    def getClient(self):
        client = Elasticsearch([{'host':'localhost'}])
        return client
    
    def createIndex(self, indexName, body):
        print('Creating Index:', indexName)    
        client      = self.getClient()
        response    = client.indices.create(index=indexName, body=body)
        return response
    
    def push(self, index, id, body):
        client      = self.getClient()
        response    = client.index(index = index, id = id, body = body)
        return response
    
    def search(self, index, termField, termValue):
        client      = self.getClient()
        searchBody  =   {            
                          "query": {
                            "term": {
                              termField: termValue
                            }
                          }
                        }
        
        return client.search(searchBody, index)
    
    def createCrawledDocumentsIndex(self):
        settingsPath    = "./Settings/ES_CrawledDocsIndexSettings.json"
        body            = {}
        
        with open(settingsPath) as f:
            body = json.load(f)
        
        response = self.createIndex(self.batchDocIndex, body)
        print("Chernobyl index created:",response)
    
    def pushCrawledDocuments(self, batchId, content):
        body        = {'content':content}
        response    = self.push(self.batchDocIndex, batchId, body)
    
    def createRobotIndex(self):
        settingsPath    = "./Settings/ES_RobotIndexSettings.json"
        body            = {}
        
        with open(settingsPath) as f:
            body = json.load(f)
        
        response = self.createIndex(self.robotIndexName, body)
        print("Robot index created:",response)
    
    def insertRobotInfo(self, url:str, content:str):
        components  = self.URLHelper.getComponents(url)
        url         = components.netloc
        body        = {'content':content}
        response    = self.push(self.robotIndexName, url, body)
    
    def getRobotInfo(self, url):
        components  = self.URLHelper.getComponents(url)
        key         = components.netloc
        
        response    = self.search(self.robotIndexName, "_id", key)
        result      = None
        if response is not None and response['hits']['total']['value'] > 0:
            result  = response['hits']['hits'][0]['_source']['content']

        return result
    
    def createMainIndex(self):
        settingsPath    = "./Settings/WebsiteContentIndexSettings.json"
        body            = {}
        
        with open(settingsPath) as f:
            body = json.load(f)
        
        response = self.createIndex(self.mainIndexName, body)
        print("Chernobyl index created:",response)
    
        
    def set_data(self, inputData, indexName):
        for item in inputData:
            id      = item['id']
            body    = {
                'content': item['content'],
                'inlinks': item['inlinks'],
                'outlinks': item['outlinks'],
                'wavenumber': int(item['wavenumber'])
                }
            
            yield {
                "_index": indexName,
                "_id": id,
                "_source": body
            }

    def bulkPush(self, data):
        client      = self.getClient()
        success, _  = bulk(client, self.set_data(data, self.mainIndexName))
    
    def pushMainIndex(self, id, content, inlinks, outlinks, waveNumber):
        body = {
            "script": {
                "lang":"painless",
                "source":"""
                    int total = params.inlinklist.size();
                    for(int i = 0; i < total; ++i) {
                      if (ctx._source.inlinks.contains(params.inlinklist[i]) == false) {
                        ctx._source.inlinks.add(params.inlinklist[i])
                        }
                      }
                    total = params.outlinklist.size();
                    for(int i = 0; i < total; ++i) {
                      if (ctx._source.outlinks.contains(params.outlinklist[i]) == false) {
                        ctx._source.outlinks.add(params.outlinklist[i]);
                        }
                      }
                """,
                "params":{
                    "inlinklist":inlinks,
                    "outlinklist":outlinks
                    }
                },
            "upsert":{
                "content": content,
                "inlinks": inlinks,
                "outlinks": outlinks,
                "wavenumber": waveNumber
                }
            }
        
        print(body)
                                
        client      = self.getClient()
        response    = client.update(index=self.mainIndexName,id=id,body=body)
        
        print(response)

# ********************* Run it once at the start to create the important indices ********************************
#esm = ESManager()
#esm.createRobotIndex()
#esm.createCrawledDocumentsIndex()
# ****************************************************** END ****************************************************


# ********************* used for testing ************************************************************************
#esm.createMainIndex()
#esm.pushMainIndex("google.com","Let us check if the stemming is working or not. Work worked worker working",["a","b"],["1","2"],1)
#esm.pushMainIndex("google.com","interesting content",["e","f"],["4","9"],1)
#esm.insertRobotInfo("http://www.robotc.com", [], [], 5)
#esm.getRobotInfo("http://www.robotz.com")