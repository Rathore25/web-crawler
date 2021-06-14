# -*- coding: utf-8 -*-
"""
Created on Thu Nov 12 14:45:29 2020

@author: nitish
"""
from elasticsearch import Elasticsearch
import json
from TRECParser import TRECParser
import time

class MergeIndex():
    def __init__(self):
        self.cloudId            = "IR-HW3-V1:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvJGIzYzc0YTdmZGNhNTQ1NDFiYjZmYjdiNWFlMTEyOTIyJDI4NjQyOTZmNDQ4YjQyZGNiYWMyZjZmMWNlZjJjNmQ5"
        self.elasticUserName    = "elastic"
        self.elasticPassword    = "0WzIfeDMIyeO4V9HQfGuRfeI"
        self.mergedIndexName    = "irfa2020g4v2"
    
    def getClient(self):
        client = Elasticsearch(cloud_id = self.cloudId, http_auth = (self.elasticUserName, self.elasticPassword), request_timeout = 10000, timeout = 10000)
        return client
    
    def createMergedIndex(self):
        settingsPath    = "./MainMergedIndexSettings.json"
        body            = {}
        
        with open(settingsPath) as f:
            body = json.load(f)
        
        client      = self.getClient()
        response    = client.indices.create(index=self.mergedIndexName, body=body)
        
        print(response)
    
    def pushDocuments(self):
        documentsPath   = './TransformedBatchDocuments/'
        parser          = TRECParser("", 10)
        filePaths       = parser.getFilePaths(documentsPath)
        startTime       = time.time()
        
        for filePath in filePaths:
            print("Parsing file path:", filePath)
            data    = parser.getParsedFile(filePath)
            
            print("Docs count:", len(data))
            for index, item in enumerate(data):
                print(index)
                self.push(item["id"],item["title"],item["content"],item["inlinks"],item["outlinks"],int(item["wavenumber"]))
            
            print("Parsed :", filePath)
            
        print("Processing complete! Time taken:", time.time() - startTime)
    
    def push(self, id, title, content, inlinks, outlinks, waveNumber):
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
                    "inlinklist": inlinks,
                    "outlinklist": outlinks
                    }
                },
            "upsert":{
                "title": title,
                "content": content,
                "inlinks": inlinks,
                "outlinks": outlinks
                }
            }
                                
        client      = self.getClient()
        response    = client.update(index=self.mergedIndexName, id=id, body=body)

#merger = MergeIndex()
#merger.createMergedIndex()
#merger.pushDocuments()