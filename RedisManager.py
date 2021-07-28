# -*- coding: utf-8 -*-
"""
Created on Thu Nov  5 22:30:24 2020

@author: nitish
"""

import redis
import urllib
import sys

class RedisManager():
    def __init__(self):
        self.pool               = redis.ConnectionPool(host='127.0.0.1', port=6379, db=0)
        self.crawledURLKeysName = "CrawledURLs"
        self.badURLsKeyName     = "BadURLs"
        self.nextWaveKeyName    = "NextWaveURLs"
        self.waveNumberKeyName  = "WN_"
        self.badDomain          = "BadDomains"
    
    def start_up(self):
        try:
            self.pushURLInfo("abcd", -1 ,["b"], ["a"])
            self.pushBadURL("abcd_bad")
            self.pushBadDomain("https://web.archive.org")
            self.pushBadDomain("https://curlie.org")
        except:
            print("Unexpected error in redis startup:", sys.exc_info()[0])
    
    def getSet(self, key):
        client      = redis.Redis(connection_pool=self.pool)
        return client.smembers(key)
    
    def pushBadDomain(self, url):
        components  = urllib.parse.urlparse(url)
        domain      = components.netloc.strip().lower()
        
        client      = redis.Redis(connection_pool=self.pool)
        client.sadd(self.badDomain, domain)
        
    def isBadDomain(self, url):
        components  = urllib.parse.urlparse(url)
        domain      = components.netloc.strip().lower()
        
        client      = redis.Redis(connection_pool=self.pool)
        response    = client.sismember(self.badDomain, domain)
        return response
    
    def pushBadURL(self, url):
        client = redis.Redis(connection_pool=self.pool)
        client.sadd(self.badURLsKeyName, url)
    
    def isBadURL(self, url):
        client      = redis.Redis(connection_pool=self.pool)
        response    = client.sismember(self.badURLsKeyName, url)
        return response
    
    def pushURLInfo(self, url:str, waveNumber:int, inlinks:list, outlinks:list):
        client = redis.Redis(connection_pool=self.pool)
        
        client.sadd(self.crawledURLKeysName, url)
        
        client.set(url+":WN", str(waveNumber))
        
        if len(outlinks) > 0:
            client.sadd(url+":OL", *set(outlinks))
        else:
            client.sadd(url+":OL", *set([""]))
            
        if len(inlinks) > 0:
            client.sadd(url+":IL", *set(inlinks))
        else:
            client.sadd(url+":IL", *set([""]))
        
        client.sadd(self.waveNumberKeyName+str(waveNumber),url)
    
    def isAlreadyCrawled(self, url):
        client      = redis.Redis(connection_pool=self.pool)
        response    = client.sismember(self.crawledURLKeysName, url)
        return response
    
    def getURLInfo(self, url):
        client      = redis.Redis(connection_pool=self.pool)
        response    = client.sismember(self.crawledURLKeysName, url)
        
        waveNumber  = None
        inlinks     = None
        outlinks    = None
        
        if response == True:
            waveNumber  = int(client.get(url+":WN"))
            outlinks    = list([str(item, 'utf-8-sig') for item in client.smembers(url+":OL")])
            inlinks     = list([str(item, 'utf-8-sig') for item in client.smembers(url+":IL")])
            
        return (response, waveNumber, inlinks, outlinks)
    
    def updateOutlinks(self, url, outlinks):
        client = redis.Redis(connection_pool=self.pool)
        client.sadd(url+":OL", *set(outlinks))
    
    def updateInlinks(self, url, inlinks):
        client = redis.Redis(connection_pool=self.pool)
        client.sadd(url+":IL", *set(inlinks))

# ******************************** RUN ONCE TO SETUP STARTER KEYS *************************************
#rm = RedisManager()
#rm.pushURLInfo("abcd", -1 ,["b"], ["a"])
#rm.pushBadURL("abcd_bad")
#rm.pushBadDomain("https://web.archive.org")
#rm.pushBadDomain("https://curlie.org")
# ******************************************** END ****************************************************