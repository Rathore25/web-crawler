# -*- coding: utf-8 -*-
"""
Created on Mon Nov  2 20:13:14 2020

@author: nitish
"""
import logging
from Helper import Helper
from WebAPIs import WebAPIs
from URLHelper import URLHelper
from ESManager import ESManager
from RedisManager import RedisManager
import time
import math

class MainCrawler():
    def __init__(self):
        self.helper     = Helper()
        self.apis       = WebAPIs()
        self.URLHelper  = URLHelper()
        self.ESManager  = ESManager()
        self.RMManager  = RedisManager()
        
        # Run at start
        self.ESManager.start_up()
        self.RMManager.start_up()
    
    def main(self):
        # get crawl settings
        settings    = self.helper.getSettings()
        maxURLs     = settings['totalDocs']
        batchSize   = settings['batchSize']
        
        # initialize counters and seeds
        count       = 0
        seeds       = self.helper.getSeeds()
        batchNumber = 1
        documents   = []
        
        isAllowedToCrawlMap = {}
        crawlDelayMap       = {}
        currentWave         = list(seeds)
        currWaveNumber      = 0
        nextWave            = {}
        startTime           = time.time()
        
        while len(currentWave) > 0:
            # url to be processed
            currentURL, currWaveNumber, prevURLs = currentWave.pop(0)
            print("Crawl count:", count)
            print("Current URL:", currentURL, "Wavenumber:", currWaveNumber)
                
            try:
                # if URL is already crawled, then just update the inlinks
                isURLAlreadyCrawled = self.RMManager.isAlreadyCrawled(currentURL)
                
                if isURLAlreadyCrawled == True:
                    print("URL is already crawled!")
                    if prevURLs != "":
                        self.RMManager.updateInlinks(currentURL, list(prevURLs.split(',')))
                    continue
                
                # if URL is not crawled but it could not be accessed, mark it as a bad URL
                if self.RMManager.isBadURL(currentURL) == True:
                    print("Bad URL!")
                    continue
                
                # if URL is in a restricted domain, this can be set at runtime in REDIS
                if self.RMManager.isBadDomain(currentURL) == True:
                    print("Bad Domain!")
                    continue
                
                # get response of header request
                currentURL, code, headers   = self.apis.getHeaders(currentURL)
                
                # validate if it is a valid header
                isValidURLHeader            = self.URLHelper.isValidURLHeader(code, headers)
                
                if isValidURLHeader == False:
                    print("Not a valid URL Header!")
                    self.RMManager.pushBadURL(currentURL)
                    continue
                
                # get crawl permissions
                isAllowedToCrawl, crawlDelay = self.getURLCrawlPermissions(currentURL, isAllowedToCrawlMap, crawlDelayMap)
                
                if isAllowedToCrawl == False:
                    self.RMManager.pushBadURL(currentURL)
                    continue
                
                if crawlDelay > 30:
                    print("Crawl delay is too long:", crawlDelay)
                    self.RMManager.pushBadDomain(currentURL)
                    continue
                
                # get request to fetch content from the URL
                content = self.apis.getContent(currentURL)
                
                if 'ERROR_IN_GET' == content:
                    print("Error while fetching content!")
                    self.RMManager.pushBadURL(currentURL)
                    continue
                
                # sleep for crawling politeness
                time.sleep(crawlDelay)
                
                # parse title and text from content
                title, text = self.URLHelper.parseTitleTextFromContent(content)
                
                if text.strip() == "":
                    print("Empty text!")
                    self.RMManager.pushBadURL(currentURL)
                    continue
                
                document    = (currentURL, title, text)
                documents.append(document)
                
                # parse and filter urls from content
                nextURLs    = self.URLHelper.getCanonizedURLs(content, currentURL)
                
                # push the urls in next wave
                self.updateNextWave(nextURLs, currentURL, nextWave, currWaveNumber + 1)
                
                # push currentURL to crawled urls in REDIS
                inlinks     = []
                outlinks    = nextURLs
                
                if prevURLs != "":
                    inlinks = list(prevURLs.split(','))
                
                self.RMManager.pushURLInfo(currentURL, currWaveNumber, inlinks, outlinks)
                
                # crawled a URL successfully!
                count += 1

                print("Current url crawl complete!")
                
                # if a batch of documents is crawled, we save it on disk
                if (count % batchSize) == 0:
                    self.saveDocument(batchNumber, documents)
                    # reset documents and increment batchNumber for next batch
                    documents   = []
                    batchNumber += 1
                    print("Saved a batch of documents! Time expired:", (time.time() - startTime))
                
                if count > maxURLs:
                    print("Maximum number of URLs have been crawled!")
                    break
            except:
                #e = sys.exc_info()[0]
                print("ERROR_IN_CRAWL. URL:", currentURL)
                logging.exception("ERROR_IN_CRAWL. URL:" + currentURL)
            finally:
                # if currentWave is empty, push in the next wave                
                if len(currentWave) == 0:
                    currentWave = self.getCurrentWave(nextWave)
                    print("Completed current wave. Going to next wave. Next wave number:",str(currWaveNumber + 1))
                    nextWave    = {}
            
        
        print("Final wave number:", currWaveNumber)
        print("Total time taken:", time.time() - startTime)

    def getCurrentWave(self, nextWave):
        if len(nextWave) == 0:
            return []
        
        currentWave     = list(nextWave.keys())
        totalInlinks    = 0
        
        # calculate total inlinks in the next wave
        for url, value in nextWave.items():
            count                   = (value['inlinks']).count(',') + 1
            nextWave[url]['count']  = count
            totalInlinks            += count
        
        # calculate the score of each URL
        for url, value in nextWave.items():            
            inlinkCount = value['count']
            
            # calculate based on terms matched
            score       = value['score']
            
            # calculate based on total inlinks
            score       += math.log(inlinkCount/totalInlinks)
            
            # update score
            nextWave[url]['score'] = score
        
        # sort in descending order based on score
        currentWave = sorted(currentWave, key = lambda url: nextWave[url]['score'], reverse=True)
        
        currentWave = [(url, nextWave[url]['wave'], nextWave[url]['inlinks']) for url in currentWave]
        
        return currentWave
        
            
    def updateNextWave(self, urls:list, prevURL, nextWave, nextWaveNumber):
        settings    = self.helper.getSettings()
        terms       = settings['searchTerms']
        length      = len(terms)
        
        for url in urls:
            if url not in nextWave:
                matchCount      = 0
                lowerURL        = url.lower()
                score           = 0
                
                for term in terms:
                    if term in lowerURL:
                        matchCount += 1
                
                if matchCount == 0:
                    continue
                else:
                    score = math.log(matchCount/length)
                
                nextWave[url] = { 'score':score, 'inlinks':prevURL, 'count': 0, 'wave': nextWaveNumber }
            else:
                nextWave[url]['inlinks'] += ',' + prevURL

    def getRobotInfo(self, currentURL):
        robotInfo = self.ESManager.getRobotInfo(currentURL)
        
        if robotInfo is None:
            robotInfo = self.apis.getRobotTxt(currentURL)
            self.ESManager.insertRobotInfo(currentURL, robotInfo)
        
        return robotInfo
    
    def getURLDomain(self, url):
        components = self.URLHelper.getComponents(url)
        return components.netloc
    
    def getURLCrawlPermissions(self, currentURL, isAllowedToCrawlMap, crawlDelayMap):
        isAllowedToCrawl    = True
        crawlDelay          = 1
        urlDomain           = self.getURLDomain(currentURL)
        
        if urlDomain not in isAllowedToCrawlMap:
            # get robots.txt info
            robotInfo           = self.getRobotInfo(currentURL)
            
            # validate if the domain has a valid robots.txt
            isValidRobotInfo    = self.URLHelper.isValidRobotInfo(robotInfo)
            if isValidRobotInfo == False:
                print("Not a valid robots.txt!")
                isAllowedToCrawl = False
            else:
                # validate if allowed to crawl current URL
                isAllowedToCrawl    = self.URLHelper.isAllowedToCrawl(currentURL, robotInfo)
                if isAllowedToCrawl == False:
                    print("Not allowed to crawl!")
            
            # get time in seconds to wait before next request
            crawlDelay          = self.URLHelper.getCrawlDelay(robotInfo)
            
            isAllowedToCrawlMap[urlDomain]  = isAllowedToCrawl
            crawlDelayMap[urlDomain]        = crawlDelay
        else:
            isAllowedToCrawl    = isAllowedToCrawlMap[urlDomain]
            crawlDelay          = crawlDelayMap[urlDomain]
        
        return (isAllowedToCrawl, crawlDelay)
    
    def saveDocument(self, batchNumber, documents):
        try:
            fileName = 'batch_'+str(batchNumber)
            
            print("Length of docs:", len(documents))
            
            data = ""
            
            for (url,title,text) in documents:
                data += "<DOC>\n"
                data += "<DOCNO>" + url + "</DOCNO>\n"
                data += "<TITLE>" + title + "</TITLE>\n"
                data += "<TEXT>\n"
                data += text
                data += "\n</TEXT>\n"
                data += "</DOC>\n"
                 
            self.ESManager.pushCrawledDocuments(fileName, data)
        except:
            print("***************************ERROR_IN_SAVING*******************************:", fileName)
            logging.exception("ERROR_IN_SAVING. Batch:" + fileName)

logging.basicConfig(filename="logFile.txt",
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S")

mc = MainCrawler()
mc.main()