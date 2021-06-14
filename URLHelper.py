# -*- coding: utf-8 -*-
"""
Created on Tue Nov  3 11:43:21 2020

@author: nitish
"""
from bs4 import BeautifulSoup as BS
import urllib
import requests
import urllib.robotparser as robotparser
from Helper import Helper
import string
import logging
import regex

class URLHelper():
    def __init__(self):
        self.helper     = Helper()
    
    def canonizeURL(self, url:str, baseURL:str):
        # if url is empty, return empty
        if url is None or url.strip() == "":
            return ""
        
        # get normalized base url and base url components
        normBaseURL = self.canonizeURL(baseURL, "")
        baseComps   = urllib.parse.urlparse(normBaseURL)
        
        # get current url components
        components  = urllib.parse.urlparse(url)
        # scheme should be in lower case
        scheme      = components.scheme.strip().lower()
        # host name should be in lower case
        netloc      = components.netloc.strip().lower()
        # path should not have duplicate /
        path        = components.path.strip().replace("//", "/")
        # remove the default ports
        if scheme == "http" and netloc.endswith(":80"):
            netloc = netloc[:-3]
        elif scheme == "https" and netloc.endswith(":443"):
            netloc = netloc[:-4]
        
        isRelativeURL = False
        
        if path.startswith("../") or (scheme == '' and netloc == '' and path != ''):
            isRelativeURL = True
        
        # if path is a relative url
        if isRelativeURL == True:
            # set values from base url components
            scheme  = baseComps.scheme
            netloc  = baseComps.netloc
            bPath   = baseComps.path
            
            # get the new path
            if path.startswith("../"):
                path    = path.replace("..","")
                items   = bPath.split("/")
                bLen    = len(items)
                bPath   = "/".join(items[0:bLen-2])
                path    = bPath + path
            else:
                path    = path
        
        # scheme is always https
        scheme = 'https'
            
        # normalized url
        normalizedURL = scheme + "://" + netloc + path
        
        return normalizedURL
    
    def getURLs(self, content):
        soup = BS(content,'html.parser')
        urls = list()
        
        for tag in soup.find_all('a', href=True):
            if 'javascript' not in tag['href']:
                url = tag['href'].strip()
                urls.append(url)
        
        return urls
    
    def getCanonizedURLs(self, content, currentPageURL):
        canonizedURLs   = set()
        nextURLs        = self.getURLs(content)
        
        for url in nextURLs:
            cURL = self.canonizeURL(url, currentPageURL)
            
            if cURL != "://":
                canonizedURLs.add(cURL)
        
        return list(canonizedURLs)
    
    def getTextFromURLContent(self, content):
        soup = BS(content, features="html.parser")
        
        # kill all script and style elements
        for script in soup(["script", "style"]):
            script.extract()    # rip it out

        # get text
        text = soup.get_text(separator = ' ')

        # break into lines and remove leading and trailing space on each
        lines = (line.strip() for line in text.splitlines())
        
        # break multi-headlines into a line each
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        
        # drop blank lines
        text = '\n'.join(chunk for chunk in chunks if chunk)

        return text
    
    def parseTitleTextFromContent(self, content):
        soup        = BS(content, "html.parser")
        lines       = list()
        printable   = set(string.printable)
        title       = ""
        
        try:
            titleSoup   = soup.find('title')
            if titleSoup is not None:
                title   = titleSoup.text
                title   = title.strip()
        except:
            logging.exception("Error_Parse_Content_Title!")
            print("Error_Parse_Content_Title")
        
        try:
            for p in soup.find_all('p'):
                line = p.text
                line = line.strip()
                line = ''.join(filter(lambda x: x in printable, line))
                line = regex.sub('\s+',' ', line)
                lines.append(line)
        except:
            logging.exception("Error_Parse_Content_Text!")
            print("Error_Parse_Content_Text")
        
        text = " ".join(lines)
        
        return title, text
    
    def isValidURLHeader(self, code, headers):
        if str(code).startswith('4') or str(code).startswith('9'):
            print('Invalid code:',code)
            return False
        
        if ('Content-Type' in headers) or  ('content-type' in headers) :
            if ('Content-Type' in headers) and (headers['Content-Type'].strip().lower()).startswith('text') == False:
                print('Invalid Content-Type:', headers['Content-Type'])
                return False
            elif ('content-type' in headers) and (headers['content-type'].strip().lower()).startswith('text') == False:
                print('Invalid Content-Type:', headers['content-type'])
                return False
        else:
            print("No Content-Type!")
            return False
        
        if ('Content-Language' in headers) and ((not headers['Content-Language'].startswith('en-')) and (headers['Content-Language'] != 'en')):
            print('Invalid Content-Language:', headers['Content-Language'])
            return False
        
        if ('content-language' in headers) and ((not headers['content-language'].startswith('en-')) and (headers['content-language'] != 'en')):
            print('Invalid Content-Language:', headers['content-language'])
            return False
        
        return True
    
    def isValidRobotInfo(self, robotInfo):
        try:
            if robotInfo is None:
                return False
            
            if 'FILE_DOES_NOT_EXIST' in robotInfo:
                return True
            
            if  robotInfo.startswith('ERROR_IN_GET'):
                return False
        except:
            print("ERROR_IN_ROBOTINFO. RobotInfo:", robotInfo)
            logging.exception("ERROR_IN_ROBOTINFO. RobotInfo:" + robotInfo)
            return False
            
        return True
    
    def isAllowedToCrawl(self, url:str, robotInfo:str):
        if 'FILE_DOES_NOT_EXIST' in robotInfo:
            return True
        
        rb      = robotparser.RobotFileParser()
        lines   = robotInfo.splitlines()
        rb.parse(lines)
        return rb.can_fetch('*', url)
    
    def getCrawlDelay(self, robotInfo:str):
        rb      = robotparser.RobotFileParser()
        lines   = robotInfo.splitlines()
        rb.parse(lines)
        
        # 1 second is the default delay
        
        delay   = rb.crawl_delay('*')
        
        if delay is None:
            delay = 0.75
        
        return delay
    
    def getComponents(self, url):
        return urllib.parse.urlparse(url)
        
    def testCanonizeURL(self):
        testCases = [
            ("HTTP://www.Example.com/SomeFile.html",""),
            ("http://www.example.com:80",""),
            ("http://www.example.com/a/b.html",""),
            ("../c.html","http://www.example.com/a/b.html"),
            ("http://www.example.com/a.html#anything",""),
            ("http://www.example.com//a.html",""),
            ("https://www.example.com/a.html#anything",""),
            ("/wiki/Smiling_Sun","http://www.example.com/a/b.html")
            ]
        
        for url, baseURL in testCases:
            print("URL:", url, "BaseURL:", baseURL)
            print("Normalized URL:", self.canonizeURL(url, baseURL), "\n")
    
    def testGetURLs(self):
        url         = "http://en.wikipedia.org/wiki/Chernobyl_disaster"
        response    = requests.get(url)
        content     = response.text        
        urls        = self.getCanonizedURLs(content, url)
        
        title, text = self.parseTitleTextFromContent(content)
        
        print(title)
        print(text)