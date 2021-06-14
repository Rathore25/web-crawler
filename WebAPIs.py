# -*- coding: utf-8 -*-
"""
Created on Mon Nov  2 21:10:20 2020

@author: nitish
"""
import requests
import logging
from URLHelper import URLHelper

class WebAPIs():
    def __init__(self):
        self.URLHelper  = URLHelper()
        self.timeout    = 10
    
    def getHeaders(self, url):
        result = (999,"")
        try:
            # first attempt
            response    = requests.head(url, timeout=self.timeout)
            statusCode  = response.status_code
            headers     = response.headers
            
            if str(response.status_code).startswith('3') and 'Location' in headers:
                # second attempt
                url         = str(headers['Location'])
                response    = requests.head(url, timeout=self.timeout)
                headers     = response.headers
                
                if str(response.status_code).startswith('3') and 'Location' in headers:
                    # 3rd attempt
                    url         = str(headers['Location'])
                    response    = requests.head(url, timeout=self.timeout)
                    headers     = response.headers
    
            result = (url, statusCode, headers)
        except:
            print("ERROR_IN_HEAD. URL:", url)
            logging.exception("ERROR_IN_HEAD. URL:" + url)
            result = (url, 999, "ERROR_IN_HEAD")
            
        return result
    
    def getRobotTxt(self, url):
        content = ""
        try:
            components  = self.URLHelper.getComponents(url)
            newURL      = components.scheme + "://" + components.netloc
            newURL      = newURL + "/robots.txt"
            response    = requests.get(newURL, timeout=self.timeout)
            
            if str(response.status_code).startswith('4'):
                return "ERROR_IN_GET. FILE_DOES_NOT_EXIST. Status code :" + str(response.status_code)
            
            content     = response.content
            content     = str(content, 'utf-8')
        except:
            print("ERROR_IN_GET. URL:", newURL)
            logging.exception("ERROR_IN_GET. URL:" + newURL)
            content = "ERROR_IN_GET"
        
        return content
    
    
    def getContent(self, url):
        content = ""
        try:
            response    = requests.get(url, timeout=self.timeout)
            content     = response.text
        except:
            #e = sys.exc_info()[0]
            print("ERROR_IN_GET. URL:", url)
            logging.exception("ERROR_IN_GET. URL:" + url)
            content = "ERROR_IN_GET"
        
        return content

#wb = WebAPIs()
#print("Final output:",wb.getHeaders('https://www.world-nuclear.org/information-library/safety-and-security/safety-of-plants/chernobyl-accident.aspx'))