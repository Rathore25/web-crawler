# -*- coding: utf-8 -*-
"""
Created on Mon Nov  2 20:28:27 2020

@author: nitish
"""

import json

class Helper():
    def __init__(self):
        self.settingsPath   = "./Settings/CrawlSettings.json"
        self.seedPath       = "./Seeds.txt"
        self.historyPath    = "./History/"
        self.settings       = None
    
    def getSettings(self):
        if self.settings is not None:
            return self.settings
        
        print(self.settingsPath)
        
        with open(self.settingsPath) as file:
            data = json.load(file)
        
        self.settings = data
        
        return self.settings
    
    def getSeeds(self):
        seeds = list()
        
        with open(self.seedPath) as file:
            for line in file:
                currLine = line.strip()
                if currLine != "":
                    items = currLine.split(',')
                    seedURL = str(items[0])
                    waveNum = int(items[1])
                    prevURL = ""
                    seeds.append((seedURL, waveNum, prevURL))
        
        return seeds