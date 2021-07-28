# -*- coding: utf-8 -*-
"""
Created on Tue Jul 27 22:18:01 2021

@author: nitis
"""
import boto3

class S3Manager():
    def __init__(self):
        self.s3 = boto3.resource(
            service_name='s3',
            region_name='us-east-2'
            )
        
    def upload_data(self, data):
        pass
    
    def get_data(self, key):
        response = self.s3.Bucket('ocm-ai-test-1').Object('OCM environment setup.txt').get()
        
        print(str(response['Body'].read().decode("utf-8")))

#s3M = S3Manager()
#s3M.get_data("Hello")