# -*- coding: utf-8 -*-
"""
Download zip files from PatentsView

https://www.patentsview.org/download/

Created on Mon Nov  9 22:38:39 2020

@author: Bowen Yan

"""
#import requests
from urllib.request import urlretrieve

main_url = "http://data.patentsview.org/20200630/download/"
zip_files = ["application.tsv.zip",
             "assignee.tsv.zip",
             "cpc_current.tsv.zip",
             "inventor.tsv.zip",
             "location.tsv.zip",
             "patent.tsv.zip",
             "patent_assignee.tsv.zip",
             "patent_inventor.tsv.zip",
             "rawinventor.tsv.zip",
             "rawlocation.tsv.zip",
             "uspatentcitation.tsv.zip"]

def download_zip(url, local_url):
    urlretrieve(url, local_url)
    
if __name__ == '__main__':
    for name in zip_files:
        download_zip(main_url + name, name)
        print (name + " downloaded.")