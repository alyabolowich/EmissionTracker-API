#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 26 13:34:41 2022

@author: alyabolowich
"""

import functions as f

#%%
def main():
    ''' Program will find the most recent version of EXIOBASE. If a new
    version exists, it will create a new folder called 'exiostorage' and proceed
    to download the IOT_year_ixi.zip files from Zenodo to this folder. A function
    will then unzip these files to be used by the next program (data_processing_and_upload)
    '''
    #years = getYears()
            
    #f.findMostRecentVersion()
    #f.createExioStorageDirectory()
    #years = f.getYears()
    years = [2022]

    for year in years:
        
        #f.dataDownload(year)
        #f.unzipExioFiles(year)
        dcba_df, dpba_df, regions   = f.readFiles(year)
        dcba_clean, dpba_clean      = f.formatData(dcba_df, dpba_df, year)
        dcba_dict, dpba_dict        = f.separateDfByRegion(dcba_clean, dpba_clean, regions)
    
        # Upload to PSQL
        engine = f.connectToPostgres()
        uploadDCBAToPostgres(dcba, engine)
        
        # Delete the file and folder from exiostorage
        #f.removeFilesFromExiostorage(year)
        
    return dcba_clean, dpba_clean, dcba_dict, dpba_dict
    
    
dcba_clean, dpba_clean, dcba_dict, dpba_dict = main()