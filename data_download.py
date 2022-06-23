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

    #f.findMostRecentVersion()
    #f.createExioStorageDirectory()
    years = f.getYears()
    for year in years:
        
        print("Downloading new data for year {}".format(year))
        
        # Download data
        f.dataDownload(year)
        f.unzipExioFiles(year)
        print("Data downloaded, now processing.")
        
        # Read files
        dcba_df, dpba_df, regions = f.readFiles(year)
        print("Files read")
        
        # Format files
        dcba_dict = f.formatData(dcba_df, year)
        dpba_dict = f.formatData(dpba_df, year)
        print("Files cleaned and processed.")
        
        del dcba_df, dpba_df
        
        # Connect to Postgres
        con, cur = f.connection()
        
        # Variable to ensure that SQL tables are not deleted on each year
        db_deleted = True
    
        while db_deleted:
                
            # Need to first drop the existing tables (regardless of year)
            # to re-upload new ones
            f.dropTable(dcba_dict, con, cur, tblext="dcba")
            f.dropTable(dpba_dict, con, cur, tblext="dpba")
            
            # Change the counter to false
            db_deleted = False
        
        # Upload dcba
        f.uploadToPostgres(dcba_dict, con, cur, tblext="dcba")
        print("Uploaded all dcba for year {} to psql".format(year))
        
        del dcba_dict
        
        # Uplaod dpba
        f.uploadToPostgres(dpba_dict, con, cur, tblext="dpba")
        print("Uploaded dpba for year {} to psql".format(year))
        
        del dpba_dict
        
        # Delete the zipfile and folder from exiostorage
        f.removeFilesFromExiostorage(year)
        print("Files deleted from exiostorage directory.")
    return
    
main()