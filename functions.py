#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 26 13:28:14 2022

@author: alyabolowich
"""

#%% Import packages
import pymrio
import pandas as pd
import psycopg2
import psycopg2.extras
import requests
import re
import zipfile
import os
import sys
from datetime import date
import config
import shutil

#%% Get path
def getPath():
    ''' Get the path name that this project is stored in. '''

    #path = os.path.join(os.path.dirname(__file__))
    path = os.getcwd()
    return path


#%% Get current DOI

def getCurrentDOI():
    ''' Get the current DOI, stored in the current_doi.txt file that accompanies
    this project. Make a request or the url to see whether the latest DOI is 
    different from the previous version.
    
    DOI 3583070 represents all versions of EXIOBASE on Zenodo.'''

    # test for existence/absence of current_doi
    with open(getPath() + "/current_doi.txt") as f:
        if True:
            print("Reading current DOI from currentDOI.txt.")
            return f.read()
        else: 
            print("Error retrieving DOI from current_doi.txt file.")


#%% Update current DOI

def updateCurrentDOI(retrieved_DOI):
    ''' If a new version of exiobase exists, this function will update the 
    current_doi.txt file with the new value.

    Input arguments:

        rerieved_DOI : value returned from getCurrentDOI '''

    with open(getPath() + "/current_doi.txt", "w") as f:
        if True:
            f.write(str(retrieved_DOI))
            print("New version found. Writing new DOI to currentDOI.txt.")
        else:
            print("Error updating DOI in the current_DOI.txt file.")

#%% Find most recent version of Exiobase

def findMostRecentVersion():
    ''' Check that the most recent EXIOBASE version is being used. '''

    current_DOI = getCurrentDOI()

    # DOI 3583070 gives access to the most recent EXOIBASE version 
    # by default, but each version has its own DOI.
    main_url = "https://doi.org/10.5281/zenodo.3583070"

    # Establish connection
    req = requests.get(main_url)

    # This will yield the url that holds the most recent Zenodo version
    retrieved_url = req.url

    # Extract only the DOI integer from the URL
    retrieved_DOI = re.findall("\d+", retrieved_url)
    retrieved_DOI = retrieved_DOI[0]

    # Perform action based on whether the retrieved_URL matches the current_URL.
    if current_DOI == retrieved_DOI:
        print("Version is the same, no need to update API.")
        #exit() # NEED TO UNCOMMENT
        #return retrieved_DOI #this will need to move to the else statement later

    elif current_DOI != retrieved_DOI:
        print(type(current_DOI), type(retrieved_DOI))
        updateCurrentDOI(retrieved_DOI)
        print("DOIs did not match. New DOI updated in current_DOI.txt file.")
        print("Running fundMostRecentVersion() again to get latest version")
        findMostRecentVersion()

    else:
        print("Error finding most recent version of EXIOBASE.")


#%% Create a storage directory for EXIOBASE files that will be updated.

def createExioStorageDirectory():
    ''' Create a file automatically for the exiobase storage.  '''

    # Create a path to the directory which will be named "exiostorage"
    new_directory = "exiostorage"
    parent_dir = getPath()
    exio_folder_path = os.path.join(parent_dir, new_directory)

    # If the directory does not already exist, create it, else skip.
    if not os.path.exists(exio_folder_path):
        os.mkdir(exio_folder_path)
        return exio_folder_path
    elif os.path.exists(exio_folder_path):
        print("Path to exiostorage folder already exists.")
        return exio_folder_path

    else:
        print("Error creating exiobase storage directory.")
        sys.exit("Cannot create EXIOBASE storage folder, and one does not exist.")

#%% Get EXIO storage directory

def getExioStorageDirectory():

    new_directory = "exiostorage"
    parent_dir = getPath()
    exio_folder_path = os.path.join(parent_dir, new_directory)

    return exio_folder_path

#%% Unzip file

def unzipExioFiles(year):
    ''' Files downloaded from Zenodo will be zipped. To use, these
    must be uziipped first.'''
    
    # Get path to exiostorage folder
    directory = getExioStorageDirectory()

    # Read the files in the directory based on the years targeted

    myfile = "IOT_{}_ixi.zip".format(year)

    file_to_unzip = os.path.join(directory, myfile)
    print(file_to_unzip)

    if zipfile.is_zipfile(file_to_unzip) is True:
        print("ZIP file is valid.")

    else:
        print("Zip file is invalid. Cannot unzip.")
        sys.exit("Unable to unzip Exiobase files in the exiostorage directory.")

    with zipfile.ZipFile(file_to_unzip, 'r') as zip_ref:
        if True:
            print("Unzipping files")
            zip_ref.extractall(directory)
        else:
            sys.exit("Error unzipping EXIOBASE files.")


#%% Get years we want to update

def getYears():
    ''' Each time the API is updated, automatically update the last 3 years to account
    for any prior changes. This function will return a list of the last three years. ''' 
    # Get current year
    current_year = date.today().year

    # Create list of the last two years

    years = []
    for i in range(2):
        years.append(current_year- i)
    return years

#%% Download from Zenodo

def dataDownload(year):

    exio_storage = getExioStorageDirectory()
    
    exio_meta = pymrio.download_exiobase3(
        storage_folder = exio_storage,
        system = "ixi",
        years = year
    )
    print("Download of year {} complete.".format(year))

    print("Downloaded successfully", exio_meta)

#%% Read csv files as dataframes

def readFiles(year):
    ''' Read the files from the exiostorage directory. Store as dataframes with 
    one vector of values. 
    
    Matrices extracted and returned as dataframes:
        D_cba, D_pba, industries, Z'''

    # Read in the files as dataframes
    dcba = pd.read_csv(getPath() + "/exiostorage" +
                       "/IOT_{}_ixi/satellite/D_cba.txt".format(year),
                       delimiter="\t",
                       header=[0,1],
                       index_col=0)
    dpba = pd.read_csv(getPath() + "/exiostorage" +
                       "/IOT_{}_ixi/satellite/D_pba.txt".format(year),
                       delimiter="\t",
                       header=[0,1],
                       index_col=0)

    # Get regions based on DCBA (assume is same for DPBA)
    regions = dcba.columns.levels[0].tolist()

    return dcba, dpba, regions

#%% Format data

def formatData(df, year):
    '''The returned dataframe will be stacked to allow for one column of 
    values. This formatting will be used in PostgreSQL.
    
    Args:
        1) dataframe from the ouput of readFiles()
        2) year - required to append to "year" column
    
    The resulting format for the dataframe (D_cba/D_pba matrices) is as follows:
        stressor | region | sector | value | year
                
    Returns a dictionary.'''

    num_sectors = 163
    num_stressors = 1113

    # Replace all values from 
    stressors_ordered = df.index.str.replace(' ','_').str.lower()
    sectors_ordered   = df.columns.to_frame()['sector'].str.replace(' ','_').str.lower().unique()
    regions_ordered   = df.columns.to_frame()['region'].str.lower().unique()
    
    data = df.values.ravel()
    df_dict = dict()

    # Need to make columns for the dataframe that will be stored in the dictionary
    for r,region in enumerate(regions_ordered):
        
        index_stack = pd.MultiIndex.from_product([stressors_ordered,
                                                  sectors_ordered,
                                                  [region],
                                                  [year]],
                                                 names=['stressor','sector','region','year'])

        df_clean = pd.DataFrame(data[r*num_sectors*num_stressors:(r+1)*num_sectors*num_stressors],
                                index=index_stack,
                                columns=['value'])

        df_clean.reset_index(inplace=True)
        
        df_dict[region] = df_clean

    return df_dict

#%% Separate each region into a separate df

def separateDfByRegion(df):
    ''' D_cba, D_pba are separated into regions and each regional dataframe
    is stored as a value in a dictionary, and the keys are the region abbreviations.
    These abbreviations follow the 2-letter country codes ISO 3166-1 country 
    codes  used in exiobase.

    Args:
        - df is the dcba_clean or d_pba clean.

    Returns a dictionary.'''

    df_dict = dict(list(df.groupby('region')))

    return df_dict

#%%

def connection():
    con = psycopg2.connect(database = config.db_connection["database"],
                           user     = config.db_connection["user"],
                           password = config.db_connection["password"],
                           host     = config.db_connection["host"],
                           port     = config.db_connection["port"])

    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    return con, cur

#%% Upload to Postgres tables

def uploadToPostgres(dictionary, con, cur, tblext):
    ''' Upload the dataframes stored in the dictionary to Postgres. This function
    should only be used when initially uploading the data to Postgres. For continual
    updates, use updateValuesInDatabase().

    Required inputs:
        - Dcba or dpba dictionary
        - Postgres connection, con, and cursor, cur, from connection()
        - Table extension (either 'dcba' or 'dpba'')

    Adapted from: https://naysan.ca/2020/05/09/pandas-to-postgresql-using-psycopg2-bulk-insert-performance-benchmark/. '''

    # Create table
    # Make list of region names to be used in the names of the tables
    names = list(dictionary.keys())

    for name in names:
        name.lower()

        # Make table name
        table = "{}_".format(name) + tblext

# =============================================================================
#         # Drop if exists
#         cur.execute("DROP TABLE IF EXISTS {}".format(table))
#         cur.execute("COMMIT")
# 
#         # Create table
#         cur.execute("""CREATE TABLE {} ("stressor" VARCHAR(255),
#                                         "sector" VARCHAR(255),
#                                         "region" VARCHAR(3),
#                                         "value" DOUBLE PRECISION,
#                                         "year" SMALLINT);""".format(table))
#         cur.execute("ROLLBACK")
# =============================================================================

        # Create a list of tuples from the dataframe values
        df_to_upload = dictionary[name]
        tuples = [tuple(x) for x in df_to_upload.to_numpy()]

        # Comma-separated dataframe columns
        cols = ','.join(list(df_to_upload.columns))
        cols = ', '.join([f'"{word}"' for word in cols.split(',')])

        query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)

        psycopg2.extras.execute_values(cur, query, tuples)
        con.commit()

        print("Uploaded {} to Postgres".format(name))

#%% Drop old table
def dropTable(dictionary, con, cur, tblext):
    
    names = list(dictionary.keys())

    for name in names:
        name.lower()

        # Make table name
        table = "{}_".format(name) + tblext

        # Drop if exists
        cur.execute("DROP TABLE IF EXISTS {}".format(table))
        print("Dropped table {}".format(table))
        cur.execute("COMMIT")

        # Create table
        cur.execute("""CREATE TABLE {} ("stressor" VARCHAR(255),
                                        "sector" VARCHAR(255),
                                        "region" VARCHAR(3),
                                        "value" DOUBLE PRECISION,
                                        "year" SMALLINT);""".format(table))
        cur.execute("ROLLBACK")
#%% Update values in DB
def updateValuesInDatabase(cur, con, df, table):
    ''' Update values in each Postgres table.

    Inputs:
        - dataframe (df) that will be updated (from dcba_dict or dpba_dict)
        - table name (e.g. AT_dcba, BG_dpba)

    '''
    # Create a tuple of the values that need to be updated. In this case, the 
    # values need to be updated according to the year. How the sectors and 
    # stressors are arranged in the df is assumed to remain the same.
    rows = zip(df.value, df.year)

    # Create a temporary table to store the data that will be uploaded
    cur.execute("""CREATE TEMP TABLE testtemp (value INTEGER, year INTEGER) ON COMMIT DROP""")
    cur.executemany("""INSERT INTO testtemp (value, year) VALUES(%s, %s)""", rows)

    cur.execute("""
        UPDATE {}
        SET value = testtemp.value
        FROM testtemp
        WHERE testtemp.year = {}.year;
        """.format(table, table))

    cur.rowcount
    con.commit()
    cur.close()
    con.close()

#%% Remove file from exiostorage folder

def removeFilesFromExiostorage(year):
    # Remove folder
    shutil.rmtree(getPath() + "/exiostorage" + "/IOT_{}_ixi".format(year))
    # Remove zip file
    os.remove(getPath() + "/exiostorage" + "/IOT_{}_ixi.zip".format(year))


