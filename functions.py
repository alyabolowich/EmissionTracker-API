#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 26 13:28:14 2022

@author: alyabolowich
"""

#%% Import packages 
import pymrio
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import psycopg2.extras
import requests
import re
import zipfile
import os
import sys 
from datetime import date

#%% Get path
def getPath():
    ''' Get the path name that this project is stored in. '''

    #path = os.path.join(os.path.dirname(__file__))
    path = os.getcwd()
    return path


#%% Get current DOI

def getCurrentDOI():
    ''' Get the current DOI, stored in the current_doi.txt file that accompaniies
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
    ''' Check that the most recent EXIOOBASE version is being used. '''

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
        return retrieved_DOI #this will need to move to the else statement later
        
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
    start_year = 1995
    
    # Create list of all years
    years = []
    for i in (range(current_year-start_year)):
        years.append(current_year - (i))
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
    
    # test if files exist (if/else works here)

    # Read in the files as dataframes
    dcba = pd.read_csv(getPath() + "/exiostorage" + "/IOT_{}_ixi/satellite/D_cba.txt".format(year), delimiter="\t", header=[0,1], index_col=0)
    dpba = pd.read_csv(getPath() + "/exiostorage" + "/IOT_{}_ixi/satellite/D_pba.txt".format(year), delimiter="\t", header=[0,1], index_col=0)
    #industries  = pd.read_csv(getPath() + "/exiostorage" + "/IOT_{}_ixi/industries.txt".format(year), delimiter="\t")
    #Z           = pd.read_csv(getPath() + "/exiostorage" + "/IOT_{}_ixi/Z.txt".format(year), delimiter="\t",  header=[0,1], index_col=0)
    
    # Get regions based on DCBA (assume is same for DPBA)
    regions = dcba.columns.levels[0].tolist()
    
    # Get industries based on DCBA (assume is same for DPBA)
    industries = dcba.columns.levels[1].tolist()
    
    return dcba, dpba, regions, industries # Z

#for year in years:
#s    readFiles(year)
#%% Format data

def formatData(dcba_df, dpba_df, year): #, dpba_df, industries_df):#, dpba_df, industries_df, Z_df): 
    '''The returned dataframe will be stacked to allow for one column of 
    values. This formatting will be used in PostgreSQL.
    
    Args:
        1) raw dcba dataframe
        2) raw dpba dataframe
        3) year of the matrix in question
        
        All 3 raw dataframes are the output of readFiles().
    
    The resulting format for the D_cba and D_pba matrices is as follows:
        stressor | region | sector | value | year

    The Z-matrix format is as follows:
        
        
    Returns DCBA df, DPBA df, and Z df
        
    '''
    
    # Remove negative values from dcba and set to 0
    num           = dcba_df._get_numeric_data()
    num[num < 0]  = 0
    
    # Repeat for dpba
    num           = dpba_df._get_numeric_data()
    num[num < 0]  = 0

    # Get matrix down to one vector of values (with indices)
    # D_cba
    
    dcba_raw = dcba_df.stack(level=[0,1]) 
    dcba_clean = dcba_raw.to_frame() 
    
    # D_pba
    dpba_raw = dpba_df.stack(level=[0,1]) 
    dpba_clean = dpba_raw.to_frame() 
    
    # Name the values column
    dcba_clean.columns = ["val"]
    dpba_clean.columns = ["val"]
    
    # Create a year column, populate with year
    dcba_clean['year'] = year
    dpba_clean['year'] = year
    
    # Drop "Number" column from industries df (superfluous data)
    #industries_clean = industries_df.drop(['Number'], axis=1)
    
    # Get Z matrix down to oone 
    
    return dcba_clean, dpba_clean #, industries_clean , Z

    sys.exit("Error in cleaning and processing dataframes in formatData().")
    
#%% Separate each region into a separate df

def separateDfByRegion(dcba_clean, dpba_clean, regions):
    ''' D_cba, D_pba are separated into regions and each regional dataframe
    is stored as a value in a dictionary, and the keys are the region abbreviations.
    These abbreviations follow the 2-letter country codes ISO 3166-1 country 
    codes  used in exiobase.
    
    Returns a dictionary.'''
    
    try:
        dcba_dict = {}
        dpba_dict = {}
        
        for region in regions:
            dcba_dict[region] = dcba_clean.xs(region, level=1, drop_level=False)
            dpba_dict[region] = dpba_clean.xs(region, level=1, drop_level=False)
        
        
        return dcba_dict, dpba_dict
    except:
        sys.exit("Error separating dataframe into regional dataframes./")
        
#%% Get regions - Block comment because this is hardcoded in the API.
# =============================================================================
# def getRegions():
#     ''' Regions extracted from the unit.txt file. '''
#         
#     exio_dir = getExioStorageDirectory()
#     
#     # Get most recent year to extract the unit.txt file which will
#     # be used to obtatin the regions
#     
#     years = getYears()
#     year = years[0]
#     
#     unit = pd.read_csv(exio_dir + "/IOT_{}_ixi/unit.txt".format(year), delimiter="\t")
#     regions = unit['region'].unique().tolist()
#     
#     return regions
# =============================================================================
        
#%% Get industries

#def getIndustries():
   # ''' The industries included in one version of EXIOBASE assumed to be the same
   # for all years, so this extracts the regions included from the latest year.'''
    
#%% Connect to Postgres

con = psycopg2.connect(database="bsp3", user="postgres", password="5555", host="localhost", port="5432")
cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)


def connectToPostgres():
    ''' Create coonnection to Postgres. '''
    engine = create_engine('postgresql://postgres:5555@localhost:5432/bsp3')
    #con = psycopg2.connect("dbname=bsp3 user=postgres password=****")
    return engine
    
    
#%% Upload to Postgres tables

def uploadDCBAToPostgres(dcba, engine):
    ''' Upload the DCBA matrix to Postgres. This uploads one dataframe at a time. 
    
    Required inputs:
        - '''
    #DCBA
    for key in dcba:
        print("Uploading {} data".format(key))
        dcba_to_upload = dcba[key]
        dcba_to_upload.to_sql('{}_dcba'.format(key), engine, if_exists='append')
        print("finished upload for year.")
    return

    #sectors
    #sectors.to_sql('sectors', engine)
    
    #CO2_econ.to_sql('co2', engine)
    
    #Z (economy) matrix all sectors
    #econ.to_sql('economy', engine)
    
    #vulnerable labor matrix
    #vl_econ.to_sql('vl', engine) 
#%% Remove file from exiostorage folder

def removeFilesFromExiostorage(year):
    # Remove folder
    os.rmdir(getPath() + "/exiostorage" + "/IOT_{}_ixi".format(year))
    # Remove zip file
    os.rmdir(getPath() + "/exiostorage" + "/IOT_{}_ixi.zip".format(year))

#%%
#%% Test


# =============================================================================
#  
#  if __name__ == '__main__':
#          
#      path = getPath()
#      years = getYears()
#      
#      for year in years:
#          print("Reading files")
#          #dcba_df, dpba_df = readFiles(year)
#          print("Formatting data")
#          #dcba_clean, dpba_clean = formatData(dcba_df, dpba_df, year)
#          print("Separating DF by region")
#          #dcba, dpba = separateDfByRegion(dcba_clean, dpba_clean)
#          print("Connecting to postgres")
#          engine = connectToPostgres()
#          print("Uploading tables to postgres")
#          #uploadDCBAToPostgres(dcba, engine)
#  
# =============================================================================
 
#%% Sample test
 
# table = "testdf"
# 
# engine = connectToPostgres()
# cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
# cur.execute("DROP TABLE IF EXISTS testdf")
# cur.execute("COMMIT")
# 
# #%%
# df = pd.DataFrame({'stressor': ['CO2', 'Land', 'Water'],
#                    'region': ['AT', 'BE', 'GR'],
#                    'sector': ['manu', 'trans', 'agri'],
#                    'value': [123.234, 345.456, 567.678],
#                    'year': [2000, 2000, 2000]},index=[0,1,2])
# 
# df2 = pd.DataFrame({'stressor': ['CO2', 'Land', 'Water'],
#                    'region': ['AT', 'BE', 'GR'],
#                    'sector': ['manu', 'trans', 'agri'],
#                    'value': [12.2, 34.4, 56.6],
#                    'year': [2001, 2001, 2001]},index=[0,1,2])
# 
# df3 = pd.DataFrame({'stressor': ['CO2', 'Land', 'Water'],
#                    'region': ['AT', 'BE', 'GR'],
#                    'sector': ['manu', 'trans', 'agri'],
#                    'value': [1234.5, 23445.6, 3456.7],
#                    'year': [2002, 2002, 2002]},index=[0,1,2])
# 
# df4 = pd.DataFrame({'stressor': 'CO2', 'region': 'ZA', 'sector': 'chem', 'value': 13, 'year': 2003},index=[0,1,2])
# #%%
# engine = connectToPostgres()
# df.to_sql('testdf', engine, if_exists='append')
# df2.to_sql('testdf', engine, if_exists='append') 
# df3.to_sql('testdf', engine, if_exists='append')
# 
# 
# #%% get row values of df
# 
#     
# #%% get values only from df
# 
# #cur.execute('''INSERT INTO testdf (stressor, region, sector, value, year)
# #    VALUES (%s, %s, %s, %s, %s) 
# #    ON CONFLICT (year) DO UPDATE ;''' % ())
# 
# #cur.execute("COMMIT")
# 
# #%% Where C = 19, update to 45
# #cur.execute('UPDATE testdf SET "value"= %s WHERE "year" = 2001;' % ()
# #cur.execute("COMMIT")
# # THIS WORKS ^^
# 
# #%%
# #%%% TEST append df4 to psql
# con = psycopg2.connect(database="bsp3", user="postgres", password="5555", host="localhost", port="5432")
# 
# def execute_values(conn, df, table):
#     """
#     Using psycopg2.extras.execute_values() to insert the dataframe
#     """
#     df = df.convert_dtypes()
#     # Create a list of tuples from the dataframe values
#     tuples = [tuple(x) for x in df.to_numpy()]
#     # Comma-separated dataframe columns
#     cols = ','.join(list(df.columns))
#     cols = ', '.join([f'"{word}"' for word in cols.split(',')])
#     # SQL query to execute
#     #query  = "INSERT INTO %s (%s) VALUES %%s" % (table, cols)
#     query = 'UPDATE %s SET "value" = %s WHERE "year" = 2002;' % (table, 42)
#     cursor = conn.cursor()
#     try:
#         psycopg2.extras.execute_values(cursor, query, tuples)
#         conn.commit()
#     except (Exception, psycopg2.DatabaseError) as error:
#         print("Error: %s" % error)
#         conn.rollback()
#         cursor.close()
#         return 1
#     print("execute_values() done")
#     cursor.close()
#     
# #%%
# 
# tuples = [tuple(x) for x in df4.to_numpy()]
# 
# tuples
# 
# cols = ','.join(list(df4.columns))
# cols = (', '.join([f'"{word}"' for word in cols.split(',')]))
# #cols = (cols)
# #%%
# #query  = "INSERT INTO %s (%s) VALUES %%s;" % (table, cols)
# 
# execute_values(con, df, table='testdf')
# execute_values(con, df2, table='testdf')
# execute_values(con, df3, table='testdf')
# =============================================================================
