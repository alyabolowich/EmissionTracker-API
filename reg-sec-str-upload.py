#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 23 23:27:28 2022

@author: alyabolowich
"""

import functions as f
import psycopg2
import psycopg2.extras

def getData():
    f.dataDownload(year=2022)
    f.unzipExioFiles(year=2022)
    
    dcba_df, dpba_df, regions = f.readFiles(year=2022)
    
    return dcba_df, dpba_df, regions

def uploadData(dcba_df, regions):
    sectors = dcba_df.columns.levels[1].tolist()
    stressors = list(dcba_df.index.values)
    
    con, cur = f.connection()
    
    # Insert regions
    cur.execute("""CREATE TABLE regions ("region" VARCHAR(3));""")
    query = """INSERT INTO regions VALUES (%param)"""
    cur.execute(query, regions)
    cur.execute("ROLLBACK")
    con.commit()
    
    return 

uploadData(dcba_df, regions)