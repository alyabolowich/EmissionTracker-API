#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 13 22:39:29 2022

@author: alyabolowich
"""

import psycopg2
import psycopg2.extras
from flask import request, jsonify, Flask, render_template
from markupsafe import escape
from flask_caching import Cache
import config
#import config

app = Flask(__name__)

app.config['DEBUG'] = True
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True


cache = Cache(config={'CACHE_TYPE': 'simple'}) 
cache.init_app(app)

#%% 
@app.errorhandler(Exception)
def response(status, data=None, message="OK"):
    return jsonify({"status": status, "result": data, "message": message})

#Error handler from pallets projects Flask documentation: https://flask.palletsprojects.com/en/1.1.x/patterns/errorpages/
@app.errorhandler(404)
def resource_404(e):
    return jsonify({"status": 404, "result": None, "message": "Not found. The URL is not valid, please verify the URL is correct."})

@app.errorhandler(500)
def resource_500(e):
    return jsonify({"status": 500, "result": None, "message": e})

#%% Singleton to create connection

class Connection:
    __instance = None
    
    def __init__(self):
        self.cur = self.get_con()  
        
    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = object.__new__(cls)
        return cls.__instance
            
    def get_con(self):
        con = psycopg2.connect(#database="bsp5", user="postgres", password="5555", host="localhost")
                               database = config.db_connection["database"], 
                               user     = config.db_connection["user"], 
                               password = config.db_connection["password"], 
                               host     = config.db_connection["host"])
        cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
        return cur

#%%
@app.route('/')
def index():
    return"My page"
#%%
@app.route('/v1', methods=['GET'])
def home():
    return render_template("index.html")

#%%
# Show all sectors
@app.route('/v1/sectors')
def allsectors():
    try:
        con = Connection()
        con.cur.execute('SELECT * from sectors;')
    except Exception as e:
        return resource_500(str(e))
    
    record = con.cur.fetchall()
    record = [dict(row) for row in record]
    return response(200, record)

#%%
# Show all regions
@app.route('/v1/regions')
def allregions():
    try:
        con = Connection()
        con.cur.execute('SELECT * from regions;')
    except Exception as e:
        return resource_500(str(e))

    record = con.cur.fetchall()
    record = [dict(row) for row in record]
    return response(200, record)

#%%
# Get DCBA data

@app.route('/v1/<lens>/<region>')
def dcba(lens, region): 

    if lens == "consumption":
        query = 'SELECT * FROM "{}_dcba" WHERE'.format(escape(region)) 
 
    elif lens == "production":
        query = 'SELECT * FROM "{}_dpba" WHERE'.format(escape(region)) 
    
    year     = request.args.get('year', type=int)
    stressor = request.args.get('stressor', "").lower() 
    sector   = request.args.get('sector', "").lower() 

    #query = 'SELECT * FROM "{}_dcba" WHERE'.format(escape(region)) 
    to_filter       = []
    
    region = region.lower()    
    if not region:
        return response(400, message="Bad request - Looks like you need to provide a stressor. Please check you have provided the correect two-letter code.")
     
    if year:
        query += ' year=%s AND'
        to_filter.append(year)
    if stressor:
        query += ' stressor=%s AND'
        to_filter.append(stressor)
    if sector:
        query += ' sector=%s AND'
        to_filter.append(sector)

          
    if not (year or stressor or sector):
        return response(400, message="Bad request - Please check that you have at least provided a year(s), sector(s), or stressor(s).")
    
    query = query[:-4] + 'LIMIT 10;'
    
    try:
        con = Connection()
        con.cur.execute(query, to_filter)
    except Exception as e:
        return resource_500(str(e))
    
        
    record = con.cur.fetchall() 
    record = [dict(row) for row in record]
    
    if not record:
        return response(400, message="Bad request - Please check that your  query is correctly entered.")
    
    return response(200, record)


#%%
# Get DPBA data
@app.route('/v1/production/<region>')
def dpba(region): 
        
    year     = request.args.get('year', type=int)
    stressor = request.args.get('stressor', "").lower() 
    sector   = request.args.get('sector', "").lower() 

    query = 'SELECT * FROM "{}_dpba" WHERE'.format(escape(region)) 
    to_filter       = []
    
    region = region.lower()    
    if not region:
        return response(400, message="Bad request - Looks like you need to provide a stressor. Please check you have provided the correect two-letter code.")
     
    if year:
        query += ' year=%s AND'
        to_filter.append(year)
    if stressor:
        query += ' stressor=%s AND'
        to_filter.append(stressor)
    if sector:
        query += ' sector=%s AND'
        to_filter.append(sector)

          
    if not (year or stressor or sector):
        return response(400, message="Bad request - Please check that you have at least provided a year(s), sector(s), or stressor(s).")
    
    query = query[:-4] + ';'
    
    try:
        con = Connection()
        con.cur.execute(query, to_filter)
    except Exception as e:
        return resource_500(str(e))
    
        
    record = con.cur.fetchall() 
    record = [dict(row) for row in record]
    
    if not record:
        return response(400, message="Bad request - Please check that your  query is correctly entered.") 
    
    return response(200, record)


#%% Run file
if __name__ == "__main__":
	app.run(debug=True)