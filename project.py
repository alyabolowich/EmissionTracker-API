
import pymrio
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import psycopg2
import requests
from bs4 import BeautifulSoup
import json
import re 

#%%
# Access Zenodo API

url = "https://doi.org/10.5281/zenodo.3583070"
r = requests.get(url)

# First check status code
sc = r.status_code

    # If status code == 200, continue, else wait for Zenodo to get back online
head = r.headers

print(sc, head)


#%%

# r.text allows you to get the HTML code and then the URL links in the <script> tag
soup = BeautifulSoup(r.text, 'html.parser')
script = soup.find_all('script')

# Eight script tags return, we want the first one
script = script[0]
#print(script)

#%%
# Remove the script tag to get the JSON text. This will first become a string
# And then using json.loads, we get a dict class, and with json.dumps we get 
# A prettified JSON object (class 'str')
for word in script:
    json_data = word

json_obj = json.loads(word)

obj = json.dumps(json_obj, indent=4)
print(obj)
print(type(obj))


#%%
soup = BeautifulSoup(r.content, 'html.parser')

data = json.loads(soup.find('script', type='application/ld+json').text)


print(data["@type"])

#%%
# Load exiobase, concordance matrix, and get metadata
exio3_folder = "/Users/alyabolowich/Desktop/exiostorage"
exio_meta = pymrio.download_exiobase3(
    storage_folder=exio3_folder, system="ixi", years=[2018,2019]
)
#%%
#exio3 = pymrio.parse_exiobase3(path='/Volumes/SD/bsp3/IOT_2018_ixi.zip')
con_mat = pd.read_excel('/Volumes/SD/bsp3/concord_matrix_bjelle_2020.xlsx').iloc[:,4:15]
engine = create_engine('postgresql://postgres:5555@localhost:5432/bsp3')
con = psycopg2.connect("dbname=bsp3 user=postgres password=****")
exio3.meta

#%%
# Get sector and region information 
sectors = exio3.get_sectors()
regions = exio3.get_regions()

#%%
# Calculate all matrices and fetch interindustry flows (Z) and stressors (S)
exio3.calc_all()


#%%
# Get list of EU countries, aggregate the non-EU countries to create new list of regions to study (EU and 1 ROW)
EUcountry       = exio3.get_regions()[:28].tolist()
reg_agg_list    = EUcountry + (49-28)*['ROW']

#%%
# Aggregate sectors. 
# Sector aggregation matrix based on ISIC sectors (UN Stats) and Bjelle et al (2020)
sector_names    = ['Agriculture', 'Mining', 'Manufacturing', 'Electricity', 'Water_and_waste', 'Construction', 'Retail', 'Transport', 'Accomodation_and_food_services', 'Finance_and_real_estate', 'Public_services']
sec_agg_matrix  = con_mat.values.T
#Modify the exio3 data to aggregate into the regions. Followed example from pymrio documentation.
ag              = exio3.aggregate(region_agg=reg_agg_list, 
                              sector_agg=sec_agg_matrix,
                              sector_names= sector_names,
                              inplace=False)

#%%

# After aggregation, matrices need to be reset and recalculated (per pymrio documentation)
ag.reset_all_full().calc_all()
#Show relevant matrices after aggregation reset
Z_sec  = ag.Z
S = ag.satellite.S
# Turn all negs to 0s
num             = Z_sec._get_numeric_data()
num[num < 0]    = 0

#%%
# Diagonalize CO2 emissions
CO2_ag          = ag.satellite.S.loc['CO2 - combustion - air']
CO2_ag_diag     = pd.DataFrame(np.diag(CO2_ag),
                           index=CO2_ag.index,
                           columns=CO2_ag.index)


# Diagonalize vulnerable labor (in persons)
vl         = ag.satellite.S.loc['Employment: Vulnerable employment']
vl_ag_diag     = pd.DataFrame(np.diag(vl),
                           index=vl.index,
                           columns=vl.index)
### ========
# Matrix needed for CO2
CO2_econ = CO2_ag_diag.dot(ag.Z)


# Matrix needed for vulnerable labor
vl_econ = vl_ag_diag.dot(ag.Z)
#%%

#regions dataframe
regions = pd.DataFrame(ag.get_regions()).applymap(str.lower)
#sectors dataframe
sectors = pd.DataFrame(ag.get_sectors()).applymap(str.lower)


#%% Get all sector emissions
CO2_econ = CO2_econ.stack(level=[0,1])
CO2_econ = CO2_econ.to_frame()
CO2_econ.columns = ['val']
CO2_econ.index.names = ['region_from', 'sector_from', 'region_to', 'sector_to']


econ = Z_sec.stack(level=[0,1])
econ = econ.to_frame()
econ.columns = ['val']
econ.index.names = ['region_from', 'sector_from', 'region_to', 'sector_to']

vl_econ = vl_econ.stack(level=[0,1])
vl_econ = vl_econ.to_frame()
vl_econ.columns = ['val']
vl_econ.index.names = ['region_from', 'sector_from', 'region_to', 'sector_to']


#add unit column
CO2_econ['unit']='kg co2'
econ['unit']    ='mio euro'
vl_econ['unit'] ='persons'

#add year column
CO2_econ['year'] = 2019
econ['unit']     = 2019
vl_econ['year']  = 2019

#%% 

### Create tables in Postgres
#regions
regions.to_sql('regions', engine)

#sectors
sectors.to_sql('sectors', engine)

#co2 all sectors
CO2_econ.to_sql('co2', engine)

#Z (economy) matrix all sectors
econ.to_sql('economy', engine)

#vulnerable labor matrix
vl_econ.to_sql('vl', engine) 