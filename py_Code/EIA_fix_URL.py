import requests
import datetime
import numpy as np
import json
import pandas
import urllib
from sqlalchemy import create_engine
import pyodbc
from urllib.request import urlopen as uReq

sort=False


#BA = ['SWPP','PSCO','AVRN','NW','AVA','PACW','PACE','PSEI','SCL','PGE','TPWR','CHPD','TEPC','AZPS','DOPD'
# ,'BPAT','SRP','GRMA','DEAA','HGMA','GRIF','WALC','IID','CFE','LDWP'
# ,'CISO','TIDC','BANC','WACM','NEVP','IPCO','WAUW','NWMT','WWA','GWA','GCPD'
# ,'GRID','CAL','SW','EPE','PNM','AESO','BCHA']




#PNW =['AVRN','AVA','PACW','PACE','PSEI','SCL','PGE','TPWR','CHPD','TEPC','AZPS','DOPD' ,'BPAT','NWMT'
#,'SRP','GRMA','DEAA','HGMA','GRIF','WALC','IID','CFE','LDWP'
# ,'CISO','TIDC','BANC','WACM','NEVP','IPCO','WAUW','WWA','GWA','GCPD'
# ,'GRID','CAL','SW','EPE','PNM','PSCO','NW']

conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                     "SERVER=JWTCVPMEDB13;"
                                     "DATABASE=Fundamentals;"
                                     "trusted_connection=yes")

#DF_URL = pandas.read_sql_query('select * from dbo.EIA_BA_BA_List',conn)
DF_URL = pandas.read_sql_query('select * from dbo.EIA_BA_NG_List',conn)
print(DF_URL)
#%%
DF_URL['0']=DF_URL['0'].apply(lambda x: x.replace('http:','https:'))
print(DF_URL)
#%%

params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                            "SERVER=JWTCVPMEDB13;"
                                             "DATABASE=Fundamentals;"
                                             "trusted_connection=yes")

engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
#%%
#DF_URL.to_sql('EIA_BA_BA_List', engine, schema = 'dbo', index = False, if_exists='replace')
DF_URL.to_sql('EIA_BA_NG_List', engine, schema = 'dbo', index = False, if_exists='replace')
#%%
