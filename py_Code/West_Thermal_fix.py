# -*- coding: utf-8 -*-
"""
Created on Tue Jun 21 17:04:39 2022

@author: zpfundisql
"""

from urllib.request import urlopen as uReq
import urllib
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import datetime as dt
from contextlib import suppress
from sqlalchemy import create_engine
import pyodbc
#%%
ymd_str=dt.date.today().strftime('%Y%m%d')
print(ymd_str)
#%%
##upload to temp table
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")
conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")
engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

SQL_READ='''
select * from WEST_Thermal
'''
DF=pd.read_sql(SQL_READ, conn)


cursor = conn.cursor()
conn.close()
#%%
DF['IT'].loc[DF['Region']=='PNW']=DF['IT'].loc[DF['Region']=='PNW'].fillna(method='ffill')
DF['IT'].loc[DF['Region']=='MTW']=DF['IT'].loc[DF['Region']=='MTW'].fillna(method='ffill')
DF['Coal'].loc[DF['Region']=='MTW']=DF['Coal'].loc[DF['Region']=='MTW'].fillna(method='ffill')
#%%
conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                     "SERVER=JWTCVPMEDB13;"
                                     "DATABASE=Fundamentals;"
                                     "trusted_connection=yes")
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                            "SERVER=JWTCVPMEDB13;"
                                             "DATABASE=Fundamentals;"
                                             "trusted_connection=yes")

engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))



DF.to_sql('WEST_Thermal2', engine, schema = 'dbo', index = False, if_exists='replace')
