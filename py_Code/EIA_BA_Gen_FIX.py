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
from dateutil.relativedelta import relativedelta
from datetime import datetime
#%% INPUT
Template_date='2023-03-02'
Cut_after_date='2023-03-12'
Table_Source='EIA_BA_Gen'
Table_Delivery='EIA_BA_Gen'
Cut_after_dt=datetime.strptime(Cut_after_date, '%Y-%m-%d')
ymd_str=dt.date.today().strftime('%Y%m%d')
print(ymd_str)
#%%
#def Updader():
## Read Data with a good date
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
SELECT *
  FROM [Fundamentals].[dbo].[%s]
  where DT= '%s'
  order by 1 desc, 2
''' % (Table_Source,Template_date)
DF=pd.read_sql(SQL_READ, conn)
cursor = conn.cursor()
conn.close()
    
#Updader(Table_Source, Template_date): 
#%%
date_now = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(date_now)
df=DF
final_date=dt.date.today()-relativedelta(days=1)
#start_date=dt.date(2023,3,12)+relativedelta(days=1)
start_date=Cut_after_dt+relativedelta(days=1)
date_list= pd.date_range(start =start_date,  end =final_date, freq ='D')#).strftime('%Y%m%d').tolist()
print(date_list[0])
#%%
df_all=pd.DataFrame()

epoch=len(date_list)
#epoch=2
#LOOPING Algorithm
for j in range(epoch):
    df['DT']=date_list[j]
    df_all=pd.concat([df_all,df],axis=0,ignore_index=True)
df_all['Local_ID']=date_now    
print(df_all.tail())
#%%
SQL_Delete="delete  from %s where [DT]> '%s' " % (Table_Delivery, Cut_after_date)
print(SQL_Delete)
#%%

params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")
conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")
engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

connection = engine.connect()
connection.execute(SQL_Delete)
connection.close()

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



df_all.to_sql(Table_Delivery, engine, schema = 'dbo', index = False, if_exists='append')
