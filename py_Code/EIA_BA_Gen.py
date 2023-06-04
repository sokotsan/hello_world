# -*- coding: utf-8 -*-

from urllib.request import urlopen as uReq
import urllib
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import datetime as dt
from contextlib import suppress
import sqlalchemy
from sqlalchemy import create_engine
import pyodbc
import datetime as dt
from dateutil.relativedelta import relativedelta
#%%
pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 100)

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
SELECT top 100000 [DT]
      ,[HE]
      ,[MW]
      ,[BA_ID]
      ,[TYPE_ID]
      ,[Local_ID]
  FROM [Fundamentals].[dbo].[EIA_BA_Gen]
  order by 1 desc    
'''

SQL_READ1='''
SELECT [HE], [BA_ID], [TYPE_ID]   
	  , avg([MW]) as MW
  FROM [Fundamentals].[dbo].[EIA_BA_Gen]
  group by [HE], [BA_ID], [TYPE_ID] 
  order by 2,1,3      
'''

EIA_Gen=pd.read_sql(SQL_READ, conn)
EIA_AVG=pd.read_sql(SQL_READ1, conn)
import time

time.sleep(5)

cursor = conn.cursor()
conn.close()
#%%
#EIA_Gen.to_sql('EIA_BA_Gen2', engine, schema = 'dbo', index = False, if_exists='replace')
#%%
max_date=EIA_Gen['DT'].max()
freq_pull='D'

BAs=EIA_Gen['BA_ID'].unique()
Types=EIA_Gen['TYPE_ID'].unique()
#%
start_date=dt.date.today()
init_date=(max_date + relativedelta(days=1)).strftime('%m-%d-%Y')

final_date=(dt.date.today())
date_map=pd.DataFrame( pd.date_range(start =init_date,  end =final_date, freq =freq_pull), columns=['DT'])

#date_map['DATE']=date_map['Start']#.apply(lambda x: x.strftime('%Y-%m-%d'))
hourly=date_map.set_index('DT').resample('H').ffill().reset_index()
hourly['HE']=hourly['DT'].dt.hour+1
hourly_map=hourly[:-1]
#print(date_map)
#%%
hourly_map2=[]
for j in range(len(BAs)):
    hourly_map['BA_ID']=BAs[j]
    hourly_map2.extend(hourly_map.values)
DF_map=pd.DataFrame(hourly_map2, columns=hourly_map.columns)

hourly_map3=[]
for i in range(len(Types)):
    DF_map['TYPE_ID']=Types[i]
    hourly_map3.extend(DF_map.values)
DF_map2=pd.DataFrame(hourly_map3, columns=DF_map.columns)    
#%%
DF_map3=DF_map2.merge(EIA_AVG, how='inner', on=['HE', 'TYPE_ID', 'BA_ID' ]).sort_values(by=['DT'], ascending=False)
#%%
DF_map3.to_sql('EIA_BA_Gen', engine, schema = 'dbo', index = False, if_exists='append')
#%%
#print(EIA_AVG.head())
#DF.to_csv('C:\\Users\\e77231\\OneDrive - Portland General Electric Company\\hydro_\\Input MidC Inflows CSV'+ymd_str+'.csv', index=False)
#DF.to_csv('\\\\corp.dom\\ensysP\\Apps\\GenOps\\Imports\\Input MidC Inflows CSV.csv', index=False)
#print(EIA_AVG.head())
#DF.to_csv('C:\\Users\\e77231\\OneDrive - Portland General Electric Company\\hydro_\\Input MidC Inflows CSV'+ymd_str+'.csv', index=False)
#DF.to_csv('\\\\corp.dom\\ensysP\\Apps\\GenOps\\Imports\\Input MidC Inflows CSV.csv', index=False)
#print(EIA_AVG.head())
#DF.to_csv('C:\\Users\\e77231\\OneDrive - Portland General Electric Company\\hydro_\\Input MidC Inflows CSV'+ymd_str+'.csv', index=False)
#DF.to_csv('\\\\corp.dom\\ensysP\\Apps\\GenOps\\Imports\\Input MidC Inflows CSV.csv', index=False)
#print(EIA_AVG.head())
#DF.to_csv('C:\\Users\\e77231\\OneDrive - Portland General Electric Company\\hydro_\\Input MidC Inflows CSV'+ymd_str+'.csv', index=False)
#DF.to_csv('\\\\corp.dom\\ensysP\\Apps\\GenOps\\Imports\\Input MidC Inflows CSV.csv', index=False)
#print(EIA_AVG.head())
#DF.to_csv('C:\\Users\\e77231\\OneDrive - Portland General Electric Company\\hydro_\\Input MidC Inflows CSV'+ymd_str+'.csv', index=False)
#DF.to_csv('\\\\corp.dom\\ensysP\\Apps\\GenOps\\Imports\\Input MidC Inflows CSV.csv', index=False)
#print(EIA_AVG.head())
#DF.to_csv('C:\\Users\\e77231\\OneDrive - Portland General Electric Company\\hydro_\\Input MidC Inflows CSV'+ymd_str+'.csv', index=False)
#DF.to_csv('\\\\corp.dom\\ensysP\\Apps\\GenOps\\Imports\\Input MidC Inflows CSV.csv', index=False)
#print(EIA_AVG.head())
#DF.to_csv('C:\\Users\\e77231\\OneDrive - Portland General Electric Company\\hydro_\\Input MidC Inflows CSV'+ymd_str+'.csv', index=False)
#DF.to_csv('\\\\corp.dom\\ensysP\\Apps\\GenOps\\Imports\\Input MidC Inflows CSV.csv', index=False)
#print(EIA_AVG.head())
#DF.to_csv('C:\\Users\\e77231\\OneDrive - Portland General Electric Company\\hydro_\\Input MidC Inflows CSV'+ymd_str+'.csv', index=False)
#DF.to_csv('\\\\corp.dom\\ensysP\\Apps\\GenOps\\Imports\\Input MidC Inflows CSV.csv', index=False)
#print(EIA_AVG.head())
#DF.to_csv('C:\\Users\\e77231\\OneDrive - Portland General Electric Company\\hydro_\\Input MidC Inflows CSV'+ymd_str+'.csv', index=False)
#DF.to_csv('\\\\corp.dom\\ensysP\\Apps\\GenOps\\Imports\\Input MidC Inflows CSV.csv', index=False)
#print(EIA_AVG.head())
#DF.to_csv('C:\\Users\\e77231\\OneDrive - Portland General Electric Company\\hydro_\\Input MidC Inflows CSV'+ymd_str+'.csv', index=False)
#DF.to_csv('\\\\corp.dom\\ensysP\\Apps\\GenOps\\Imports\\Input MidC Inflows CSV.csv', index=False)
