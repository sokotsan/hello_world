# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

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

ymd_str=dt.date.today().strftime('%Y-%m-%d %H:%M:%S')
print(ymd_str)

date_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(date_time)
#%% Names
# path='D:\\DASH\\CSV_in\\'
# file1='Hy_Ratios.csv'
table_name='_DA_MIDC'
#%%

DF2=pd.read_excel(path+'Hy_pgeshare2023.xlsx', sheet_name='shares')
DF2['UpdateDate']=dt.datetime.now()
DF1=pd.read_csv(path+file1, parse_dates=['UpdateDate'])
print(DF2.info())
#%%
DF_all=pd.concat([DF1,DF2])
print(DF_all.info())

#%% UNCOMMENT

#DF2.to_sql('Hy_Midc_PGEShare', engine, schema = 'dbo', index = False, if_exists='append')
#%% END Of the file. 
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

connection = engine.connect()
connection.execute("DROP TABLE " +  table_name)
connection.execute("""CREATE TABLE """ + table_name + """ (
    [LocationID] [smallint] NOT NULL,
	[ShareMonth] [smallint] NULL,
	[ShareYear] [smallint] NULL,
	[pgeshare] [float] NULL,
	[UpdateDate] [datetime] NULL
                    )""")


connection.close()



#%%



#%%
D
#%% sqL that reads table

# SQL_READ='''
# SELECT *
#   FROM [Fundamentals].[dbo].[Hy_Midc_PGEShare]
#   order by 1 desc    
# '''

# HY_Ratios=pd.read_sql(SQL_READ, conn)

# import time

# time.sleep(5)

# cursor = conn.cursor()
# conn.close()
#%%
# print(HY_Ratios)
# HY_Ratios.to_csv(path+file1, index=False)


#%% Export Ratios to backup

