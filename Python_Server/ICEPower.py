import os
import time
import datetime
import pandas as pd
import numpy as np
import win32com.client
import urllib
import pyodbc
from sqlalchemy import create_engine
from io import StringIO
import requests
from datetime import date, timedelta

filedest = "C:/Users/E09138/Desktop/Test Ice"
GasSavePath = '//JWTCVDMEDB03/Fundamentals/ICEGas/Stefan Fix/'
PowerSavePath = '//JWTCVDMEDB03/Fundamentals/ICEPower/Stefan Fix/'
ProdSavePath = '//corp.dom/PM1/G1/POWER OPERATIONS/fundies/ICE_Curves/'


iceurl = "https://downloads.theice.com/Settlement_Reports_CSV/Power/icecleared_power_"

username = "portlandgeneralelectric"
password = "settledata"

#pubdate = format(today(), "%m_%d_%Y")
#https://downloads.theice.com/Settlement_Reports_CSV/Power/icecleared_power_2019_08_15.dat
#link = 'https://downloads.theice.com/Settlement_Reports_CSV/Power/icecleared_Power_2019_08_15.dat'

#link = 'https://downloads.theice.com/Settlement_Reports_CSV/Power/icecleared_power_2019_08_14.dat'


pubdate = datetime.datetime.today().strftime('%Y_%m_%d')
#pubdate = datetime.date.today() - timedelta(days=1)
#pubdate = pubdate.strftime('%Y_%m_%d')
#.strftime('%Y_%m_%d')



url = iceurl+pubdate+'.dat'


result = requests.get(url, auth=(username, password)).content
s=str(result,'utf-8')
data = StringIO(s)
df = pd.read_csv(data,
                   sep="|")
#,
#                   skiprows = (1),
#                   header=None)
#cols = ['TRADE DATE','HUB','PRODUCT','STRIP','CONTRACT','CONTRACT TYPE','STRIKE','SETTLEMENT PRICE','NET CHANGE','EXPIRATION DATE','PRODUCT_ID']
#data.columns = cols

params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

df.to_sql('ICEPOWER_Landing', engine, schema = 'dbo', index = False, if_exists='replace')
time.sleep(25)

sql = 'EXEC dbo.IcePower_Import'

cursor = conn.cursor()
cursor.execute(sql)
conn.commit()
time.sleep(5)
conn.close()

