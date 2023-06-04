from urllib.request import urlopen as uReq
import urllib
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import datetime 
from contextlib import suppress
from sqlalchemy import create_engine
import pyodbc
import datetime as dt
import selenium
from selenium import webdriver
from time import sleep

from selenium.webdriver.common.by  import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver import ActionChains
from io import StringIO
#%% Header + Options
pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 100)

ymd_str=dt.date.today().strftime('%Y%m%d')
print(ymd_str)
#%%

#%%
PATH='D:\\DASH\\selenium\\chromedriver.exe'
driver=webdriver.Chrome(PATH)

url = "https://www.nwrfc.noaa.gov/station/flowplot/textPlot.cgi?id=MODO3&pe=HG"
url2 = "https://www.nwrfc.noaa.gov/station/flowplot/textPlot.cgi?id=TRBO3&pe=HG"


driver.get(url)
driver.maximize_window()
sleep(4)
page_csv=driver.find_element(By.XPATH, "/html/body").text

driver.get(url2)
driver.maximize_window()
sleep(4)
page_csv2=driver.find_element(By.XPATH, "/html/body").text


driver.quit() 
#%%


df = pd.DataFrame([x for x in page_csv.split('\n')])
df1=df.loc[df.index>4, :].copy()
df2=df1[0].str.split(expand=True).dropna()
df2.columns=['Date1', 'Time1', 'Stage1', 'Discharge1', 'Date2', 'Time2', 'Stage2', 'Discharge2' ]
df2['Date2']=pd.to_datetime(df2['Date2'])
df2['Discharge2']=df2['Discharge2'].astype(float)
print(df2.info())
#%%  Building Daily Forecast
#df2['Loc_ID']='MODO3'
df3=df2[['Date2','Discharge2']].groupby('Date2').mean().reset_index()
df3.columns=['Forecast_Date', 'MODO3']
print(df3)
#%%
d = pd.DataFrame([x for x in page_csv2.split('\n')])
print(d)
d1=d.loc[d.index>4, :].copy()
d2=d1[0].str.split(expand=True)
d2.columns=['Date1', 'Time1', 'Stage1', 'Discharge1', 'Date2', 'Time2', 'Stage2', 'Discharge2' ]
d2['Date2']=pd.to_datetime(d2['Date2'])
d2['Discharge2']=d2['Discharge2'].astype(float)
#%%
d3=d2[['Date2','Discharge2']].groupby('Date2').mean().reset_index()
d3.columns=['Forecast_Date', 'TRBO3']
print(d3)
#%%
df_all=pd.merge(df3, d3)
df_all['UpdateDate']=dt.datetime.now()
print(df_all)
#%%
data_in_path='D:\\DASH\Excel_in\\'
df_in=pd.read_excel(data_in_path+'Deschutes Inflow and Outflow Calcs.xlsx',sheet_name='Summary')
print(df_in.info())
#%%
df_in['ForecastMonthDate']=pd.to_datetime(dict(year=df_in.Year, month=df_in.month_nu, day=1))
print(df_in)
#%%
#Creates Dictionary for data

#Get some time stamps started
upDT = datetime.datetime.today().strftime('%Y-%m-%d')
today = datetime.date.today()
now = datetime.datetime.today()

#download lookup tables
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))




#%% Add_Fake Year for mm-dd

   

#print(cols)
try:
    df_all.to_csv(r'F:\Fundamentals\RFCSTP\PYSTP Scrape\NOAA_FCST'+upDT+'.csv',  index=False)
except:
    print('ERROR - cannot save .csv')
#%%
##upload to temp table
sql_in='''
SELECT TOP(12) [IssuanceDate]
      ,[RFCStationID]
      ,[ForecastMonthDate]
      ,[PercOfAvg]
  FROM [Fundamentals].[dbo].[HY_RFCESPWaterSupplyForecastMonthly]
  where RFCStationID =75 
  and WATerYear = 2023
    order by 1 DESC, 3
'''
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")
conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")
engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
df_in_HY_ratio=pd.read_sql(sql_in, conn)
#%%
df_merged=pd.merge(df_in, df_in_HY_ratio, how='left')

df_exp1=df_merged[df_merged['ForecastMonthDate']>=upDT].copy()
df_exp1['PercOfAvg']=df_exp1['PercOfAvg'].fillna(1)
df_exp1=df_exp1.reset_index(drop=True)
#print(df_exp1.head(20) )
df_exp1.to_sql('_HY_Ratios', engine, schema = 'dbo', index = False, if_exists='replace')

#%%
df_all.to_sql('NOAA_F', engine, schema = 'dbo', index = False, if_exists='replace')


import time

time.sleep(5)


conn.close()
#%%
