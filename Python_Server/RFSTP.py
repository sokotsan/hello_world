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

driver.get('https://www.nwrfc.noaa.gov/stp/stp_table.cgi')
driver.maximize_window()

driver.find_element(By.XPATH,"/html/body/form/input[4]").click()
sleep(2)
page_csv=driver.find_element(By.XPATH, "/html/body").text
driver.quit() 
#%%

data = page_csv
df = pd.DataFrame([x.split(',') for x in data.split('\n')])
df.columns=df.iloc[0]
df1=df.iloc[1:,:].copy()
#%%
dat_list=df1.columns[6:].tolist()
id_list=df1.columns[:6].tolist()
print(id_list)
#%%
cols0=["RFCCode", "Description", "ParCode",   "Val",  "Average" , "Pct_Avg", "ForecastDate", "ParameterValue"  ]
df2=pd.melt(df1,id_vars=id_list, value_vars=dat_list)
df2.columns=cols0
print(df2)
#%%

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


lkpparam = pd.read_sql_query("SELECT [ParameterID],[ParCode] FROM [Fundamentals].[dbo].[LkpHY_RFCSTPParameterCodes]", engine)

lkprfc = pd.read_sql_query("SELECT [RFCID],[RFCCode] FROM [Fundamentals].[dbo].[LkpHY_RFCSTPCodes]", engine)  



#%% Add_Fake Year for mm-dd
df2["ForecastDate"]=df2["ForecastDate"].apply(lambda x: datetime.datetime.strptime(x.strip()+'-1976', '%m-%d-%Y'))
#%

#%%
#Fix year (and year) for the forecast date
def year_replace(fs):
    if fs.month == today.month :
        fs = fs.replace(year=today.year)
    elif fs.month == 12 :
        fs = fs.replace(year=today.year - 1)
    else :
        fs = fs.replace(year=today.year)
    return fs
df2["ForecastDate"]=df2["ForecastDate"].apply(lambda y: year_replace(y))

#%% Strip spaces and force Monthly label to 0, to eliminate non-numeric data.
df2["ReportDate"]=df2["ForecastDate"].min()
df2["DateTimeStamp"]=now
df2["Description"]=df2["Description"].apply(lambda x: x.strip())
df2["ParCode"]=df2["ParCode"].apply(lambda x: x.strip())
df2["RFCCode"]=df2["RFCCode"].apply(lambda x: x.strip())
#%
df2["Val"]=pd.to_numeric(df2["Val"],errors='coerce').fillna(0).astype(int)
df2["Average"]=pd.to_numeric(df2["Average"],errors='coerce').fillna(0).astype(int)
df2["Pct_Avg"]=pd.to_numeric(df2["Pct_Avg"],errors='coerce').fillna(0).astype(int)
df2["ParameterValue"]=pd.to_numeric(df2["ParameterValue"],errors='coerce').fillna(0).astype(float)


#print(df2.info())

#%%
#join the lookups
#print(lkpparam)
df3=pd.merge(df2,lkpparam,on="ParCode")
NWRFC=pd.merge(df3,lkprfc,on="RFCCode")
# cols = NWRFC.columns.tolist()
# print(len(cols))
cols = ["ReportDate","ForecastDate","RFCCode","RFCID","ParCode","ParameterID","Description","Val","Average","Pct_Avg","ParameterValue","DateTimeStamp"]
#print(NWRFC[cols].head())

#%
#print(NWRFC[cols].info())
#%% Export to csv in case something breaks
      
        

#print(cols)
try:
    NWRFC[cols].to_csv(r'F:\Fundamentals\RFCSTP\PYSTP Scrape\RiverForecastTrace'+upDT+'.csv', columns =cols, index=False)
except:
    print('ERROR - cannot save .csv')
#%
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

NWRFC[cols].to_sql('tmpHY_RFCSTP', engine, schema = 'dbo', index = False, if_exists='replace')


#execute stored proceedure
sql = 'EXEC spImport_HY_RFCSTP_v2'

import time

time.sleep(5)

cursor = conn.cursor()
cursor.execute(sql)
conn.commit()
conn.close()
