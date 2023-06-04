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


#Creates Dictionary for data
dic = {
    "ReportDate" : [],
    "ForecastDate" : [],
    "RFCCode" : [],
#    "RFCID" : [],
    "ParCode" : [],
#    "ParameterID" : [],
    "Description" : [],
    "Val" : [],
    "Average" : [],
    "Pct_Avg" : [],
    "ParameterValue" : [],
    "DateTimeStamp" : []
    }
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


##start the scrape
fl = 0
page_html = bytearray()

#begin loop to get data to soup
try:
    url = "https://www.nwrfc.noaa.gov/stp/stp_table.cgi"
    r =  requests.get(url , timeout=5)
    page_html = r.content
    soup = BeautifulSoup(page_html, 'html.parser')
except:
    print(url+' SSL Failed')
    page_html = bytearray()
    try:
        r =  requests.get(url, verify=False , timeout=10)

        page_html = r.content
        soup = BeautifulSoup(page_html, 'html.parser')
        print('Recover')
    except:
        print('ERROR - No Connection'+url)
page_html = r.content
soup = BeautifulSoup(page_html, 'html.parser')
#%%
print(soup)
#%%
#parse data within soup
tr = soup.find_all(class_='mono2')
headerlist = tr[0].contents[0].strip().split(',')
#%%
#have correct dates (and year) for the forecast date
fs=headerlist[6].strip()
fs = datetime.datetime.strptime(fs, '%m-%d')
if fs.month == today.month :
    fs = fs.replace(year=today.year)
elif fs.month == 12 :
    fs = fs.replace(year=today.year - 1)
else :
    fs = fs.replace(year=today.year)

for i in range (1,len(tr)):
    contlist = tr[i].contents[0].strip().split(',')

    for j in range(6,len(headerlist)):
        fcstdt = datetime.timedelta(j-6) + fs
        dic["ForecastDate"].append(fcstdt)
        dic["ReportDate"].append(fs)
        dic["DateTimeStamp"].append(now)
        dic["RFCCode"].append(contlist[0].strip())
        dic["Description"].append(contlist[1].strip())
        dic["ParCode"].append(contlist[2].strip())
        try:
            dic["Val"].append(int(contlist[3].strip()))
        except:
            dic["Val"].append(0)
        try:
            dic["Average"].append(int(contlist[4].strip()))
        except:
            dic["Average"].append(0)
        try:
            dic["Pct_Avg"].append(int(contlist[5].strip()))
        except:
            dic["Pct_Avg"].append(0)
        try:
            dic["ParameterValue"].append(float(contlist[j].strip()))
        except:
            dic["ParameterValue"].append(0)
       
        

#convert parsed data to dataframe to convert to csv
NWRFC = pd.DataFrame(dic)

#join the lookups
NWRFC=pd.merge(NWRFC,lkpparam,on="ParCode")
NWRFC=pd.merge(NWRFC,lkprfc,on="RFCCode")
##
cols = NWRFC.columns.tolist()
cols = ["ReportDate","ForecastDate","RFCCode","RFCID","ParCode","ParameterID","Description","Val","Average","Pct_Avg","ParameterValue","DateTimeStamp"]
#print(cols)
try:
    NWRFC.to_csv(r'F:\Fundamentals\RFCSTP\PYSTP Scrape\RiverForecastTrace'+upDT+'.csv', columns =cols, index=False)
except:
    print('ERROR - cannot save .csv')
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

NWRFC.to_sql('tmpHY_RFCSTP', engine, schema = 'dbo', index = False, if_exists='replace')


#execute stored proceedure
sql = 'EXEC spImport_HY_RFCSTP_v2'

import time

time.sleep(5)

cursor = conn.cursor()
cursor.execute(sql)
conn.commit()
conn.close()
