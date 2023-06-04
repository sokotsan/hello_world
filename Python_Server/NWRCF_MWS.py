from urllib.request import urlopen as uReq
import urllib
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import datetime 
from contextlib import suppress
from sqlalchemy import create_engine
from sqlalchemy.types import Float
from sqlalchemy.types import NVARCHAR
from sqlalchemy.types import DATETIME
from sqlalchemy.types import DATE
import pyodbc
from time import strptime
#http = urllib3.PoolManager()
LocNM = []
#Names of dams
LocNM = ['PRWW1'
         , 'LGSW1', 'TDAO3', 'IHDW1', 'LGDW1'
         , 'DWRI1', 'LMNW1', 'BONO3', 'GCDW1'
         , 'JDAO3', 'MCDW1', 'CHJW1', 'WANW1'
         , 'WELW1', 'LYDM8', 'UBDW1', 'SNQW1'
         ,'RODW1','MSRW1','MERW1','MYDW1'
         ,'SHAW1','MKNW1','MAYW1','SLTW1'
         ,'GORW1','COKW1','CHDW1','ALRW1'
         ,'NOXM8','FPOm8','CABI1','ALFW1'
         ,'KBDM8','SFCM8','LLKW1','BRNI1','KERM8','PALI1','AMFI1','SWAI1','PLNM8','ESTO3','MODO3']

#Creates Dictionary for data
dic = {
    "Loc" : [],
    "RFCStationID" : [],
    "ReportDate" : [],
    "UpDT" : [],
    "Month" : [],
    "DT" : [],
    "KAF_90" : [],
    "KAF_50" : [],
    "Perc_Avg" : [],
    "KAF_10" : [],
    "Avg_30Yr" : []}
upDT = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
upDTDT=datetime.datetime.today()
tdymnthnum = int(datetime.datetime.today().strftime('%m'))
fl = 0
page_html = bytearray()

#begin loop
for x in range (0,len(LocNM)):
    try:
        url = "https://www.nwrfc.noaa.gov/water_supply/monthly/monthly_forecasts.php?id="+LocNM[x]+"&datepick=&nextwy=1&csv_min_max=1"
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
    #page_html = r.content
    #soup = BeautifulSoup(page_html, 'html.parser')
    try:
        div = soup.find('div', {'id':'tabular_data_1'})
        tr = div.find_all('tr')
        activetag = tr[1].find_all('span')
        reportdate= strptime(activetag[1].get_text(),'%Y-%m-%d')
        for i in range (4,len(tr)-2):
            activetag = tr[i].find_all('td')
            dic["Loc"].append(LocNM[x]) 
            dic["RFCStationID"].append(None)
            dic["ReportDate"].append(datetime.datetime(*reportdate[:3]).strftime('%Y-%m-%d'))
            dic["UpDT"].append(upDT)
            mnthnum = strptime(activetag[0].get_text(),'%b').tm_mon
            if tdymnthnum > mnthnum:
                yrnum = strptime(upDT,'%Y-%m-%d %H:%M:%S').tm_year+1
            else:
                yrnum = strptime(upDT,'%Y-%m-%d %H:%M:%S').tm_year
            fcstmon = datetime.datetime(yrnum, mnthnum,1)
            dic["DT"].append(fcstmon.strftime('%Y-%m-%d %H:%M:%S'))
            dic["Month"].append(activetag[0].get_text())
            try: dic["KAF_90"].append(float(activetag[1].get_text()))
            except: dic["KAF_90"].append(None)
            try: dic["KAF_50"].append(float(activetag[2].get_text()))
            except: dic["KAF_50"].append(None)
            try: dic["KAF_10"].append(float(activetag[4].get_text()))
            except: dic["KAF_10"].append(None)
            try: dic["Perc_Avg"].append(float(activetag[3].get_text()))
            except: dic["Perc_Avg"].append(None)
            try: dic["Avg_30Yr"].append(float(activetag[6].get_text()))
            except: dic["Avg_30Yr"].append(None)
        print("success for "+LocNM[x])
    except:
        print("fail for "+LocNM[x])

   
NWRFC_WSF = pd.DataFrame(dic)

cols = NWRFC_WSF .columns.tolist()
col = cols[-1:] + cols[:-1]
NWRFC_WSF_df = NWRFC_WSF [col]

params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")
conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")
engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

NWRFC_WSF_df.to_sql('HY_RFCESPWaterSupplyForecastMonthly_Landing', engine, schema = 'dbo', index = False, dtype={'KAF_90':Float(),'KAF_50':Float(),'KAF_10':Float(),'Perc_Avg':Float(),'Avg_30Yr':Float(),'Loc':NVARCHAR(),'Mon':NVARCHAR(),'ReportDate':DATE(),'DT':DATE(),'UpDT':DATETIME()}, if_exists='replace')

sql = 'EXEC spImport_HY_RFCESPWaterSupplyForecastMonthly'


import time

time.sleep(5)

cursor = conn.cursor()
cursor.execute(sql)
conn.commit()
conn.close()

time.sleep(2)

                                  
#conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
  #                                       "SERVER=JWTCVPMEDB13;"
  #                                       "DATABASE=Fundamentals;"
  #                                       "trusted_connection=yes")


#cursor = conn.cursor()
#cursor.execute(sqll)
#conn.commit()
#conn.close()

#if fl == 1:
    #driver.quit()

#SaveName= '\\\JWTCVDMEDB03\\Fundamentals\\Gas Scrapes\\Hydrofcst.txt'
#np.savetxt(SaveName, NWRFC.values, fmt='%s',delimiter="\t")









