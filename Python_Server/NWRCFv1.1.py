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
         ,'KBDM8','SFCM8','LLKW1','BRNI1','KERM8','PALI1','AMFI1','SWAI1','PLNM8']
PoolNM = ['GCDW1','LYDM8','DWRI1']

#Creates Dictionary for data
dic = {
    "Loc" : [],
    "UpDT" : [],
    "Type" : [],
    "Measure" : [],
    "DT" : [],
    "Units" : [],
    "Value" : []}
upDT = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
fl = 0
page_html = bytearray()

#begin loop
for x in range (0,len(LocNM)):
    try:
        url = "https://www.nwrfc.noaa.gov/station/flowplot/textPlot.cgi?id="+LocNM[x]+"&pe=QI"
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
    
    tr = soup.find_all('tr')
    for i in range (2,len(tr)):
        activetag = tr[i].find_all('td')
        if activetag[2].get_text().lstrip():
            dic["Loc"].append(LocNM[x])
            dic["UpDT"].append(upDT)
            dic["Type"].append("FCST")
            dic["Measure"].append("Inflows")
            dic["DT"].append(activetag[2].get_text().lstrip())
            dic["Units"].append("CFS")
            dic["Value"].append(activetag[3].get_text())

        if activetag[0].get_text().lstrip():
            dic["Loc"].append(LocNM[x])
            dic["UpDT"].append(upDT)
            dic["Type"].append("ACT")
            dic["Measure"].append("Inflows")
            dic["DT"].append(activetag[0].get_text().lstrip())
            dic["Units"].append("CFS")
            dic["Value"].append(activetag[1].get_text())
    print("Inflows "+LocNM[x])





    
for x in range (0,len(LocNM)):
    try:
        url = "https://www.nwrfc.noaa.gov/station/flowplot/textPlot.cgi?id="+LocNM[x]+"&pe=QR"
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
    tr = soup.find_all('tr')
    for i in range (2,len(tr)):
        activetag = tr[i].find_all('td')
        if activetag[0].get_text().lstrip():
            dic["Loc"].append(LocNM[x])
            dic["UpDT"].append(upDT)
            dic["Type"].append("ACT")
            dic["Measure"].append("Outflow")
            dic["DT"].append(activetag[0].get_text().lstrip())
            dic["Units"].append("CFS")
            dic["Value"].append(activetag[1].get_text())

        if activetag[2].get_text().lstrip():
            dic["Loc"].append(LocNM[x])
            dic["UpDT"].append(upDT)
            dic["Type"].append("FCST")
            dic["Measure"].append("Outflow")
            dic["DT"].append(activetag[2].get_text().lstrip())
            dic["Units"].append("CFS")
            dic["Value"].append(activetag[3].get_text())

for x in range (0,len(PoolNM)):
    try:
        url = "https://www.nwrfc.noaa.gov/station/flowplot/textPlot.cgi?id="+PoolNM[x]+"&pe=HF"
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
    tr = soup.find_all('tr')
    for i in range (2,len(tr)):
        activetag = tr[i].find_all('td')
        if activetag[2].get_text().lstrip():
            dic["Loc"].append(PoolNM[x])
            dic["UpDT"].append(upDT)
            dic["Type"].append("FCST")
            dic["Measure"].append("PoolHeight")
            dic["DT"].append(activetag[2].get_text().lstrip())
            dic["Units"].append("Feet")
            dic["Value"].append(activetag[3].get_text())

        if activetag[0].get_text().lstrip():
            dic["Loc"].append(PoolNM[x])
            dic["UpDT"].append(upDT)
            dic["Type"].append("ACT")
            dic["Measure"].append("PoolHeight")
            dic["DT"].append(activetag[0].get_text().lstrip())
            dic["Units"].append("Feet")
            dic["Value"].append(activetag[1].get_text())
    print("Outflows "+LocNM[x])

for x in range (0,len(LocNM)):
    try:
        url = "https://www.nwrfc.noaa.gov/station/flowplot/textPlot.cgi?id="+LocNM[x]+"&pe=HG"
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
    tr = soup.find_all('tr')
    for i in range (2,len(tr)):
        activetag = tr[i].find_all('td')
        #if activetag[0].get_text().lstrip():
        #    dic["Loc"].append(LocNM[x])
        #    dic["UpDT"].append(upDT)
        #    dic["Type"].append("ACT")
        #    dic["Measure"].append("Stage")
        #    dic["DT"].append(activetag[0].get_text().lstrip())
        #    dic["Units"].append("Feet")
        #    dic["Value"].append(activetag[1].get_text())

        if activetag[1].get_text().lstrip():
            dic["Loc"].append(LocNM[x])
            dic["UpDT"].append(upDT)
            dic["Type"].append("ACT")
            dic["Measure"].append("Outflow")
            dic["DT"].append(activetag[0].get_text().lstrip())
            dic["Units"].append("CFS")
            dic["Value"].append(activetag[2].get_text())


            

        #if activetag[3].get_text().lstrip():
        #    dic["Loc"].append(LocNM[x])
        #    dic["UpDT"].append(upDT)
        #    dic["Type"].append("FCST")
        #    dic["Measure"].append("Stage")
        #    dic["DT"].append(activetag[3].get_text().lstrip())
        #    dic["Units"].append("CFS")
        #    dic["Value"].append(activetag[4].get_text())
        if activetag[3].get_text().lstrip():
            dic["Loc"].append(LocNM[x])
            dic["UpDT"].append(upDT)
            dic["Type"].append("FCST")
            dic["Measure"].append("Outflow")
            dic["DT"].append(activetag[3].get_text().lstrip())
            dic["Units"].append("CFS")
            dic["Value"].append(activetag[5].get_text())
    print("Outflow HG "+LocNM[x])

   
NWRFC = pd.DataFrame(dic)
#NWRFC['IDKEY'] = NWRFC['Loc'].map(str)+NWRFC[['UpDT'][8:10]].map(str)+NWRFC['Measure'].map(str)+NWRFC['Type'].map(str)+NWRFC['DT'].map(str)
NWRFC['IDKEY'] = NWRFC['Loc'].map(str)+upDT[8:10]+NWRFC['Measure'].astype(str).str[0]+NWRFC['Type'].astype(str).str[0]+NWRFC['Units'].astype(str).str[0]+NWRFC['DT'].astype(str).str[2:4]+NWRFC['DT'].astype(str).str[5:7]+NWRFC['DT'].astype(str).str[8:10]+NWRFC['DT'].astype(str).str[11:13]+NWRFC['DT'].astype(str).str[14:16]       

cols = NWRFC.columns.tolist()
col = cols[-1:] + cols[:-1]
NWRFCdf = NWRFC[col]
#NWRFCdf.to_csv('\\\JWTCVDMEDB03\\Fundamentals\\Gas Scrapes\\Hydrofcst.txt', sep='\t', encoding='utf-8', columns =col)
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

NWRFCdf.to_sql('NWRFC_10_fcst_Landing', engine, schema = 'dbo', index = False, if_exists='replace')

sql = 'EXEC Upload_HY_NWRFC_10Day'
sqll = 'EXEC Upload_HY_10day_Gen'
sql2 = 'EXEC HY_Hist_Gen'

import time

time.sleep(5)

cursor = conn.cursor()
cursor.execute(sql)
conn.commit()
conn.close()
time.sleep(5)
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

cursor = conn.cursor()
cursor.execute(sql2)
conn.commit()
conn.close()                                  
#conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
  #                                       "SERVER=JWTCVPMEDB13;"
  #                                       "DATABASE=Fundamentals;"
  #                                       "trusted_connection=yes")


#cursor = conn.cursor()
#cursor.execute(sqll)
#conn.commit()
#conn.close()



#SaveName= '\\\JWTCVDMEDB03\\Fundamentals\\Gas Scrapes\\Hydrofcst.txt'
#np.savetxt(SaveName, NWRFC.values, fmt='%s',delimiter="\t")









