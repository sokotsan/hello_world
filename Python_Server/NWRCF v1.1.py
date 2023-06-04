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

LocNM = ['PRWW1', 'LGSW1', 'TDAO3', 'IHDW1', 'LGDW1', 'DWRI1', 'LMNW1', 'BONO3', 'GCDW1', 'JDAO3', 'MCDW1', 'CHJW1', 'WANW1','WELW1']
PoolNM = ['GCDW1']
dic = {
    "Loc" : [],
    "UpDT" : [],
    "Type" : [],
    "Measure" : [],
    "DT" : [],
    "Units" : [],
    "Value" : []}
upDT = datetime.datetime.today().strftime('%Y-%m-%d')
fl = 0
for x in range (0,len(LocNM)):
    try:
        url = "https://www.nwrfc.noaa.gov/station/flowplot/textPlot.cgi?id="+LocNM[x]+"&pe=QI"
        uClient = uReq(url)
        page_html = uClient.read()
       # page_html = uReq(url)
        uClient.close()
        soup = BeautifulSoup(page_html, 'html.parser')
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
        print('success')
        break
    except:
        print('Failed' + LocMN[x])
        from selenium import webdriver
        chromedriver = 'C:/Users/E09138/AppData/Local/Programs/Python/Python37/TSK/chromedriver'
        driver = webdriver.Chrome(chromedriver)
        driver.get("https://forecast.weather.gov/MapClick.php?w0=t&w1=td&w2=wc&w3=sfcwind&w3u=1&w4=sky&w5=pop&w6=rh&w7=rain&w8=thunder&w9=snow&w10=fzg&w11=sleet&w13u=0&w16u=1&AheadHour=0&Submit=Submit&FcstType=graphical&textField1=45.764636&textField2=-120.24008&site=all&unit=0&dd=&bw=")
        fl = 1
        pass
#NWRFC = pd.DataFrame(dic)
    
for x in range (0,len(LocNM)):
    url = "https://www.nwrfc.noaa.gov/station/flowplot/textPlot.cgi?id="+LocNM[x]+"&pe=QR"
    uClient = uReq(url)
    page_html = uClient.read()
    uClient.close()
    soup = BeautifulSoup(page_html, 'html.parser')
    tr = soup.find_all('tr')
    for i in range (2,len(tr)):
        activetag = tr[i].find_all('td')
        if activetag[1].get_text().lstrip():
            dic["Loc"].append(LocNM[x])
            dic["UpDT"].append(upDT)
            dic["Type"].append("ACT")
            dic["Measure"].append("Outflow")
            dic["DT"].append(activetag[0].get_text().lstrip())
            dic["Units"].append("CFS")
            dic["Value"].append(activetag[1].get_text())



for x in range (0,len(PoolNM)):
    url = "https://www.nwrfc.noaa.gov/station/flowplot/textPlot.cgi?id="+PoolNM[x]+"&pe=HF"
    uClient = uReq(url)
    page_html = uClient.read()
    uClient.close()
    soup = BeautifulSoup(page_html, 'html.parser')
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






   
NWRFC = pd.DataFrame(dic)
#NWRFC['IDKEY'] = NWRFC['Loc'].map(str)+NWRFC[['UpDT'][8:10]].map(str)+NWRFC['Measure'].map(str)+NWRFC['Type'].map(str)+NWRFC['DT'].map(str)
NWRFC['IDKEY'] = NWRFC['Loc'].map(str)+upDT[8:10]+NWRFC['Measure'].astype(str).str[0]+NWRFC['Type'].astype(str).str[0]+NWRFC['Units'].astype(str).str[0]+NWRFC['DT'].astype(str).str[2:4]+NWRFC['DT'].astype(str).str[5:7]+NWRFC['DT'].astype(str).str[8:10]+NWRFC['DT'].astype(str).str[11:13]+NWRFC['DT'].astype(str).str[14:16]       

cols = NWRFC.columns.tolist()
col = cols[-1:] + cols[:-1]
NWRFCdf = NWRFC[col]
#NWRFCdf.to_csv('\\\JWTCVDMEDB03\\Fundamentals\\Gas Scrapes\\Hydrofcst.txt', sep='\t', encoding='utf-8', columns =col)

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

cursor = conn.cursor()
cursor.execute(sql)
conn.commit()
conn.close()

if fl == 1:
    driver.quit()

#SaveName= '\\\JWTCVDMEDB03\\Fundamentals\\Gas Scrapes\\Hydrofcst.txt'
#np.savetxt(SaveName, NWRFC.values, fmt='%s',delimiter="\t")









