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

LocNM = ['PRWW1', 'LGSW1', 'TDAO3']
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
#for x in range (0,len(LocNM)):
#    url = "https://www.nwrfc.noaa.gov/station/flowplot/textPlot.cgi?id="+LocNM[x]+"&pe=QI"
#    uClient = uReq(url)
#    page_html = uClient.read()
#     # page_html = uReq(url)
#    uClient.close()
#    soup = BeautifulSoup(page_html, 'html.parser')
#    tr = soup.find_all('tr')
#    for i in range (2,len(tr)):
#        activetag = tr[i].find_all('td')
#        if activetag[2].get_text().lstrip():
#            dic["Loc"].append(LocNM[x])
#            dic["UpDT"].append(upDT)
#            dic["Type"].append("FCST")
#            dic["Measure"].append("Inflows")
#            dic["DT"].append(activetag[2].get_text().lstrip())
#            dic["Units"].append("CFS")
#            dic["Value"].append(activetag[3].get_text())

#NWRFC = pd.DataFrame(dic)



try:
    url = "https://www.nwrfc.noaa.gov/station/flowplot/textPlot.cgi?id="
    #PRWW1dd&pe=QI"
    uClient = uReq(url)
    page_html = uClient.read()
    soup = BeautifulSoup(page_html, 'html.parser')
    pass
except:
    print('Failed')
    from selenium import webdriver
    chromedriver = 'C:/Users/E09138/AppData/Local/Programs/Python/Python37/TSK/chromedriver'
    driver = webdriver.Chrome(chromedriver)
    driver.get("https://forecast.weather.gov/MapClick.php?w0=t&w1=td&w2=wc&w3=sfcwind&w3u=1&w4=sky&w5=pop&w6=rh&w7=rain&w8=thunder&w9=snow&w10=fzg&w11=sleet&w13u=0&w16u=1&AheadHour=0&Submit=Submit&FcstType=graphical&textField1=45.764636&textField2=-120.24008&site=all&unit=0&dd=&bw=")
    fl = 1
    pass
for x in range (0,len(LocNM)):
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
        

