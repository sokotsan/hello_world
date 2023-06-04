from urllib.request import urlopen as uReq
import urllib
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
from datetime import date, timedelta
from contextlib import suppress
from sqlalchemy import create_engine
import pyodbc
#http = urllib3.PoolManager()

#setdate
DB = 1

upDT = (date.today() - timedelta(days = DB)).strftime('%m/%d/%Y')
url = 'https://www.nwd-wc.usace.army.mil/dd/nwdp/project_hourly/webexec/rep?r=gcl&ago=1'


LocNM = []

LocNM = ['alf','bcl','bon','cgr','chj','det','dex','dwr','fos','gcl','gpr','hcr','hgh','ihr','jda',
'lgs','lib','lmn','lop','los','lwg','mcn','prd','ris','rrh','tda','wan','wel']

dic = {
    "Loc" : [],
    "DT" : [],
    "Hour":[],
    "Measure":[],
    "Flow" : []}

fl = 0
page_html = bytearray()
#try:
#    url = 'http://www.nwd-wc.usace.army.mil/dd/nwdp/project_hourly/webexec/rep?r=gcl&ago=6'
#    uClient = uReq(url)
#    page_html = uClient.read()
#    # page_html = uReq(url)
#    uClient.close()
#    soup = BeautifulSoup(page_html, 'html.parser')
#    pass
#except:
#    print('Failed')
#    from selenium import webdriver
#    chromedriver = 'D:/Program/PYTHON_SERVER/Task/TSK/chromedriver'
#    driver = webdriver.Chrome(chromedriver)
#    driver.get("http://www.nwd-wc.usace.army.mil/dd/nwdp/project_hourly/webexec/rep?r=gcl&ago=1")
#    fl = 1
#    pass
for x in range (0,len(LocNM)):
    try:
        url = 'https://www.nwd-wc.usace.army.mil/dd/nwdp/project_hourly/webexec/rep?r='+LocNM[x]+"&ago="+str(DB)
        r =  requests.get(url , timeout=5)
        page_html = r.content
        soup = BeautifulSoup(page_html, 'html.parser')
    # page_html = uReq(url)
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
    tr = soup.find_all('tr')
    cl = tr[0].find_all('th')
    rn = len(cl)
    print ('Success ' + LocNM[x])
    for i in range (1,25):
        activetag = tr[i].find_all('td')
        for y in range (1,rn):
            dic["Loc"].append(LocNM[x])
            dic["DT"].append(upDT)
            dic["Hour"].append(activetag[0].get_text().lstrip())
            dic["Measure"].append(cl[y].get_text().lstrip())
            dic["Flow"].append(activetag[y].get_text().lstrip())




   
USACE = pd.DataFrame(dic)
##NWRFC['IDKEY'] = NWRFC['Loc'].map(str)+NWRFC[['UpDT'][8:10]].map(str)+NWRFC['Measure'].map(str)+NWRFC['Type'].map(str)+NWRFC['DT'].map(str)
#NWRFC['IDKEY'] = NWRFC['Loc'].map(str)+upDT[8:10]+NWRFC['Measure'].astype(str).str[0]+NWRFC['Type'].astype(str).str[0]+NWRFC['Units'].astype(str).str[0]+NWRFC['DT'].astype(str).str[2:4]+NWRFC['DT'].astype(str).str[5:7]+NWRFC['DT'].astype(str).str[8:10]+NWRFC['DT'].astype(str).str[11:13]+NWRFC['DT'].astype(str).str[14:16]       

#cols = NWRFC.columns.tolist()
#col = cols[-1:] + cols[:-1]
#NWRFCdf = NWRFC[col]
##NWRFCdf.to_csv('\\\JWTCVDMEDB03\\Fundamentals\\Gas Scrapes\\Hydrofcst.txt', sep='\t', encoding='utf-8', columns =col)

params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")
conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")
engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

USACE.to_sql('USACEHY_Landing', engine, schema = 'dbo', index = False, if_exists='replace')

sql = 'EXEC Upload_USACE_HLY'
#sqll = 'EXEC Upload_HY_10day_Gen'


#import time

#time.sleep(5)

cursor = conn.cursor()
cursor.execute(sql)
conn.commit()
conn.close()

#time.sleep(2)

                                  
#conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
#                                         "SERVER=JWTCVPMEDB13;"
#                                         "DATABASE=Fundamentals;"
#                                         "trusted_connection=yes")


#cursor = conn.cursor()
#cursor.execute(sqll)
#conn.commit()
#conn.close()

#if fl == 1:
#    driver.quit()

#SaveName= '\\\JWTCVDMEDB03\\Fundamentals\\Gas Scrapes\\Hydrofcst.txt'
#np.savetxt(SaveName, NWRFC.values, fmt='%s',delimiter="\t")









