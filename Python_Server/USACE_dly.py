from urllib.request import urlopen as uReq
import urllib
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
from datetime import date, timedelta
import datetime
from contextlib import suppress
from sqlalchemy import create_engine
import pyodbc
#http = urllib3.PoolManager()

#setdate
DB = [0,1]
tday = datetime.datetime.today().day
#upDT = (date.today() - timedelta(days = DB)).strftime('%m/%d/%Y')
url = 'https://www.nwd-wc.usace.army.mil/dd/nwdp/project_daily/webexec/rep?r=alf&ago=1'


LocNM = []

LocNM = ['alf','bcl','bon','cgr','chj','det','dex','dwr','fos','gcl','gpr','hcr','hgh','ihr','jda',
'lgs','lib','lmn','lop','los','lwg','mcn','prd','ris','rrh','tda','wan','wel']

dic = {
    "Loc" : [],
    "DT" : [],
    "Measure":[],
    "Flow" : []}

fl = 0
page_html = bytearray()
#try:
#    url = 'http://www.nwd-wc.usace.army.mil/dd/nwdp/project_daily/webexec/rep?r=alf&ago=1'
#    uClient = uReq(url)
#    page_html = uClient.read()
    # page_html = uReq(url)
#    uClient.close()
#    soup = BeautifulSoup(page_html, 'html.parser')
#    pass
#except:
#    print('Failed')
#    from selenium import webdriver
#    chromedriver = 'C:/Users/E09138/AppData/Local/Programs/Python/Python37/TSK/chromedriver'
#    driver = webdriver.Chrome(chromedriver)
#    driver.get("http://www.nwd-wc.usace.army.mil/dd/nwdp/project_daily/webexec/rep?r=alf&ago=1")
#    fl = 1
#    pass
for x in range (0,len(LocNM)):
    try:
        url = 'https://www.nwd-wc.usace.army.mil/dd/nwdp/project_daily/webexec/rep?r='+LocNM[x]+"&ago="+str(DB[0])
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
        
    MMY = soup.find_all('h2')
    MY = MMY[0].get_text().split(" ")
    M=MY[0]
    Y=MY[1]
    tr = soup.find_all('tr')
    cl = tr[0].find_all('th')
    rn = len(cl)
    drn = len(tr)
    for i in range (1,drn-6):
        activetag = tr[i].find_all('td')
        for d in range (1,rn):
            dmy = str(activetag[0].get_text().lstrip())+' '+str(M)+' '+str(Y)
            dt = datetime.datetime.strptime(dmy,'%d %B %Y')
            DT = dt.strftime('%Y-%m-%d')
            dic["Loc"].append(LocNM[x])
            dic["DT"].append(DT)
            dic["Measure"].append(cl[d].get_text().lstrip())
            dic["Flow"].append(activetag[d].get_text().lstrip())
    print ('Succuss '+   LocNM[x])   
# if early in current month, scrape last months data as well 
if tday < 15:
    for x in range (0,len(LocNM)):
        try:
            url = 'https://www.nwd-wc.usace.army.mil/dd/nwdp/project_daily/webexec/rep?r='+LocNM[x]+"&ago="+str(DB[1])
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
        
        soup = BeautifulSoup(page_html, 'html.parser')
        MMY = soup.find_all('h2')
        MY = MMY[0].get_text().split(" ")
        M=MY[0]
        Y=MY[1]
        tr = soup.find_all('tr')
        cl = tr[0].find_all('th')
        rn = len(cl)
        drn = len(tr)
        for i in range (1,drn-6):
            activetag = tr[i].find_all('td')
            for d in range (1,rn):
                dmy = str(activetag[0].get_text().lstrip())+' '+str(M)+' '+str(Y)
                dt = datetime.datetime.strptime(dmy,'%d %B %Y')
                DT = dt.strftime('%Y-%m-%d')
                dic["Loc"].append(LocNM[x])
                dic["DT"].append(DT)
                dic["Measure"].append(cl[d].get_text().lstrip())
                dic["Flow"].append(activetag[d].get_text().lstrip())
        print ('Succuss '+   LocNM[x]) 


   
USACE_dly = pd.DataFrame(dic)


params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")
conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")
engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

USACE_dly.to_sql('USACEdly_Landing', engine, schema = 'dbo', index = False, if_exists='replace')

sql = 'EXEC Upload_USACE_DLY'
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









