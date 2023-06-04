import pandas as pd
import datetime
import numpy as np
from sqlalchemy import create_engine
import pyodbc
import urllib
from io import StringIO
import requests
import io
from zipfile import ZipFile
from requests.auth import HTTPBasicAuth
#from requests.auth import HTTPDigestAuth
import pyodbc
import pandas as pd
import time


d = 0
dd=d+1
#DST = datetime.datetime.today().strftime('%Y%m%d')
#DD = datetime.date.today()+datetime.timedelta(days=1)
DD1 = datetime.date.today()+datetime.timedelta(days= -dd)
#DD1 = datetime.date.today()
#DED = DD.strftime('%Y%m%d')
#DD2  =datetime.date.today()+datetime.timedelta(days=1)
DD2 = datetime.date.today()+datetime.timedelta(days= -d)
#dt2 = DD2.strftime('%Y%m%d')
#DT1 = DD1.strftime('%Y%m%d')
DTS = DD1.strftime('%Y%m%d')
DTE = DD2.strftime('%Y%m%d')
a=[]
#TH_NP15_GEN-APND
#TH_NP15_GEN_OFFPEAK-APND
#TH_NP15_GEN_ONPEAK-APND
#TH_SP15_GEN-APND
#TH_SP15_GEN_OFFPEAK-APND
#TH_SP15_GEN_ONPEAK-APND
#TH_ZP26_GEN-APND
#TH_ZP26_GEN_OFFPEAK-APND
#TH_ZP26_GEN_ONPEAK-APND
x=0

#'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=SLD_REN_FCST&version=1&startdatetime='+DTS+'01T07:00-0000&enddatetime='+DTE+'01T07:00-0000'

#'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=SLD_REN_FCST&version=1&startdatetime='+DTS+'T07:00-0000&enddatetime='+DTE+'T07:00-0000'

url = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_LMP&version=1&startdatetime='+DTS+'T07:00-0000&enddatetime='+DTE+'T07:00-0000&market_run_id=DAM&node=TH_NP15_GEN_OFFPEAK-APND'
url2 = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_LMP&version=1&startdatetime='+DTS+'T07:00-0000&enddatetime='+DTE+'T07:00-0000&market_run_id=DAM&node=TH_NP15_GEN_ONPEAK-APND'
url3 = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_LMP&version=1&startdatetime='+DTS+'T07:00-0000&enddatetime='+DTE+'T07:00-0000&market_run_id=DAM&node=TH_SP15_GEN_OFFPEAK-APND'
url4 = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_LMP&version=1&startdatetime='+DTS+'T07:00-0000&enddatetime='+DTE+'T07:00-0000&market_run_id=DAM&node=TH_SP15_GEN_ONPEAK-APND'
url5 = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_LMP&version=1&startdatetime='+DTS+'T07:00-0000&enddatetime='+DTE+'T07:00-0000&market_run_id=DAM&node=TH_ZP26_GEN_OFFPEAK-APND'
url6 = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_LMP&version=1&startdatetime='+DTS+'T07:00-0000&enddatetime='+DTE+'T07:00-0000&market_run_id=DAM&node=TH_ZP26_GEN_ONPEAK-APND'
a=[]
while x<3:
    try:
        response = requests.get(url)
        thezip = ZipFile(io.BytesIO(response.content))
        a = thezip.namelist()
        thefile = thezip.read(a[0])
        s=str(thefile,'utf-8')
        data=StringIO(s)
        dt1 = pd.read_csv(data)
        time.sleep(5)
        print('1')
        break
    except:
        x=x+1
        time.sleep(5)
        print('1 Try' + str(x+1))
        if x == 3:
            print('1'+'Failed')
            time.sleep(5)
            break
            

x=0
#print('1') 
#print('1 Try' + str(x+1)) 
a=[]
while x<3:
    try:
        response = requests.get(url3)
        thezip = ZipFile(io.BytesIO(response.content))
        a = thezip.namelist()

        thefile = thezip.read(a[0])
        s=str(thefile,'utf-8')
        data=StringIO(s)
        dt3 = pd.read_csv(data)
        time.sleep(5)
        print('2')
        break
    except:
        x=x+1
        time.sleep(5)
        print('2 Try' + str(x+1))
        if x == 3:
            print('2'+'Failed')
            time.sleep(5)
            break
   
x=0

a=[]
while x<3:
    try:
        response = requests.get(url2)
        thezip = ZipFile(io.BytesIO(response.content))
        a = thezip.namelist()

        thefile = thezip.read(a[0])
        s=str(thefile,'utf-8')
        data=StringIO(s)
        dt2 = pd.read_csv(data)
        time.sleep(5)
        print('3')
        break
    except:
        x=x+1
        time.sleep(5)
        print('3 Try' + str(x+1))
        if x == 3:
            print('3'+'Failed')
            time.sleep(5)
            break        


x=0

a=[]
while x<3:
    try:
        response = requests.get(url4)
        thezip = ZipFile(io.BytesIO(response.content))
        a = thezip.namelist()

        thefile = thezip.read(a[0])
        s=str(thefile,'utf-8')
        data=StringIO(s)
        dt4 = pd.read_csv(data)
        time.sleep(5)
        print('4')
        break
    except:
        x=x+1
        time.sleep(5)
        print('4 Try' + str(x+1))
        if x == 3:
            print('4'+'Failed')
            time.sleep(5)
            break    

x=0

a=[]
while x<3:
    try:
        response = requests.get(url5)
        thezip = ZipFile(io.BytesIO(response.content))
        a = thezip.namelist()

        thefile = thezip.read(a[0])
        s=str(thefile,'utf-8')
        data=StringIO(s)
        dt5 = pd.read_csv(data)
        time.sleep(5)
        print('5')
        break
    except:
        x=x+1
        time.sleep(5)
        print('5 Try' + str(x+1))
        if x == 3:
            print('5'+'Failed')
            time.sleep(5)
            break   

x=0
a=[]
while x<3:
    try:

        response = requests.get(url6)
        thezip = ZipFile(io.BytesIO(response.content))
        a = thezip.namelist()

        thefile = thezip.read(a[0])
        s=str(thefile,'utf-8')
        data=StringIO(s)
        dt6 = pd.read_csv(data)
        time.sleep(5)
        print('6')
        break
    except:
        x=x+1
        time.sleep(6)
        print('6 Try' + str(x+1))
        if x == 3:
            print('6'+'Failed')
            time.sleep(5)
            break  


dt = pd.concat([dt1,dt2,dt3,dt4,dt5,dt6])
 
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")
conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                        "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
dt.to_sql('CAISO_DA_LMP_Landing', engine, schema = 'dbo', index = False, if_exists='replace')

import time
time.sleep(10)
sql = 'EXEC dbo.Upload_CAISO_DA_LMP'
cursor = conn.cursor()
cursor.execute(sql)
conn.commit()
conn.close()
print(DTS)
