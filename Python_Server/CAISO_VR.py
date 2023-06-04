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
import os
import time

d=-1
dd=d+4

a=[]

DD = datetime.date.today()+datetime.timedelta(days=dd)
DD1 = datetime.date.today()+datetime.timedelta(days= d)
dt2 = DD.strftime('%Y%m%d')
DT1 = DD1.strftime('%Y%m%d')
a=[]
#MN = mn[m]
#MM = mn[m+1]
#dtt = str(MN)+'/'+str(YR)
url = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=ENE_CB_AWARDS&version=1&startdatetime='+DT1+'T07:00-0000&enddatetime='+dt2+'T07:00-0000' 
            #url2 = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=ENE_CB_AWARDS&version=1&startdatetime='+YR+MN+'01T07:00-0000&enddatetime='+YR+MM+'01T07:00-0000'            #http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=ENE_SLRS&version=1&market_run_id=DAM&tac_zone_name=ALL&schedule=ALL&startdatetime=20190710T07:00-0000&enddatetime=20190713T07:00-0000
x=0
while x<3:
    try:
        response = requests.get(url, timeout=10)
        thezip = ZipFile(io.BytesIO(response.content))
        a = thezip.namelist()
        thefile = thezip.read(a[0])
        s=str(thefile,'utf-8')
        data=StringIO(s)
        dt = pd.read_csv(data)
        break
    except:
        x=x+1
        time.sleep(5)
        print('Try' + str(x+1))
        if x == 3:
            print('1'+'Failed')
            time.sleep(5)
            break

params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")
conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
dt.to_sql('CAISO_DA_VR_Landing', engine, schema = 'dbo', index = False, if_exists='replace')
import time
time.sleep(10)
sql = 'EXEC dbo.Upload_CAISO_DA_VR'
cursor = conn.cursor()
cursor.execute(sql)
conn.commit()
conn.close()
#print(dtt)

#http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=ENE_SLRS&version=1&market_run_id=DAM&tac_zone_name=ALL&schedule=ALL&startdatetime=20170101T08:00-0000&enddatetime=20170202T08:00-0000
#http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=ENE_SLRS&version=1&market_run_id=DAM&tac_zone_name=ALL&schedule=ALL&startdatetime=20190715T07:00-0000&enddatetime=20190716T07:00-0000
