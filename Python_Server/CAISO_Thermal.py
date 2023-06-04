import pandas as pd
import datetime
import numpy as np
from sqlalchemy import create_engine
import pyodbc
import urllib

import requests
import io
from zipfile import ZipFile
from requests.auth import HTTPBasicAuth
from requests.auth import HTTPDigestAuth
import pyodbc
import pandas as pd
import time

DST = datetime.datetime.today().strftime('%Y%m%d')
DD = datetime.date.today()+datetime.timedelta(days=1)
DD1 = datetime.date.today()+datetime.timedelta(days= -1)
DED = DD.strftime('%Y%m%d')
DD2  =datetime.date.today()+datetime.timedelta(days=2)
dt2 = DD2.strftime('%Y%m%d')
DT1 = DD1.strftime('%Y%m%d')
DTS = '20190701'
DTE = '20190702'
#DT = datetime(DD1)
a=[]
#dtt = str(MN)+'/'+str(DN)+'/'+str(YR)
url = 'http://www.caiso.com/outlook/SP/History/'+DT1+'/fuelsource.csv?_=1562096487119'
dt = pd.read_csv(url)
dt=dt.fillna(0)
dt['date']=DD1.strftime('%Y/%m/%d')
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                                    "SERVER=JWTCVPMEDB13;"
                                                  "DATABASE=Fundamentals;"
                                                  "trusted_connection=yes")

conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                       "DATABASE=Fundamentals;"
                                       "trusted_connection=yes")
engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
dt.to_sql('CAISO_STACK_Landing', engine, schema = 'dbo', index = False, if_exists='replace')
time.sleep(1)
conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                       "DATABASE=Fundamentals;"
                                       "trusted_connection=yes")

sql = 'EXEC dbo.Upload_CAISO_RT_Stack'
cursor = conn.cursor()
cursor.execute(sql)
conn.commit()
conn.close()
                
