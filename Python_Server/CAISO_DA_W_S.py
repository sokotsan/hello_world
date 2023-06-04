import pandas as pd
import datetime
import numpy as np
from sqlalchemy import create_engine
import pyodbc
import urllib

import requests
import io
from io import StringIO
from zipfile import ZipFile
from requests.auth import HTTPBasicAuth
#from requests.auth import HTTPDigestAuth
import pyodbc
import pandas as pd


#DST = datetime.datetime.today().strftime('%Y%m%d')
#DD = datetime.date.today()+datetime.timedelta(days=1)
DD1 = datetime.date.today()+datetime.timedelta(days= -1)
#DED = DD.strftime('%Y%m%d')
DD2  =datetime.date.today()+datetime.timedelta(days=8)
DT1 = DD1.strftime('%Y%m%d')
dt2 = DD2.strftime('%Y%m%d')
DTS = '20190101'
DTE = '20190131'

#'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=SLD_REN_FCST&version=1&startdatetime='+DTS+'01T07:00-0000&enddatetime='+DTE+'01T07:00-0000'

#'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=SLD_REN_FCST&version=1&startdatetime='+DTS+'T07:00-0000&enddatetime='+DTE+'T07:00-0000'

url = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=SLD_REN_FCST&version=1&market_run_id=DAM&startdatetime='+DT1+'T07:00-0000&enddatetime='+dt2+'T07:00-0000'
#http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=ENE_CB_AWARDS&version=1&startdatetime=20190701T07:00-0000&enddatetime=20190711T07:00-0000
response = requests.get(url, timeout=10)
thezip = ZipFile(io.BytesIO(response.content))
a = thezip.namelist()
thefile = thezip.read(a[0])
s=str(thefile,'utf-8')
data=StringIO(s)
dt = pd.read_csv(data)
 

params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")
conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                        "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
dt.to_sql('CAISO_DA_WS_Landing', engine, schema = 'dbo', index = False, if_exists='replace')

import time
time.sleep(10)
sql = 'EXEC dbo.Upload_CAISO_W_S_FC'
cursor = conn.cursor()
cursor.execute(sql)
conn.commit()
conn.close()
print(dt2)
