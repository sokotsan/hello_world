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
import pyodbc
import pandas as pd
import os
path = 'C:/Users/E09138/AppData/Local/Programs/Python/Python37/TSK'
DST = datetime.datetime.today().strftime('%Y%m%d')
DD = datetime.date.today()+datetime.timedelta(days=2)
DD1 = datetime.date.today()+datetime.timedelta(days= -5)
DED = DD.strftime('%Y%m%d')
DD2  =datetime.date.today()+datetime.timedelta(days=8)
dt2 = DD2.strftime('%Y%m%d')
DT1 = DD1.strftime('%Y%m%d')
DTS = '20190701'
DTE = '20190702'

a=[]
url = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=SLD_FCST&version=1&startdatetime='+DT1+'T07:00-0000&enddatetime='+dt2+'T07:00-0000'

response = requests.get(url,  timeout=10)

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

dt.to_sql('CAISO_LDFCST_Landing', engine, schema = 'dbo', index = False, if_exists='replace')

import time
time.sleep(10)
sql = 'EXEC dbo.Upload_CAISO_LD_FC'

cursor = conn.cursor()
cursor.execute(sql)
conn.commit()
conn.close()
