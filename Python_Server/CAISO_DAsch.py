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

#DST = datetime.datetime.today().strftime('%Y%m%d')
#DD = datetime.date.today()+datetime.timedelta(days=1)
DD1 = datetime.date.today()+datetime.timedelta(days= -3)
#DED = DD.strftime('%Y%m%d')
DD2  =datetime.date.today()+datetime.timedelta(days=3)
dt2 = DD2.strftime('%Y%m%d')
DT1 = DD1.strftime('%Y%m%d')
#DTS = '20171201'
#DTE = '20171231'

path = 'C:/Users/E09138/AppData/Local/Programs/Python/Python37/TSK'
url = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=ENE_SLRS&version=1&market_run_id=DAM&tac_zone_name=ALL&schedule=ALL&startdatetime='+DT1+'T08:00-0000&enddatetime='+dt2+'T08:00-0000&resultformat=6'
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
dt.to_sql('CAISO_DAsch_Landing', engine, schema = 'dbo', index = False, if_exists='replace')

engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
dt.to_sql('CAISO_DAsch_Landing', engine, schema = 'dbo', index = False, if_exists='replace')
import time
time.sleep(10)
sql = 'EXEC dbo.Upload_CAISO_DA_Sched'
cursor = conn.cursor()
cursor.execute(sql)
conn.commit()
conn.close()
print(DT1)
