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
#from requests.auth import HTTPDigestAuth
import pyodbc
import pandas as pd
import time
a=[]


yr=['2020']
mn=['03']
dn=['18','19']





for y in range(len(yr)):
    YR = yr[y]
    for m in range(len(mn)):
        MN = mn[m] 
        for d in range(len(dn)):
            try:
                DN = dn[d]
                dtt = str(MN)+'/'+str(DN)+'/'+str(YR)
                url = 'http://www.caiso.com/outlook/SP/History/'+YR+MN+DN+'/fuelsource.csv?_=1562096487119'
                dt = pd.read_csv(url)
                dt=dt.fillna(0)
                dt['date']=MN+'/'+DN+'/'+YR
                params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                                         "SERVER=JWTCVPMEDB03;"
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
                print(dtt+' Success')
                time.sleep(1)
            except:
                DN = dn[d]
                dtt = str(MN)+'/'+str(DN)+'/'+str(YR)
                print(dtt+' FAILED')
                #break
                
