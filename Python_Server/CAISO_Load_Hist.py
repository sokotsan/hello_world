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
import os
#from selenium import webdriver
#chromedriver = 'C:/Users/E09138/AppData/Local/Programs/Python/Python37/TSK/chromedriver'
#driver = webdriver.Chrome(chromedriver)
#driver.get("https://www.stormvistawxmodels.com/")


#DST = datetime.datetime.today().strftime('%Y%m%d')
#DD = datetime.date.today()+datetime.timedelta(days=1)
#DD1 = datetime.date.today()+datetime.timedelta(days= -1)
#DED = DD.strftime('%Y%m%d')
#DD2  =datetime.date.today()+datetime.timedelta(days=2)
#dt2 = DD2.strftime('%Y%m%d')
#DT1 = DD1.strftime('%Y%m%d')
#DTS = '20190701'
#DTE = '20190702'
    #z = ['006','030','054','078','102','126','150','174','198','222','246','270','294','318','342']
a=[]
    #url = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=TRNS_USAGE&version=1&market_run_id=DAM&ti_id=ALL&ti_direction=I&startdatetime='+DST+'T07:00-0000&enddatetime='+DED+'T07:00-0000'
#url = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=TRNS_USAGE&version=1&market_run_id=DAM&ti_id=ALL&ti_direction=I&startdatetime='+DED+'T07:00-0000&enddatetime='+dt2+'T07:00-0000'
#url = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=TRNS_USAGE&version=1&market_run_id=DAM&ti_id=ALL&ti_direction=I&startdatetime='+DTS+'T07:00-0000&enddatetime='+DTE+'T07:00-0000'




#yr=['2017','2018','2019']
#mn=['01','02','03','04','05','06','07','08','09','10','11','12']
#dn=['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31']


yr=['2019']
mn=['07','08']
dn=['12','13','14','15','16']



path = 'C:/Users/E09138/AppData/Local/Programs/Python/Python37/TSK'

for y in range(len(yr)):
    YR = yr[y]
    for m in range(len(mn)):
        try:
            a=[]
            MN = mn[m]
            MM = mn[m+1]
            dtt = str(MN)+'/'+str(YR)
            url = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=SLD_FCST&version=1&market_run_id=DAM&startdatetime='+YR+MN+'01T07:00-0000&enddatetime='+YR+MM+'01T07:00-0000'
            response = requests.get(url)
            thezip = ZipFile(io.BytesIO(response.content))
            a = thezip.namelist()
            thefile = thezip.extract(a[0])
            dt = pd.read_csv(thefile)
            os.remove(path+'/'+str(a[0]))
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
            print(dtt)
        except:
            dtt = str(MN)+'/'+str(YR)
            print(dtt+' FAILED')
                #break
