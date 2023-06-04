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




#yr=['2019','2017','2018']
#mn=['01','02','03','04','05','06','07','08','09','10','11','12']
#dn=['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31']

yr=['2016']
mn=['02','03','04','05','06','07','08','09','10','11','12']
dn=['01','15','28','30','31']
DDD=['20160313','20160314',
'20161018','20161019',
'20161128','20161129',
'20161129','20161130',
'20170312','20170313',
'20170406','20170407',
'20170502','20170503',
'20170508','20170509',
'20170511','20170512',
'20170515','20170516',
'20170520','20170521',
'20170521','20170522',
'20170522','20170523',
'20170523','20170524',
'20170525','20170526',
'20170530','20170531',
'20170702','20170703',
'20170804','20170805',
'20170805','20170806',
'20170807','20170808',
'20170808','20170809',
'20180220','20180221',
'20180221','20180222',
'20180222','20180223',
'20180223','20180224',
'20180224','20180225',
'20180225','20180226',
'20180226','20180227',
'20180227','20180228',
'20180228','20180301',
'20180301','20180302',
'20180303','20180304',
'20180304','20180305',
'20180311','20180312',
'20180719','20180720',
'20180727','20180728',
'20181001','20181002',
'20181110','20181111',
'20181111','20181112',
'20190220','20190221',
'20190221','20190222',
'20190222','20190223',
'20190310','20190311',
'20190813','20190814'
]

path = 'C:/Users/E09138/AppData/Local/Programs/Python/Python37/TSK'
for i in range(len(DDD)):
    try:
        a=[]
        #DN = dn[d]
        #DM = dn[d+1]
        DD = DDD[i]
        i=i+1
        DD2 = DDD[i]
        url = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_LMP&version=1&startdatetime='+DD+'T07:00-0000&enddatetime='+DD2+'T07:00-0000&market_run_id=DAM&node=TH_NP15_GEN_OFFPEAK-APND'
        url2 = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_LMP&version=1&startdatetime='+DD+'T07:00-0000&enddatetime='+DD2+'T07:00-0000&market_run_id=DAM&node=TH_NP15_GEN_ONPEAK-APND'
        url3 = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_LMP&version=1&startdatetime='+DD+'T07:00-0000&enddatetime='+DD2+'T07:00-0000&market_run_id=DAM&node=TH_SP15_GEN_OFFPEAK-APND'
        url4 = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_LMP&version=1&startdatetime='+DD+'T07:00-0000&enddatetime='+DD2+'T07:00-0000&market_run_id=DAM&node=TH_SP15_GEN_ONPEAK-APND'
        url5 = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_LMP&version=1&startdatetime='+DD+'T07:00-0000&enddatetime='+DD2+'T07:00-0000&market_run_id=DAM&node=TH_ZP26_GEN_OFFPEAK-APND'
        url6 = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_LMP&version=1&startdatetime='+DD+'T07:00-0000&enddatetime='+DD2+'T07:00-0000&market_run_id=DAM&node=TH_ZP26_GEN_ONPEAK-APND'

        response = requests.get(url)
        thezip = ZipFile(io.BytesIO(response.content))
        a = thezip.namelist()
        thefile = thezip.extract(a[0])
        dt1 = pd.read_csv(thefile)
        time.sleep(5)
        os.remove(path+'/'+str(a[0]))

                #time.sleep(5)
        response = requests.get(url3)
        thezip = ZipFile(io.BytesIO(response.content))
        a = thezip.namelist()
        thefile = thezip.extract(a[0])
        dt3 = pd.read_csv(thefile)
        time.sleep(5)
        os.remove(path+'/'+str(a[0]))
 
        response = requests.get(url2)
        thezip = ZipFile(io.BytesIO(response.content))
        a = thezip.namelist()
        thefile = thezip.extract(a[0])
        dt2 = pd.read_csv(thefile)
        time.sleep(5)
        os.remove(path+'/'+str(a[0]))

                #time.sleep(5)
        response = requests.get(url4)
        thezip = ZipFile(io.BytesIO(response.content))
        a = thezip.namelist()
        thefile = thezip.extract(a[0])
        time.sleep(5)
        dt4 = pd.read_csv(thefile)

        os.remove(path+'/'+str(a[0]))

        response = requests.get(url5)
        thezip = ZipFile(io.BytesIO(response.content))
        a = thezip.namelist()
        thefile = thezip.extract(a[0])
        dt5 = pd.read_csv(thefile)
        time.sleep(5)
        os.remove(path+'/'+str(a[0]))

        response = requests.get(url6)
        thezip = ZipFile(io.BytesIO(response.content))
        a = thezip.namelist()
        thefile = thezip.extract(a[0])
        dt6 = pd.read_csv(thefile)
        time.sleep(5)
        os.remove(path+'/'+str(a[0]))

        dt = pd.concat([dt1,dt2,dt3,dt4,dt5,dt6])
                #dt = pd.concat([dt2,dt4])


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
        time.sleep(5)
        sql = 'EXEC dbo.Upload_CAISO_DA_LMP'
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        conn.close()
        print(DD)
        i=i+1
    except:
        time.sleep(5)
        dtt = DDD[i]
        i=i+1
        print(DD+' FAILED')
#                pass
  

#http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=ENE_SLRS&version=1&market_run_id=DAM&tac_zone_name=ALL&schedule=ALL&startdatetime=20170101T08:00-0000&enddatetime=20170202T08:00-0000
#http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=ENE_SLRS&version=1&market_run_id=DAM&tac_zone_name=ALL&schedule=ALL&startdatetime=20190715T07:00-0000&enddatetime=20190716T07:00-0000
