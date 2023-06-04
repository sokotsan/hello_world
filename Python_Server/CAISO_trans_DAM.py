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

#from selenium import webdriver
#chromedriver = 'C:/Users/E09138/AppData/Local/Programs/Python/Python37/TSK/chromedriver'
#driver = webdriver.Chrome(chromedriver)
#driver.get("https://www.stormvistawxmodels.com/")


DST = datetime.datetime.today().strftime('%Y%m%d')
DD = datetime.date.today()+datetime.timedelta(days=1)
DD1 = datetime.date.today()+datetime.timedelta(days= -5)
DED = DD.strftime('%Y%m%d')
DD2  =datetime.date.today()+datetime.timedelta(days=2)
dt2 = DD2.strftime('%Y%m%d')
DT1 = DD1.strftime('%Y%m%d')
DTS = '20190718'
DTE = '20190719'
    #z = ['006','030','054','078','102','126','150','174','198','222','246','270','294','318','342']
a=[]
    #url = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=TRNS_USAGE&version=1&market_run_id=DAM&ti_id=ALL&ti_direction=I&startdatetime='+DST+'T07:00-0000&enddatetime='+DED+'T07:00-0000'
#url = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=TRNS_USAGE&version=1&market_run_id=DAM&ti_id=ALL&ti_direction=I&startdatetime='+DED+'T07:00-0000&enddatetime='+dt2+'T07:00-0000'
url = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=TRNS_USAGE&version=1&market_run_id=DAM&ti_id=ALL&ti_direction=I&startdatetime='+DT1+'T07:00-0000&enddatetime='+dt2+'T07:00-0000'

 #                  'http://oasis.caiso.com/oasisapi/SingleZip?queryname=TRNS_CURR_USAGE&ti_id=ALL&ti_direction=ALL&startdatetime=20190708T07:00-0000&enddatetime=20190709T07:00-0000&version=1&resultformat=6'
    #url = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=TRNS_USAGE&version=1&market_run_id=HASP&ti_id=ALL&ti_direction=I&startdatetime='+DT1+'T07:00-0000&enddatetime='+DST+'T07:00-0000'

#TRNS_CURR_USA

response = requests.get(url,  timeout=45)
    #data = pd.read_csv(url, header = None)
    #hdr = list(data.iloc[0])
    #lst = list(range(0,len(hdr)))
    #df_lk = pd.DataFrame({'dn':lst,'DT':hdr})
thezip = ZipFile(io.BytesIO(response.content))
a = thezip.namelist()
thefile = thezip.extract(a[0])

    #data2 = pd.read_csv(url, skiprows = 1)

    #hdr2 = list(data.iloc[1])
    #temps = data.iloc[2:]


dt = pd.read_csv(thefile)

params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

dt.to_sql('CAISO_Trans_Landing', engine, schema = 'dbo', index = False, if_exists='replace')

import time
time.sleep(10)
sql = 'EXEC dbo.Upload_CAISO_Trans'

cursor = conn.cursor()
cursor.execute(sql)
conn.commit()
conn.close()

#df = pd.melt(temps,id_vars=0)
#df['Metric'] = np.where(df['variable']%2 == 0,'Max','Min')
#dff = df.merge(df_lk, how = 'left', left_on='variable',right_on='dn')
#dff=dff.drop(['variable','dn'], axis=1)
#dff['HR']='000'
#dff.rename(columns={0:'station'},inplace = True)
#cols = ['station', 'Metric', 'DT', 'HR','value']
#dff=dff[cols]
#dff['Station','Value','Type','DT','DTT')
#temps = data = data[hdr2]
#hdr = []
#hdr = list(data)


#for i in range (0,len(z)):
#    url2 = 'http://www.stormvistawxmodels.com/client-files/81120c2a18f397d2eb959e030fc77b19/model-data/ecmwf-eps/'+DT+'/00z/city-extraction/f'+z[i]+'_raw_northamerica.csv'
#    data = pd.read_csv(url2)
#    df = pd.melt(data,id_vars='station')
#    df['HR']=z[i]
#    df['DT']=dt
#    df.rename(columns={'variable':'Metric'},inplace = True)
#    df=df[cols]
#    dff = pd.concat([dff,df])

#dff['Run'] = '00z'
#dff['UPDT'] = dt
#driver.quit()
#params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
#                                         "SERVER=JWTCVPMEDB03;"
#                                         "DATABASE=Fundamentals;"
#                                         "trusted_connection=yes")
#
#conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
#                                         "SERVER=JWTCVPMEDB03;"
#                                         "DATABASE=Fundamentals;"
#                                         "trusted_connection=yes")

#engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

#dff.to_sql('SV_WX_Landing', engine, schema = 'dbo', index = False, if_exists='replace')

#import time

#sql = 'EXEC Upload_SV_WX'



#time.sleep(15)

#cursor = conn.cursor()
#cursor.execute(sql)
#conn.commit()
#conn.close()

#sqll = 'EXEC Upload_HY_10day_Gen'

#time.sleep(5)

#url2 = 'http://www.stormvistawxmodels.com/client-files/81120c2a18f397d2eb959e030fc77b19/model-data/ecmwf-eps/'+DT+'/00z/city-extraction/f006_raw_northamerica.csv'
#data = pd.read_csv(url2)
#df[z] = pd.melt(data,id_vars='station')
#df['run']=z[1]
