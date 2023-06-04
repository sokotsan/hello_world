import pandas as pd
import datetime
import numpy as np
from sqlalchemy import create_engine
import pyodbc
import urllib

from selenium import webdriver
#chromedriver = 'C:/Users/E09138/AppData/Local/Programs/Python/Python37/TSK/chromedriver'
#driver = webdriver.Chrome(chromedriver)
#driver.get("https://www.stormvistawxmodels.com/")


DT = datetime.datetime.today().strftime('%Y%m%d')
dt = datetime.datetime.today().strftime('%Y-%m-%d')
#DT = '20200415'
z = ['00','06','12','18']
Region = ['KSAC','KSAN','KSFO','KBFL','KSJC','KPSP','KBUR']
Model = ['gfs', 'gfs-ens-mem', 'ecmwf', 'ecmwf-eps']

column_names = ['HR', 'variable', 'value','Model','Run','DT','Station']

df = pd.DataFrame(columns = column_names)




#/client-files/[client-key]/model-data/[model]/[YYYYMMDD]/[cycle]z/city-extraction/individual/[station]_raw.csv

#z = ['00']
     #,'06','12','18']
#Region = ['caiso']
#Model = ['ecmwf']


#,'102','126','150','174','198','222','246','270','294','318','342']

#url = 'http://stormvistawxmodels.com/client-files/81120c2a18f397d2eb959e030fc77b19/model-data/ecmwf-eps/'+DT+'/00z/city-extraction/corrected-max-min_northamerica.csv'


for i in range (0,len(z)):
    Z=z[i]
    for r in range (0,len(Region)):
        R=Region[r]
        for a in range (0,len(Model)):
            try:
                M=Model[a]
                url = 'http://stormvistawxmodels.com/client-files/81120c2a18f397d2eb959e030fc77b19/model-data/'+M+'/'+DT+'/'+Z+'z/city-extraction/individual/'+R+'_raw.csv'
                data = pd.read_csv(url, header = None)
                data.columns = ['HR','tmp2m','tmin2m','tmax2m','precip','dpt2m','heatindex','tmp850']
                dt=pd.DataFrame(data[1:])
                dff = pd.melt(dt,id_vars=['HR'], value_vars=['tmp2m','tmin2m','tmax2m','precip','dpt2m','heatindex','tmp850'])
                #df=df.columns = ['HR','tmp2m','tmin2m','tmax2m','precip','dpt2m','heatindex','tmp850']
                #hdr = list(data.iloc[0])
                #hdr2 = list(data.iloc[1])
                #df_lk = pd.DataFrame({'HR':hdr,'MW':hdr2})
                #df_lk = pd.DataFrame({'HR':hdr,'MW':hdr2})
                #df=df_lk[1:]
                dff['Model']=M
                dff['Run']=Z
                dff['DT']=DT
                dff['Station']=R
                df = pd.concat([df,dff])
                print(str(M)+str(Z)+str(R)+' Success')
            except:
                print(str(M)+str(Z)+str(R)+' Failed')

#driver.quit()
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

df.to_sql('SV_CA_WX_Landing', engine, schema = 'dbo', index = False, if_exists='replace')

##import time

sql = 'EXEC  dbo.Upload_CAISO_SV_WX'

import time

time.sleep(15)

cursor = conn.cursor()
cursor.execute(sql)
conn.commit()
conn.close()

#sqll = 'EXEC Upload_HY_10day_Gen'

#time.sleep(5)

#url2 = 'http://www.stormvistawxmodels.com/client-files/81120c2a18f397d2eb959e030fc77b19/model-data/ecmwf-eps/'+DT+'/00z/city-extraction/f006_raw_northamerica.csv'
#data = pd.read_csv(url2)
#df[z] = pd.melt(data,id_vars='station')
#df['run']=z[1]
