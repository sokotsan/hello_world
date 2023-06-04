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
z = ['00','06','12','18']
Region = ['caiso']
Model = ['gfs', 'gfs-ens-mem', 'ecmwf', 'ecmwf-eps']

column_names = ['HR', 'MW', 'Model','Run','DT']
dff = pd.DataFrame(columns = column_names)

#z = ['00']
     #,'06','12','18']
#Region = ['caiso']
#Model = ['ecmwf']


#,'102','126','150','174','198','222','246','270','294','318','342']

#url = 'http://stormvistawxmodels.com/client-files/81120c2a18f397d2eb959e030fc77b19/model-data/ecmwf-eps/'+DT+'/00z/city-extraction/corrected-max-min_northamerica.csv'


for i in range (0,len(z)):
    Z=z[i]
    for a in range (0,len(Model)):
        try:
            M=Model[a]
            url = 'http://stormvistawxmodels.com/client-files/81120c2a18f397d2eb959e030fc77b19/model-data/'+M+'/'+DT+'/'+Z+'z/renewables/caiso-windgen-forecast.csv'
            data = pd.read_csv(url, header = None)
            hdr = list(data.iloc[0])
            hdr2 = list(data.iloc[1])
            df_lk = pd.DataFrame({'HR':hdr,'MW':hdr2})
            df=df_lk[1:]
            df['Model']=M
            df['Run']=Z
            df['DT']=dt
            dff = pd.concat([dff,df])
        except:
            print(str(M)+str(Z)+' Failed')
##data2 = pd.read_csv(url, skiprows = 1)

##hdr2 = list(data.iloc[1])
#temps = data.iloc[2:]

#df = pd.melt(temps,id_vars=0)
#df['Metric'] = np.where(df['variable']%2 == 0,'Max','Min')
#dff = df.merge(df_lk, how = 'left', left_on='variable',right_on='dn')
#dff=dff.drop(['variable','dn'], axis=1)
#dff['HR']='000'
#dff.rename(columns={0:'station'},inplace = True)
#cols = ['station', 'Metric', 'DT', 'HR','value']
#dff=dff[cols]
##dff['Station','Value','Type','DT','DTT')
##temps = data = data[hdr2]
##hdr = []
##hdr = list(data)


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
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

dff.to_sql('SV_CA_Wind_Landing', engine, schema = 'dbo', index = False, if_exists='replace')

##import time

sql = 'EXEC  dbo.UPLOAD_CAISO_WIND_FCST'

import time

time.sleep(5)

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
