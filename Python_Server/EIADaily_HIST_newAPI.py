import requests
import datetime
import numpy as np
import json
import pandas
import urllib
from sqlalchemy import create_engine
import pyodbc
from urllib.request import urlopen as uReq
import pandas as pd
import datetime as dt
sort=False
#%%
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 1000)

ymd_str=dt.date.today().strftime('%Y%m%d')
print(ymd_str)
#%%
BA_in='LKP_EIA_BA_hrly'
fuel_id='LKP_EIA_Type_hrly'

print(ymd_str)

## Read Data with a good date
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")
conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")
engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

SQL_READ='''
SELECT *
  FROM [Fundamentals].[dbo].[%s]
''' % (BA_in)
SQL_READ1='''
SELECT *
  FROM [Fundamentals].[dbo].[%s]
''' % (fuel_id)

BAs=pd.read_sql(SQL_READ, conn)
fuels=pd.read_sql(SQL_READ1, conn)
cursor = conn.cursor()
conn.close()
#%%

BA =['SRP']
FT = ['NG.NUC','NG.OIL','NG.WND','NG.SUN','NG.OTH','NG.NG','NG.COL','NG.WAT','DF','TI','D',]

date_list=pd.date_range(start='2023/5/30', end=ymd_str)
print(date_list)
#%%

df_all=pd.DataFrame()
epoch=len(date_list)
#epoch=2         
for j in range(epoch):
    url = 'https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/?frequency=local-hourly&data[0]=value&facets[respondent][]=PNW&start=%sT00:00:00-08:00&end=%sT23:59:59-08:00&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000&api_key=642c17f567bb726530c47a1e2e0ef433' % (date_list[j].strftime('%Y-%m-%d'),date_list[j].strftime('%Y-%m-%d'))
    data = requests.get(url).json()
    df = pd.DataFrame(data['response']['data']) 
    df_all=pd.concat([df_all,df],axis=0, ignore_index=True)

#%% INPUT


#%%
#print(df_all[['period', 'respondent',  'fueltype', 'type-name', 'value']])
df_all['DT']=pd.to_datetime(df_all['period'].apply(lambda x: x[:10]))
df_all['HE']=(df_all['period'].apply(lambda x: x[11:13])).astype(int)+1
df_all['Fuel_Type']=df_all['fueltype']
df_all['BA']=df_all['respondent']
print(df_all[['DT','HE', 'respondent',  'fueltype', 'type-name', 'value']])

#%%
df_all1=pd.merge(df_all,fuels,how='left')
df_all2=pd.merge(df_all1,BAs,how='left')
df3=df_all2[['DT','HE','BA_ID', 'Type_ID','value']]
print(df3[(df3['HE']==24)*(df3['DT']=='2023-01-15')])
#%%            
# print(df_all2.info())

            #df.to_sql('EIAHR_Landing', engine, schema = 'dbo', index = False, if_exists='replace')

            conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                          "SERVER=JWTCVPMEDB13;"
                                            "DATABASE=Fundamentals;"
                                            "trusted_connection=yes")
            import time
            time.sleep(5)
            sql = 'EXEC dbo.UPLOAD_EIA_GEN'

            cursor = conn.cursor()
            cursor.execute(sql)
            conn.commit()
            conn.close()                                
            print(BA[a]+' '+FT[b])
            x = 0

        except:
            print('ERROR-bad data'+BA[a]+FT[b])
