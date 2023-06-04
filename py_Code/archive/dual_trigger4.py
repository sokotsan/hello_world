
import urllib
import pandas as pd
import numpy as np
from sklearn.metrics import r2_score
import matplotlib.pyplot as plt
#import numpy as np
import cx_Oracle
#reg = linear_model.LinearRegression()
#x=[0,1,2]
#x=x.reshape(-1,1)
#y=[0,2,4]
#reg.fit(x,y)
#r_sq = reg.score(x,y)
#reg.coef_
import sqlalchemy
from contextlib import suppress
from sqlalchemy import create_engine
import pyodbc
import os, sys
import datetime as dt
#%%
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 1000)

ymd_str=dt.date.today().strftime('%Y%m%d')
print(ymd_str)
#%%
import cx_Oracle
try:
    cx_Oracle.init_oracle_client(lib_dir=r"D:\Oracle\instantclient_21_6")
except:
    print('Oracle Client intialized')    
print(sys.version)
#print(os.environ['ORACLE_HOME'])
#%%
#DF_URL = pandas.read_sql_query('select * from dbo.EIA_BA_NG_List',conn)

dsn_tns = cx_Oracle.makedsn('xpmedbe01', '1521', service_name='merch01p')
conn = cx_Oracle.connect(user=r'zprmrcmotr', password='w28sdLMswa23$pQSRB', dsn=dsn_tns)

sql_Daily_MAX_TEMP="""
select PGE_DATE as TRADEDATE , max(ACTUAL_VALUE) DAILY_MAX_ACT
from MOTRVIEW.ACTUAL_HOURLY_PROFILES  b
where 9=9
and b.PGE_DATE between '25-JUN-2022' and '05-OCT-2022'
and b.ACTUAL_NUMBER=52
group by PGE_DATE
order by PGE_DATE
"""

sql_Temp="""
select a.*, b.actual_value from motrview.KPDX_TEMP  a

left join MOTRVIEW.ACTUAL_HOURLY_PROFILES  b
on a.PGE_DATE=b.PGE_DATE
and  a.HOUR_ENDING=b.HOUR_ENDING

where 9=9
and  a.PGE_DATE between '25-JUN-2022' and '05-OCT-2022'
and b.PGE_DATE between '25-JUN-2022' and '05-OCT-2022'
and b.ACTUAL_NUMBER=52
order by 5, 2, 3
"""

DF_TEMP = pd.read_sql_query(sql_Temp,conn)
Daily_MAX_TEMP=pd.read_sql_query(sql_Daily_MAX_TEMP,conn)
print(DF_TEMP)
#%%
print(Daily_MAX_TEMP)
#%%Join Temperature
DF_TEMP['PRESCHEDULE_ON_DATE']=DF_TEMP['Last_change'].dt.date
DF_TEMP['updated']=DF_TEMP['Last_change'].dt.floor('Min')
DF_TEMP['HH:MM']=DF_TEMP['updated'].dt.time 
DF_TEMP['FCST_HR']=DF_TEMP['updated'].dt.hour 
DF_TEMP1=DF_TEMP.drop(['FORECAST_NUMBER', 'Last_change'], axis=1)
DF_TEMP1['PRESCHEDULE_ON_DATE']=pd.to_datetime(DF_TEMP1['PRESCHEDULE_ON_DATE'])
#remove duplicates
DF_TEMP2=DF_TEMP1.drop_duplicates(subset=['PGE_DATE', 'HOUR_ENDING',  'PRESCHEDULE_ON_DATE'], keep='first')
#Comment for all hours
#DF_TEMP2=DF_TEMP2[DF_TEMP2.FCST_HR==4]
print(DF_TEMP2.tail(50))
#%%
#print(DF_TEMP2[DF_TEMP2['PRESCHEDULE_ON_DATE']=='2022-07-22'])
#%%
Max_FCST=DF_TEMP1['PRESCHEDULE_ON_DATE'].max()
Advisory=DF_TEMP2[DF_TEMP2['PRESCHEDULE_ON_DATE']==Max_FCST].groupby(['PGE_DATE'])['Forecast_value'].max().reset_index()
Advisory['BOM']=Advisory['PGE_DATE'].apply(lambda x: x.replace(day=1))
print(Advisory)
#%%
WECC_cal=pd.read_excel(r'D:\DASH\Excel_in\Preschedule calendar.xlsx', sheet_name='WECC')
WECC_peak=WECC_cal[WECC_cal.Peak==1]
print(WECC_peak.head(125))

#print(DF_TEMP2[DF_TEMP2.PGE_DATE=='2022-07-29'])
#%%
#print(DF_TEMP2.info())
TEMP_Cal=pd.merge(WECC_peak,DF_TEMP2, how='inner')
DF_TEMP3=TEMP_Cal.groupby(['PRESCHEDULE_ON_DATE','PGE_DATE', 'updated'])['Forecast_value'].max().reset_index()
DF_TEMP3.rename(columns={'PGE_DATE':'TRADEDATE', 'Forecast_value':'DAILY_MAX'},inplace=True)
print(DF_TEMP3)
#%%
conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                     "SERVER=JWTCVPMEDB08;"
                                     "DATABASE=Endur;"
                                     "trusted_connection=yes")
sql_MID_C="""
SELECT DISTINCT
[start_date] as TRADEDATE,
price as MiD_C_PK
FROM idx_historical_prices
where 1=1
and index_id=1020009
and start_date >= '2022-07-01'
order by 1 
"""

DF_Price0 = pd.read_sql_query(sql_MID_C,conn)
DF_Price1=pd.merge(DF_Price0, WECC_peak.rename(columns={'PGE_DATE':'TRADEDATE'}),how='left')
DF_Price_Temp=pd.merge(DF_Price1,DF_TEMP3.rename(columns={'PGE_DATE':'TRADEDATE'}), how='left')
DF_Price_ALL=pd.merge(DF_Price_Temp, Daily_MAX_TEMP, how='left')
DF_Price_ALL['DIFF_TEMP']=abs(DF_Price_ALL.DAILY_MAX.fillna(0)-DF_Price_ALL.DAILY_MAX_ACT)
DF_Price_ALL['DAILY_MAX1']=np.where(DF_Price_ALL['DIFF_TEMP']>15, DF_Price_ALL.DAILY_MAX_ACT, DF_Price_ALL.DAILY_MAX)
print(DF_Price_ALL)

#%%Loading data from Fundies. 
conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                     "SERVER=JWTCVPMEDB13;"
                                     "DATABASE=Fundamentals;"
                                     "trusted_connection=yes")


#%
sql_Morning="""
SELECT top (1) [TradeDate] as  TRADEDATE
      ,[RegionCode]
      ,[RegionID]
   
      ,max([AvgPrice]) as MiD_C_PK

      ,max([DateTimeStamp]) as Updated1
FROM [Fundamentals].[dbo].[ICE_MorningPowerPrices]
 
where [RegionID]=4
and TradeDate>GETDATE()-10
group by [TradeDate], [RegionCode], [RegionID]
ORDER BY 1 desc
"""


sql_MID_C_Forw='''
SELECT  [TradeDate]
      ,[RegionID]
      ,[FutureMonth]    as BOM 
      ,[AvgPrice] as MiD_C_PK
      ,[UserID]
      ,[DateTimeStamp]
  FROM [Fundamentals].[dbo].[ICE_MorningFuturePowerCurve]
  where RegionID=4 
  and TradeDate >=Getdate()-5
  order by 3, 1 desc
'''
#
DF_Morning = pd.read_sql_query(sql_Morning,conn)

DF_MiDC_forw = pd.read_sql_query(sql_MID_C_Forw,conn)

#%% Remove Duplicates 
DF_MiDC_forw.drop_duplicates(subset=['BOM'], keep='first',inplace=True)
print(DF_MiDC_forw)
#%%
DF_Price=DF_Price0#pd.concat([DF_Price0,DF_Morning  ], ignore_index=True, sort=False)

print(DF_Price)

#%%
DF=pd.merge(DF_Price,DF_TEMP3, how='left')
print(DF)

#%%
DF_forw=pd.merge(Advisory,DF_MiDC_forw, how='left')
DF_forw2=DF_forw[['PGE_DATE', 'Forecast_value','BOM', 'MiD_C_PK']].copy()
DF_forw2['PRESCHEDULE_ON_DATE']=dt.date.today()
DF_forw2_a=DF_forw2[DF_forw2['PGE_DATE']>pd.to_datetime(dt.date.today())]
DF_forw3=pd.merge(DF_forw2_a, WECC_peak['PGE_DATE'], how='inner' )
DF_forw3['Run_Type']='Advisory'
DF_forw3.rename(columns={'Forecast_value':'DAILY_MAX', 'PGE_DATE':'TRADEDATE'}, inplace=True)
#%%

#%%
DF.drop_duplicates(subset=['TRADEDATE'],keep='last', inplace=True)
DF.sort_values(by=['TRADEDATE'], inplace=True)
DF['Run_Type']='Actual'
DF['MiD_C_PK']=DF['MiD_C_PK'].fillna(method='bfill')
DF_clean=DF.dropna()

DF_export=pd.concat([DF,DF_clean,DF_forw3] , ignore_index=True, sort=False)
DF_export.drop_duplicates(subset=['TRADEDATE'],keep='last', inplace=True)
print(DF_export)
#%%
conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                     "SERVER=JWTCVPMEDB13;"
                                     "DATABASE=Fundamentals;"
                                     "trusted_connection=yes")
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                            "SERVER=JWTCVPMEDB13;"
                                             "DATABASE=Fundamentals;"
                                             "trusted_connection=yes")

engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

DF_export.to_sql('_Dual_Trig', engine, schema = 'dbo', index = False, if_exists='replace')


