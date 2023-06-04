
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
#%%   TEMPERATURE LOADING....
#DF_URL = pandas.read_sql_query('select * from dbo.EIA_BA_NG_List',conn)

dsn_tns = cx_Oracle.makedsn('xpmedbe02', '1521', service_name='merch01p')
conn = cx_Oracle.connect(user=r'zprmrcmotr', password='w28sdLMswa23$pQSRB', dsn=dsn_tns)


sql_Latest_TEMP="""
select PGE_DATE
, MAX("Forecast_value") as High_Temp 
, MIN("Forecast_value") as Low_Temp
from motrview.KPDX_TEMP 
where to_char(current_date)=to_char(CAST(("Last_change") as DATE))
and TO_CHAR("Last_change", 'HH24')='04'
and PGE_DATE>=current_date
and PGE_DATE<=current_date+5
group by PGE_DATE
order by 1
"""
TEMP_forw=pd.read_sql_query(sql_Latest_TEMP,conn)
print(TEMP_forw)
#%%
#print(Daily_MAX_TEMP)

#%% Join ACTUAL and Forecast Temperature 
DF_TEMP['PRESCHEDULE_ON_DATE']=DF_TEMP['Last_change'].dt.date
DF_TEMP['updated']=DF_TEMP['Last_change'].dt.floor('Min')
DF_TEMP['HH:MM']=DF_TEMP['updated'].dt.time 
DF_TEMP['FCST_HR']=DF_TEMP['updated'].dt.hour 
DF_TEMP1=DF_TEMP.drop(['FORECAST_NUMBER', 'Last_change'], axis=1)
DF_TEMP1['PRESCHEDULE_ON_DATE']=pd.to_datetime(DF_TEMP1['PRESCHEDULE_ON_DATE'])
#remove duplicates
DF_TEMP2=DF_TEMP1.drop_duplicates(subset=['PGE_DATE', 'HOUR_ENDING',  'PRESCHEDULE_ON_DATE'], keep='first')
#%%


#%% WRONG
#Max_FCST=DF_TEMP1['PRESCHEDULE_ON_DATE'].max()
#Advisory=DF_TEMP2[DF_TEMP2['PRESCHEDULE_ON_DATE']==Max_FCST].groupby(['PGE_DATE'])['Forecast_value'].max().reset_index()
#Advisory['BOM']=Advisory['PGE_DATE'].apply(lambda x: x.replace(day=1))
#print(Advisory)
#%%
WECC_cal=pd.read_excel(r'D:\DASH\Excel_in\Preschedule calendar.xlsx', sheet_name='WECC')
WECC_peak=WECC_cal[WECC_cal.Peak==1]
print(WECC_peak.head(130))
WECC_peak1=WECC_peak[WECC_peak['PRESCHEDULE_ON_DATE']<=ymd_str].rename(columns={'PGE_DATE':'TRADEDATE'})
#print(DF_TEMP2[DF_TEMP2.PGE_DATE=='2022-07-29'])
#%%
#print(DF_TEMP2.info())
TEMP_Cal=pd.merge(WECC_peak,DF_TEMP2, how='left')
DF_TEMP3=TEMP_Cal.groupby(['PRESCHEDULE_ON_DATE','PGE_DATE', 'updated'])['Forecast_value'].max().reset_index()
DF_TEMP3.rename(columns={'PGE_DATE':'TRADEDATE', 'Forecast_value':'DAILY_MAX'},inplace=True)
DF_TEMP_sort=DF_TEMP3.sort_values(['PRESCHEDULE_ON_DATE', 'updated'])
DF_TEMP4=DF_TEMP_sort.drop_duplicates(subset=['PRESCHEDULE_ON_DATE','TRADEDATE'], keep='first')
print(DF_TEMP4)
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
DF_Price1=pd.merge(DF_Price0, WECC_peak1, how='right')
DF_Price_Temp=pd.merge(DF_Price1,DF_TEMP4, on=['PRESCHEDULE_ON_DATE', 'TRADEDATE'] ,how='outer')
DF_Price_ALL=pd.merge(DF_Price_Temp, Daily_MAX_TEMP, how='left')
#%%
print(DF_Price_ALL)
#%%

#Loading data from Fundies. 
conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                     "SERVER=JWTCVPMEDB13;"
                                     "DATABASE=Fundamentals;"
                                     "trusted_connection=yes")

#%
sql_Morning="""
SELECT top (1) [TradeDate] as  PRESCHEDULE_ON_DATE
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
      ,[FutureMonth]    as MON_YR 
      ,[AvgPrice] as MiD_C_PK
      ,[UserID]
      ,[DateTimeStamp]
  FROM [Fundamentals].[dbo].[ICE_MorningFuturePowerCurve]
  where RegionID=4 
  and TradeDate >=Getdate()-5
  order by 3, 1 desc
'''
#

DF_MiDC_forw = pd.read_sql_query(sql_MID_C_Forw,conn)


#% Remove Duplicates 
DF_MiDC_forw.drop_duplicates(subset=['MON_YR'], keep='first',inplace=True)
DF_Morning = pd.read_sql_query(sql_Morning,conn)


#%%JOIN Advisory forecast
print(DF_MiDC_forw)
TEMP_forw['MON_YR']=TEMP_forw['PGE_DATE'].apply(lambda x: x.replace(day=1))
BOM=pd.merge(TEMP_forw, DF_MiDC_forw[['MON_YR', 'MiD_C_PK']], how='left', on='MON_YR')
BOM1=pd.merge(BOM.rename(columns={'PGE_DATE':'PRESCHEDULE_ON_DATE'}), WECC_peak, how='inner')
BOM2=BOM1[['PRESCHEDULE_ON_DATE', 'DAILY_MAX', 'MON_YR', 'MiD_C_PK', 'PGE_DATE']].rename(columns={'PGE_DATE':'TRADEDATE'})
#print(TEMP_forw)
print(BOM2)




#%%
#DF_sub.rename(columns={'DAILY_MAX1':'DAILY_MAX'}, inplace=True)
DF_sub=DF_Price_ALL[['TRADEDATE', 'MiD_C_PK', 'PRESCHEDULE_ON_DATE','DAILY_MAX', 'updated']]
DF_sub['MiD_C_PK'][DF_sub['PRESCHEDULE_ON_DATE']==DF_Morning['PRESCHEDULE_ON_DATE'][0]]=DF_Morning['MiD_C_PK'][0]
DF_sub['MON_YR']=DF_sub['TRADEDATE'].apply(lambda x: x.replace(day=1))
DF_sub['DAILY_MAX'][DF_sub['TRADEDATE']=='2022-07-25']=93.0
#DF_sub['DAILY_MAX'][DF_sub['TRADEDATE']=='2022-07-26']=100.0
#DF_sub['DAILY_MAX'][DF_sub['TRADEDATE']=='2022-07-28']=99.0


#%%
DF_sub.drop_duplicates(subset=['TRADEDATE'],keep='first', inplace=True)

DF_export=pd.concat([DF_sub, BOM2], ignore_index=True, sort=False)
print(DF_export)
#%%
#print(DF_export)
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


