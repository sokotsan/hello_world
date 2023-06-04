
import urllib
import pandas as pd
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

dsn_tns = cx_Oracle.makedsn('xpmedbe02', '1521', service_name='merch01p')
conn = cx_Oracle.connect(user=r'zprmrcmotr', password='w28sdLMswa23$pQSRB', dsn=dsn_tns)

sql_Tem_old="""
select PGE_DATE as TRADEDATE, MAX("Forecast_value") as Daily_Max from motrview.KPDX_TEMP 
where to_char(PGE_DATE)=to_char(CAST(("Last_change") as DATE))
group by PGE_DATE
"""

sql_Temp="""
select * from motrview.KPDX_TEMP 
where PGE_DATE between '25-JUN-2022' and '05-OCT-2022'
order by 5, 2, 3
"""

DF_TEMP = pd.read_sql_query(sql_Temp,conn)
print(DF_TEMP)
#%%

#%%
DF_TEMP['PRESCHEDULE_ON_DATE']=DF_TEMP['Last_change'].dt.date
DF_TEMP['updated']=DF_TEMP['Last_change'].dt.floor('Min')
DF_TEMP['HH:MM']=DF_TEMP['updated'].dt.time 
DF_TEMP['FCST_HR']=DF_TEMP['updated'].dt.hour 
DF_TEMP1=DF_TEMP.drop(['FORECAST_NUMBER', 'Last_change'], axis=1)
DF_TEMP1['PRESCHEDULE_ON_DATE']=pd.to_datetime(DF_TEMP1['PRESCHEDULE_ON_DATE'])
#remove duplicates
DF_TEMP2=DF_TEMP1.drop_duplicates(subset=['PGE_DATE', 'HOUR_ENDING',  'PRESCHEDULE_ON_DATE'], keep='first')
DF_TEMP2=DF_TEMP2[DF_TEMP2.FCST_HR==4]
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
#Loading data from Fundies. 
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
sql_MID_C="""
SELECT  [RegionID]
      ,[RegionCode]

      ,[TradeDate] as  TRADEDATE

      ,[Price] as MiD_C_PK
  FROM [Fundamentals].[dbo].[vw_ICE_POWER_GAS_DAILY_PRICE]
  where RegionID=4
  and TradeDate>='2022-06-01'
  and TradeDate<GETDATE()
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
DF_Price0 = pd.read_sql_query(sql_MID_C,conn)
DF_MiDC_forw = pd.read_sql_query(sql_MID_C_Forw,conn)

#%% Remove Duplicates 
DF_MiDC_forw.drop_duplicates(subset=['BOM'], keep='first',inplace=True)
print(DF_MiDC_forw)

#%%
print(DF_Morning)

#%%
DF_Price=DF_Morning.append(DF_Price0, sort=True )

DF=pd.merge(DF_TEMP3,DF_Price, how='left')
DF_forw=pd.merge(Advisory,DF_MiDC_forw, how='left')
DF_forw2=DF_forw[['PGE_DATE', 'Forecast_value','BOM', 'MiD_C_PK']].copy()
DF_forw2['PRESCHEDULE_ON_DATE']=dt.date.today()
DF_forw2_a=DF_forw2[DF_forw2['PGE_DATE']>pd.to_datetime(dt.date.today())]
DF_forw3=pd.merge(DF_forw2_a, WECC_peak['PGE_DATE'], how='inner' )
DF_forw3['Run_Type']='Advisory'
DF_forw3.rename(columns={'Forecast_value':'DAILY_MAX', 'PGE_DATE':'TRADEDATE'}, inplace=True)
print(DF_forw3)
#%%
DF.drop_duplicates(subset=['TRADEDATE'],keep='last', inplace=True)
DF.sort_values(by=['TRADEDATE'], inplace=True)
DF['Run_Type']='Actual'
DF['MiD_C_PK']=DF['MiD_C_PK'].fillna(method='bfill')
DF_clean=DF.dropna()

DF_export=pd.concat([DF_clean, DF_forw3], ignore_index=True, sort=False)
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

#DF_export.to_sql('_Dual_Trig', engine, schema = 'dbo', index = False, if_exists='replace')


