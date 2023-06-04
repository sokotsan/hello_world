
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

dsn_tns = cx_Oracle.makedsn('xpmedbe01', '1521', service_name='merch01p')
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
DF_TEMP['PRESCHEDULE_ON_DATE']=DF_TEMP['Last_change'].dt.date
DF_TEMP['updated']=DF_TEMP['Last_change'].dt.floor('Min')
DF_TEMP['HH:MM']=DF_TEMP['updated'].dt.time 
DF_TEMP1=DF_TEMP.drop(['FORECAST_NUMBER', 'Last_change'], axis=1)
#remove duplicates
DF_TEMP2=DF_TEMP1.drop_duplicates(subset=['PGE_DATE', 'HOUR_ENDING',  'PRESCHEDULE_ON_DATE', 'updated', 'HH:MM'], keep='first')
print(DF_TEMP2)
#%%
WECC_cal=pd.read_excel(r'D:\DASH\Excel_in\Preschedule calendar.xlsx', sheet_name='WECC')
print(WECC_cal.head(10))
#%%

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

      ,max([DateTimeStamp]) as Updated
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

#
DF_Morning = pd.read_sql_query(sql_Morning,conn)
DF_Price0 = pd.read_sql_query(sql_MID_C,conn)

#%%
DF_Price=DF_Morning.append(DF_Price0, sort=True )
print(DF_TEMP2)
#%%
DF=pd.merge(DF_Price0,DF_TEMP2.rename(columns={'TRADEDATE':'PGE_DATE'},inplace=True))
print(DF)
#%%
DF.drop_duplicates(subset=['TRADEDATE'],keep='last', inplace=True)
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

DF.to_sql('_Dual_Trig', engine, schema = 'dbo', index = False, if_exists='replace')


