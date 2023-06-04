from sklearn import linear_model
import urllib
from sqlalchemy import create_engine
import pyodbc
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
import time
#exec dbd.prep_solardata

#params = urllib.parse.quote_plus("DRIVER={Microsoft ODBC for Oracle};"
#                                         "SERVER=xwtcvpmedbe04;"
#                                         "DATABASE=Fundamentals;"
#                                         "trusted_connection=yes")

#conn = pyodbc.connect("DRIVER={Microsoft ODBC for Oracle};"
#                                         "SERVER=xwtcvpmedbe04;"
#                                         "DATABASE=merch01p;"
#                                       "Uid=zprmrcmotr;"
#"Pwd=w28sdLMswa23$pQSRB;")

dsn_tns = cx_Oracle.makedsn('xpmedbe01', '1521', service_name='merch01p')
conn = cx_Oracle.connect(user=r'zprmrcmotr', password='w28sdLMswa23$pQSRB', dsn=dsn_tns)

sql_1='''SELECT
DEAL_NUMBER,
t.BOOK,
t.MARKET1,
t.ZONE1,
TRANSACTION_TYPE,
TRANSACTION_TYPE || DEAL_TYPE,
HP.PGE_DATE,
HP.HOUR_ENDING,
CASE WHEN TRANSACTION_TYPE = 'S' THEN 0 - MWH ELSE MWH end as MWH,
case when deal_type = 'Index' then idx.index_value 
                        * COALESCE(SALE_INDEX_FORMULA_MULT, PURCH_INDEX_FORMULA_MULT, SALE_INDEX_OFFPK_FORMULA_MULT,PURCH_INDEX_OFFPK_FORMULA_MULT) 
                        + COALESCE(SALE_INDEX_FORMULA_ADD, PURCH_INDEX_FORMULA_ADD, SALE_INDEX_OFFPK_FORMULA_ADD, PURCH_INDEX_OFFPK_FORMULA_ADD)  else PRICE end as deal_price                        
FROM MOTRVIEW.POWER_DEAL_HOURLY_PROFILES HP
INNER JOIN MOTRVIEW.POWER_DEALS t USING (DEAL_NUMBER)
left join (select index_name, market_type, component, pge_date, hour_ending, index_value 
          from MOTRVIEW.PRICE_INDEX_HOURLY_PROFILES idxhp, MOTRVIEW.PRICE_INDICES idxn 
          where idxhp.INDEX_NUMBER = idxn.index_id 
          and  PGE_DATE between to_date('07/01/2021','mm/dd/yyyy') and to_Date('09/30/2021','mm/dd/yyyy')) idx
          on hp.pge_date = idx.pge_date and hp.hour_ending = idx.hour_ending 
          and COALESCE(sale_index, purch_index, SALE_INDEX_OFFPK, PURCH_INDEX_OFFPK) = idx.index_name 
          and COALESCE(sale_index_component, purch_index_component, SALE_INDEX_OFFPK_COMPONENT, PURCH_INDEX_OFFPK_COMPONENT) = idx.component 
          and COALESCE(t.SALE_INDEX_MARKET_TYPE, t.purch_index_market_type, t.SALE_INDEX_OFFPK_MARKET_TYPE, t.PURCH_INDEX_OFFPK_MARKET_TYPE) = idx.market_type
WHERE STATUS != 'VOIDED'
--AND BOOK in ('PGEM/Power/Pre-Schedule', 'PGEM/Power/Real-Time')
AND FINANCIAL_PHYSICAL = 'P'
AND HP.PGE_DATE between to_date('06/01/2021','mm/dd/yyyy') 
AND to_Date('09/30/2021','mm/dd/yyyy')
'''

sql_2='''SELECT b.*, a.*
FROM MOTRVIEW.PRESCHED_POS_DAILY_PROFILES  a, 
MOTRVIEW.preschedule_positions b
where  pge_date between to_date('06/01/2021','mm/dd/yyyy') and to_date('9/30/2021','mm/dd/yyyy')
and a.POSITION_NUMBER =b.POSITION_NUMBER
ORDER BY 2, 3
'''

DF_1 = pd.read_sql_query(sql_1,conn)
DF_2 = pd.read_sql_query(sql_2,conn)
#%%
DF3=DF_2[DF_2['VALUE_TYPE']=='MWh']
DF4=pd.melt(DF3, id_vars=['PGE_DATE','POSITION_NAME']
, value_vars=['HE01', 'HE02', 'HE03', 'HE04', 'HE05', 'HE06','HE07', 'HE08', 'HE09', 'HE10', 'HE11', 'HE12'
              ,'HE13', 'HE14', 'HE15', 'HE16', 'HE17', 'HE18','HE19', 'HE20', 'HE21', 'HE22', 'HE23', 'HE24'], value_name='HE')
DF4.tail()
#%%

#'PGE_Date', 'Hour_Ending', 'Forecast_Value', 'Timestamp', 'Data_Name']

params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

#%%
DF_1.to_sql('_TEST1_Q3', engine, schema = 'dbo', index = False, if_exists='replace')
DF_2.to_sql('_TEST2_Q3', engine, schema = 'dbo', index = False, if_exists='replace')
#%%
DF4.to_sql('_TEST3_Q3', engine, schema = 'dbo', index = False, if_exists='replace')