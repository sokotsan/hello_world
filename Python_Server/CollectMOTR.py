from sklearn import linear_model
import urllib
from sqlalchemy import create_engine
import pyodbc
import pandas as pd
from sklearn.metrics import r2_score
import matplotlib.pyplot as plt
import sys
#import numpy as np

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
print(sys.version)
import cx_Oracle

try:
    cx_Oracle.init_oracle_client(lib_dir=r"D:\Oracle\instantclient_21_6")
except:
    print('Oracle Client intialized')    



dsn_tns = cx_Oracle.makedsn('xpmedbe02', '1521', service_name='merch01p')
conn = cx_Oracle.connect(user=r'zprmrcmotr', password='w28sdLMswa23$pQSRB', dsn=dsn_tns)


DF_LC = pd.read_sql_query("select PGE_DATE, Hour_Ending, Forecast_Value,Modified_TimeSTAMP as Timestamp,'DOPD_CHJ_Fcst' as Data_Name from MOTRVIEW.Forecast_Hourly_Profiles b join MOTRVIEW.FORECASTS a on a.FORECAST_NUMBER = b.FORECAST_NUMBER where a.Forecast_NAME ='DOPD Wells Inflow KCFS Frcst' and PGE_Date between sysdate-2 and sysdate+7",conn)
#%%
#'PGE_Date', 'Hour_Ending', 'Forecast_Value', 'Timestamp', 'Data_Name']

params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

DF_LC.to_sql('DOPD_HY_Fcst_Landing', engine, schema = 'dbo', index = False, if_exists='replace')



conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

sql = 'EXEC dbo.DOPB_HY_Fcst_Upld'

cursor = conn.cursor()
cursor.execute(sql)
conn.commit()
conn.close()



#DF_LC = pd.read_sql_query('select distinct LC from dbo.solar_data',conn)
#LC = DF_LC.values.tolist()

#CN <- odbcDriverConnect("Driver={Microsoft ODBC for Oracle};
#                        Server=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=xwtcvpmedbe04)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=merch01p)));
                       
#                        Uid=zprmrcmotr;
#                        Pwd=w28sdLMswa23$pQSRB;"
#                        ,readOnlyOptimize = TRUE)
