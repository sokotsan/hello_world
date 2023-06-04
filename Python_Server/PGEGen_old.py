from sklearn import linear_model
import urllib
from sqlalchemy import create_engine
import pyodbc
import pandas as pd
from sklearn.metrics import r2_score
import matplotlib.pyplot as plt
import numpy as np
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

dsn_tns = cx_Oracle.makedsn('xpmedbe02', '1521', service_name='merch01p')
conn = cx_Oracle.connect(user=r'zprmrcmotr', password='w28sdLMswa23$pQSRB', dsn=dsn_tns)
print('correct')

DF_LC = pd.read_sql_query("""select PGE_Date, Hour_Ending,Actual_Value as Gen, Calculation_Name as Plant, a.Calculation_Number as Code
 from MOTRVIEW.Calculation_HOURLY_PROFILES d
 join MOTRVIEW.Calculations a on a.Calculation_NUMBER=d.Calculation_NUMBER
  where a.calculation_Number = 38
  and PGE_Date between sysdate-20 and sysdate
  union all 
  select PGE_Date, Hour_Ending, Actual_Value as Gen, Actual_Name, a.Actual_Number 
 from MOTRVIEW.ACTUAL_HOURLY_PROFILES d
join MOTRVIEW.ACTUALs a on a.ACTUAL_NUMBER=d.ACTUAL_NUMBER
 WHERE a.actual_number in (51,90,98,105,114,119,128,129,137,138,21,28)
  AND PGE_Date between sysdate-180 and sysdate""",conn)
print('datacollected')
#%%

#'PGE_Date', 'Hour_Ending', 'Forecast_Value', 'Timestamp', 'Data_Name']

params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

DF_LC.to_sql('BA_GEN_MOTR', engine, schema = 'dbo', index = False, if_exists='replace')



conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

sql = 'EXEC dbo.PGE_GEN_Motr_Upload'

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
