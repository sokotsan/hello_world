from sqlalchemy import create_engine
import pyodbc
#params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
#                                         "SERVER=JWTCVPMEDB03;"
#                                         "DATABASE=Fundamentals;"
#                                         "trusted_connection=yes")
conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")
#engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

#NWRFCdf.to_sql('', engine, schema = 'dbo', index = False, if_exists='replace')

sql = 'EXEC HY_Hist_Gen'

cursor = conn.cursor()
cursor.execute(sql)
conn.commit()
conn.close()
