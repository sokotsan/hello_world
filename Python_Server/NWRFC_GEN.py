import pyodbc
import time
conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                      "trusted_connection=yes")
sql = 'EXEC dbo.Upload_HY_10day_Gen'

cursor = conn.cursor()
cursor.execute(sql)
time.sleep(15)
conn.commit()
time.sleep(15)
conn.close()
