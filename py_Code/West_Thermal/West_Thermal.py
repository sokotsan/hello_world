import subprocess
import time
import pyodbc
subprocess.call("D:\DASH\py_Code\West_Thermal\Wind_Fcst.py", shell=True)


time.sleep(10)

subprocess.call("D:\DASH\py_Code\West_Thermal\Solar_Fcst.py", shell=True)

time.sleep(10)

subprocess.call("D:\DASH\py_Code\West_Thermal\CBHYDRO_Fcst_HRLY.py", shell=True)
time.sleep(10)

conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                        "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                          "trusted_connection=yes")

sql = 'EXEC dbo.Upload_west_thermal'
cursor = conn.cursor()
cursor.execute(sql)
conn.commit()
conn.close()    

print('Sucessful Run')