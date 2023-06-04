import subprocess
import time
import pyodbc
subprocess.call("D:\Program\PYTHON_SERVER\Task\TSK\Wind_Fcst.py", shell=True)


time.sleep(10)

subprocess.call("D:\Program\PYTHON_SERVER\Task\TSK\Solar_Fcst.py", shell=True)

time.sleep(10)

subprocess.call("D:\Program\PYTHON_SERVER\Task\TSK\CBHYDRO_Fcst_HRLY.py", shell=True)
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
