import time
import requests
import pyodbc
from sqlalchemy import create_engine
import pandas as pd
import urllib
import win32com.client
import os
import time
import datetime
from io import StringIO
username = 'portlandgeneralelectric'
password = 'settledata'
yr=['2017']
mn=['12']
#['01','02','03','04','05','06','07','08','09','10','11','12']
dn=['28']
#['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31']
path = 'https://downloads.theice.com/EOD_Gas_CSV/gasindices_'
#https://downloads.theice.com/Settlement_Reports_CSV/Gas/icecleared_gas_2016_01_01.dat
#'https://downloads.theice.com/Settlement_Reports_CSV/Gas/icecleared_gas_2016_01_01.dat'
#FilePath= '\\\JWTCVDMEDB03\\Fundamentals\\ICEPower\\Stefan Fix'
#Filepathtst = '//JWTCVDMEDB03/Fundamentals/ICEPower/Stefan Fix/Power_2019_08_06.txt'
#https://downloads.theice.com/Settlement_Reports_CSV/Power/icecleared_power_2018_08_09.dat

#https://downloads.theice.com/EOD_Gas_CSV/gasindices_2017_12_28.dat

a = []
b = [] 
#a.append(os.listdir(FilePath1))
#data = 
for y in range(len(yr)):
    YR = yr[y]
    for m in range(len(mn)):
        MN = mn[m]
        time.sleep(7)
        for d in range(len(dn)):
            try:
                DN = dn[d]
                dtt = str(YR)+'_'+str(MN)+'_'+str(DN)+'.dat'
                url = path+dtt
                
                #url = 'https://downloads.theice.com/Settlement_Reports_CSV/Gas/icecleared_gas_2016_04_29.dat'
                
                result = requests.get(url, auth=(username, password)).content
                s=str(result,'utf-8')
                data = StringIO(s)
                df = pd.read_csv(data,
                   sep="|")

                params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                                 "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

                conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

                engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

                #df.to_sql('ICEPower_Landing', engine, schema = 'dbo', index = False, if_exists='replace')
                #time.sleep(5)
                #sql = 'EXEC dbo.IcePower_Import'

               # cursor = conn.cursor()
                #cursor.execute(sql)
               # conn.commit()
               # time.sleep(2)
               # conn.close()
               # print(dtt)
            except:
                dtt = str(YR)+'_'+str(MN)+'_'+str(DN)
                print(dtt+' FAILED')
                time.sleep(2)
