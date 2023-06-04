import requests
import datetime

import urllib
from sqlalchemy import create_engine
import pyodbc

sort=False
month = datetime.datetime.now().month
year = datetime.datetime.now().year
day = datetime.datetime.now().day 
i=1
err=0
if day < 10:Day="0"+str(day)
else:Day=str(day)

if month < 10:Month="0"+str(month)
else:Month=str(month)
Year = str(year)[2:4]

Date = Month+"/"+Day+"/"+Year
Cycle = i
#if month in (1,3,5,7,8,10,12):daysinmonth = 31
#elif month in (2,13):daysinmonth = 28
#else:daysinmonth = 30
import numpy as np
import json
import pandas
dict_of_df = {}
for i in range(1,6):
    try:
        Cycle = str(i)
        Key = 'GTN'+str(i)
        URLTXT = "http://tcplus.com/GTN/OperationalCapacity/Generate?filter.GasDay="+Date+"&filter.CycleType="+Cycle+"&page=1&sort=LocationName&sort_direction=undefined"
        r =  requests.get(URLTXT)
        #body = response.read(r).decode('utf-8')
        GTN= json.loads(r.content.decode('utf-8'))
        GTN_GasDay = GTN['data']['EffectiveGasDay']
        GTN_PostingDate = GTN['data']['PostingDate']
        GTN_Cycle = GTN['data']['Cycle']
        dict_of_df[Key]= pandas.DataFrame(GTN['data']['Content'])
        dict_of_df[Key]['GasDay'] = GTN_GasDay
        dict_of_df[Key]['PostingDate'] = GTN_PostingDate
        dict_of_df[Key]['Pipe'] = 'GTN'
        dict_of_df[Key]['Cycle'] = GTN_Cycle
    except:
        import webbrowser
        import time
        err = err+1
        if err > 3:
            break
        else:
            ie = webbrowser.get(webbrowser.iexplore)
            ie.open('http://tcplus.com/GTN/OperationalCapacity')
            print('Restart Attempted')
            time.sleep(10)
            pass
#frames = [dict_of_df['GTN1'],dict_of_df['GTN2'],dict_of_df['GTN3'],dict_of_df['GTN4']]

frames = [dict_of_df['GTN1'],dict_of_df['GTN2'],dict_of_df['GTN3'],dict_of_df['GTN4'],dict_of_df['GTN5']]
GTNdf = pandas.concat(frames)[['AllQtyAvailable', 'DesignCapacity', 'FlowIndicatorDescription', 'IT', 'LocQti', 'LocationID', 'LocationName', 'LocationPurposeDescription', 'OperatingCapacity', 'OperationallyAvailableCapacity', 'TotalScheduledQuantity', 'GasDay', 'PostingDate', 'Pipe', 'Cycle']]
#SavePath= '\\\JWTCVDMEDB03\\Fundamentals\\Gas Scrapes\\'
#PipeName = 'GTN_'
#SaveDay=str(day)
#SaveMonth= str(month)
#SaveYear=str(year)
#SaveName = SavePath+PipeName+SaveDay+'_'+SaveMonth+'_'+SaveYear+'.txt'
#print(SaveName)
#np.savetxt(SaveName, GTNdf.values, fmt='%s',delimiter="\t")

params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

GTNdf.to_sql('GTN_Landing', engine, schema = 'dbo', index = False, if_exists='replace')

conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

sql = 'EXEC dbo.GTN_Upload'

cursor = conn.cursor()
cursor.execute(sql)
conn.commit()
conn.close()
