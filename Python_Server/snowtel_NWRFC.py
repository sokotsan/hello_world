from urllib.request import urlopen as uReq
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import datetime
import webbrowser
import urllib
from sqlalchemy import create_engine
import pyodbc
import urllib3
import time
urllib3.disable_warnings()
DT = datetime.datetime.today()
MN = DT.month
if len(str(MN)) == 1:
    MN = '0'+str(MN)
else:
    pass

url = "https://wcc.sc.egov.usda.gov/nwcc/view"

conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

locnum=pd.read_sql_query("select distinct Ltrim(Rtrim(str(SiteID))) as 'Number' from dbo.NWRFC_Snotel a join LKP_NWRFC_Snotel_Measure b on a.MeasureID=b.MeasureID join dbo.Snotel_loc c on c.Number = a.SiteID where c.Lat <>'' ", conn)

#locnum=['301','302']
#locnum.Name.iloc[1]
x=0

#yr = ['20']
#yr = ['01']

for a in range(0,len(locnum)):
#for a in range(1,2):    
    time.sleep(3)
    try:
        while x<3:
            try:
                payload = {
                        'intervalType':'+View+Current+',
                        #'report':'STAND'
                        #'timeseries':'Daily'
                        #'format':'view'
                        'sitenum':locnum.Number.iloc[a],
                        'report':'STAND',
                       'timeseries':'Daily',
                       'interval':'Month',
                       'month':MN,
                       'temp_unit':'8',
                       'format':'copy',
                       'userEmail':''}
                r = requests.post(url, payload, verify=False, timeout = 10)
                soup = BeautifulSoup(r.text, 'html.parser')
                string=str(soup)
                lst = string.split('\n')
                Col = lst[4].split(',')
                Col = [w.replace('.','_') for w in Col]
                Col = [w.replace(' ','_') for w in Col]
                df=pd.DataFrame([],columns=Col)
                for d in Col:
                    if(d == ''):
                        df=df.drop(columns=[''])
                for i in range(5,len(lst)):
                    if len(lst[i]) > 1:
                        df2=pd.DataFrame([lst[i].split(',')],columns=Col)
                        for b in Col:
                            if(b == ''):
                                df3=df2.drop(columns=[''])
                        df=df.append(df3, sort = False)
                #D_Col=list(df.columns.values)
                df4=pd.melt(df,id_vars=['Site_Id','Date','Time'])
                    
                x=0
                break
                     #   df=df[['Site_Id', 'Date', 'Time', 'WTEQ_I-1_(in)_', 'PREC_I-1_(in)_', 'TOBS_I-1_(degC)_', 'TMAX_D-1_(degC)_', 'TMIN_D-1_(degC)_', 'TAVG_D-1_(degC)_', 'SNWD_I-1_(in)_']]
            except:
                x=x+1
                print(str(locnum.Number.iloc[a])+' Try '+str(x+1))
                if x ==3:
                      print(str(locnum.Number.iloc[a])+' Failed')
                      time.sleep(3)
                      x=0
                      break
            
           # print('Here')
        params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                                 "SERVER=JWTCVPMEDB13;"
                                                 "DATABASE=Fundamentals;"
                                                 "trusted_connection=yes")
            #print('Here 1')
        engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
            #print('Here 2')
        
        df4.to_sql('Snotel_Landing', engine, schema = 'dbo', index = False, if_exists='replace')
            #print('Here 3')
        time.sleep(1)
            #print('Here 4')
        conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                                "SERVER=JWTCVPMEDB13;"
                                              "DATABASE=Fundamentals;"
                                                 "trusted_connection=yes")
            #print('Here 5')
        
        sql = 'EXEC dbo.Upload_NWRCF_Snotel'
            #print('Here 6')
        
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        conn.close()
        print (str(locnum.Number.iloc[a]))
            #print('Done')
    except:
        print('FAILED_UPLOAD '+str(locnum.Number.iloc[a]))

            #locnum.Number.iloc[a]

