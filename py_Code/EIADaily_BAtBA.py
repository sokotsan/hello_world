import requests
import datetime
import numpy as np
import json
import pandas
import urllib
from sqlalchemy import create_engine
import pyodbc
from urllib.request import urlopen as uReq

sort=False


#BA = ['SWPP','PSCO','AVRN','NW','AVA','PACW','PACE','PSEI','SCL','PGE','TPWR','CHPD','TEPC','AZPS','DOPD'
# ,'BPAT','SRP','GRMA','DEAA','HGMA','GRIF','WALC','IID','CFE','LDWP'
# ,'CISO','TIDC','BANC','WACM','NEVP','IPCO','WAUW','NWMT','WWA','GWA','GCPD'
# ,'GRID','CAL','SW','EPE','PNM','AESO','BCHA']




#PNW =['AVRN','AVA','PACW','PACE','PSEI','SCL','PGE','TPWR','CHPD','TEPC','AZPS','DOPD' ,'BPAT','NWMT'
#,'SRP','GRMA','DEAA','HGMA','GRIF','WALC','IID','CFE','LDWP'
# ,'CISO','TIDC','BANC','WACM','NEVP','IPCO','WAUW','WWA','GWA','GCPD'
# ,'GRID','CAL','SW','EPE','PNM','PSCO','NW']

conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                     "SERVER=JWTCVPMEDB13;"
                                     "DATABASE=Fundamentals;"
                                     "trusted_connection=yes")


DF_URL = pandas.read_sql_query('select * from dbo.EIA_BA_BA_list',conn)
URL = DF_URL.values.tolist()

x = 0 
start = 'id=EBA.'
end = '.ID.HL'
#URLTXT = "http://api.eia.gov/series/?api_key=e79d5730e424c57ad40929e31fc372e5&series_id=EBA.AVA-ALL.NG.WAT.HL"
#URLS = "http://api.eia.gov/series/?api_key=e79d5730e424c57ad40929e31fc372e5&series_id="


#URLTXT = "http://api.eia.gov/series/?api_key=e79d5730e424c57ad40929e31fc372e5&series_id=EBA.AVA-ALL.NG.NUC.HL"
#r =  requests.get(URLTXT)
#EIA = json.loads(r.content.decode('utf-8'))
#if EIA['series'][0]['data'] not in EIA:
#    print('Does Not Exist')
#else:
#    Print('data exists')


#E_DATA = EIA['series'][0]['data']

#EBA.AVA-ALL.NG.WAT.HL
#EBA.PACE-ALL.NG.COL.HL
#EBA.PACE-ALL.NG.WAT.HL
#EBA.PACE-ALL.NG.NG.HL
#EBA.PACE-ALL.NG.OTH.HL
#EBA.PACE-ALL.NG.SUN.HL
#EBA.PACE-ALL.NG.WND.HL
#EBA.PSEI-ALL.NG.OIL.HL
#EBA.BPAT-ALL.NG.NUC.HL

#EIA = json.loads(r.content.decode('utf-8'))
#E_DATA = EIA['series'][0]['data']

for a in range(0, len(URL)):
#for a in range(0,3):
    url=str(URL[a]).strip("['").strip("']")
    try:
        r =  requests.get(url)
        EIA = json.loads(r.content.decode('utf-8'))
    except:
        try:
            print(url+' SSL Failed')
            r =  requests.get(url, verify=False)
            EIA = json.loads(r.content.decode('utf-8'))
        except:
                print('ERROR-bad string 1'+url)
                pass
                
    try:
        E_DATA = EIA['series'][0]['data']
        for i in range(0,10):
            
            #for i in range(100,len(E_DATA)):
            strng = str(EIA['series'][0]['data'][i])
            st2=strng.split(',')
            S_Data = st2[1].replace(']','').strip()
            st3 = st2[0].replace("'","").replace("[","").strip().split('T')
            D_T = datetime.datetime.strptime(st3[0],'%Y%m%d')
            DT = D_T.date().strftime('%m/%d/%Y')
            st4 = st3[1].split('-')
            HE = st4[0]
            Loc = url[url.find(start)+len(start):url.find(end)]
            Loc2 = Loc.split('-')
            BA=Loc2[0]
            DS=Loc2[1]
            if x == 0:
                df = pandas.DataFrame({'DT':[DT],'HR':[HE],'DATA':[S_Data],'BA':[BA],'DataSet':[DS]})
                x = 1
            else:
                df1 = pandas.DataFrame({'DT':[DT],'HR':[HE],'DATA':[S_Data],'BA':[BA],'DataSet':[DS]})
    except:
        print('ERROR-bad string'+url)
                    
        
    params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                            "SERVER=JWTCVPMEDB13;"
                                             "DATABASE=Fundamentals;"
                                             "trusted_connection=yes")

    engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

    df.to_sql('EIABA_Landing', engine, schema = 'dbo', index = False, if_exists='replace')

    conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                           "DATABASE=Fundamentals;"
                                            "trusted_connection=yes")
    import time
    time.sleep(5)
    sql = 'EXEC dbo.UPLOAD_EIA_BA_BA'
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    conn.close()                                
    print(BA+' '+DS)
           # x = 0

      #  except:
           # print('ERROR-bad data'+PNW[a]+BA[b])

    

#params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
#                                         "SERVER=JWTCVPMEDB13;"
#                                         "DATABASE=Fundamentals;"
#                                         "trusted_connection=yes")

#engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

#df.to_sql('EIAHR_Landing', engine, schema = 'dbo', index = False, if_exists='replace')

#conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
#                                         "SERVER=JWTCVPMEDB13;"
#                                         "DATABASE=Fundamentals;"
#                                         "trusted_connection=yes")


        
