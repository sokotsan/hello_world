import requests
import datetime
import numpy as np
import json
import pandas
import urllib
from sqlalchemy import create_engine
import pyodbc
from urllib.request import urlopen as uReq
import time
sort=False


BA =['AVRN','NW','AVA','PACW','PACE','PSEI','SCL','PGE','TPWR','CHPD','TEPC','AZPS','DOPD','PSCO'
 ,'BPAT','SRP','GRMA','DEAA','HGMA','GRIF','WALC','IID','CFE','LDWP'
 ,'CISO','TIDC','BANC','WACM','NEVP','IPCO','WAUW','NWMT','WWA','GWA','GCPD'
 ,'GRID','CAL','SW','EPE','PNM']
FT = ['NG.NUC','NG.OIL','NG.WND','NG.SUN','NG.OTH','NG.NG','NG.COL','NG.WAT','DF','TI','D',]

#['AVRN','NW','AVA','PACW','PACE','PSEI','SCL','PGE','TPWR','CHPD','TEPC','AZPS','DOPD','PSCO'
# ,'BPAT','SRP','GRMA','DEAA','HGMA','GRIF','WALC','IID','CFE','LDWP'
# ,'CISO','TIDC','BANC','WACM','NEVP','IPCO','WAUW','NWMT','WWA','GWA','GCPD'
# ,'GRID','CAL','SW','EPE','PNM']

start = 'id=EBA.'
end = '.HL'

#BA = ['NW']
#,'BANC','CAL','CISO','DOPD','GCPD','NEVP','CHPD','PGE','PGE','PACW','BPAT','NW','NWMT','PACE','PSCO','PSEI','TIDC','AVA']
#FT= ['D']
     #,'NG.WAT']

x = 0 

conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                     "SERVER=JWTCVPMEDB13;"
                                     "DATABASE=Fundamentals;"
                                     "trusted_connection=yes")

DF_URL = pandas.read_sql_query('select * from dbo.EIA_BA_NG_list',conn)
URL = DF_URL.values.tolist()

for a in range(0, len(URL)):
#for a in range(0, 2):
    url=str(URL[a]).strip("['").strip("']")
    #print(url)
    
    #for b in range(0,len(FT)):
     #   ft=FT[b]
      #  SeriesID ='EBA.'+BA[a]+'-ALL.'+FT[b]+'.HL'
       # URL = URLS+SeriesID
        #uClient = uReq(URL)
        #page_html = uClient.read()
        #uClient.close()
    try:
        r =  requests.get(url, verify=False , timeout=5)
       # time.sleep(2)
        #r =  requests.get(URL, verify=False)
        EIA = json.loads(r.content.decode('utf-8'))
    except:
        print(url+' SSL Failed')
        #df=df.loc[0:0]
        EIA.clear()
        try:
            r =  requests.get(url, verify=False , timeout=10)
            #time.sleep(2)
            EIA = json.loads(r.content.decode('utf-8'))
            print('Recover')
        except:
            print(url+' SSL Failed 2')
    try:
        E_DATA = EIA['series'][0]['data']
        for i in range(0,168):
                                      #for i in range(100,len(E_DATA)):
            try:
                strng = str(EIA['series'][0]['data'][i])
                st2=strng.split(',')
                S_Data = st2[1].replace(']','').strip()
                st3 = st2[0].replace("'","").replace("[","").strip().split('T')
                D_T = datetime.datetime.strptime(st3[0],'%Y%m%d')
                DT = D_T.date().strftime('%m/%d/%Y')
                st4 = st3[1].split('-')
                HE = st4[0]
                Loc = url[url.find(start)+len(start):url.find(end)]
                Loc2=Loc.split('-ALL.')
                ba=Loc2[0]
                ft=Loc2[1]
                if x == 0:
                    df = pandas.DataFrame({'DT':[DT],'HR':[HE],'DATA':[S_Data],'BA':[ba],'DataSet':[ft]})
                    x = 1
                else:
                    df1 = pandas.DataFrame({'DT':[DT],'HR':[HE],'DATA':[S_Data],'BA':[ba],'DataSet':[ft]})
                    df = pandas.concat([df,df1])
                    #print(SeriesID+DT+HE)
        #d = {'DT':DT,'Hr':HE,'DATA':S_Data}        

            except:
                print('ERROR-bad string'+ba+' '+ft+' '+DT+HE)
                    
        
        params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                             "SERVER=JWTCVPMEDB13;"
                                             "DATABASE=Fundamentals;"
                                             "trusted_connection=yes")

        engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

        df.to_sql('EIAHR_Landing', engine, schema = 'dbo', index = False, if_exists='replace')

        conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                           "DATABASE=Fundamentals;"
                                            "trusted_connection=yes")
        import time
        time.sleep(5)
        sql = 'EXEC dbo.UPLOAD_EIA_GEN'

        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        conn.close()                                
        print(ba+' '+ft)
        x = 0

    except:
        print('ERROR-bad data'+ba+ft)

    

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


        
     
