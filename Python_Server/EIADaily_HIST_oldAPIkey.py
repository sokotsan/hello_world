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


BA =['SRP']
FT = ['NG.NUC','NG.OIL','NG.WND','NG.SUN','NG.OTH','NG.NG','NG.COL','NG.WAT','DF','TI','D',]

#['AVRN','NW','AVA','PACW','PACE','PSEI','SCL','PGE','TPWR','CHPD','TEPC','AZPS','DOPD','PSCO'
# ,'BPAT','SRP','GRMA','DEAA','HGMA','GRIF','WALC','IID','CFE','LDWP'
# ,'CISO','TIDC','BANC','WACM','NEVP','IPCO','WAUW','NWMT','WWA','GWA','GCPD'
# ,'GRID','CAL','SW','EPE','PNM']



#BA = ['NW']
#,'BANC','CAL','CISO','DOPD','GCPD','NEVP','CHPD','PGE','PGE','PACW','BPAT','NW','NWMT','PACE','PSCO','PSEI','TIDC','AVA']
#FT= ['D']
     #,'NG.WAT']

x = 0 

#URLTXT = "http://api.eia.gov/series/?api_key=e79d5730e424c57ad40929e31fc372e5&series_id=EBA.AVA-ALL.NG.WAT.HL"
URLS = "http://api.eia.gov/series/?api_key=e79d5730e424c57ad40929e31fc372e5&series_id="


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


for a in range(0, len(BA)):
    ba=BA[a]
    for b in range(0,len(FT)):
        ft=FT[b]
        SeriesID ='EBA.'+BA[a]+'-ALL.'+FT[b]+'.HL'
        URL = URLS+SeriesID
        #URL = 'https://api.eia.gov/series/?api_key=e79d5730e424c57ad40929e31fc372e5&series_id=EBA.SRP-ALL.NG.SUN.HL'
        #uClient = uReq(URL)
        #page_html = uClient.read()
        #uClient.close()
        try:
            r =  requests.get(URL)
        #r =  requests.get(URL)
            EIA = json.loads(r.content.decode('utf-8'))
        except:
            print(SeriesID+' SSL Failed')
            r =  requests.get(URL, verify=False)
            EIA = json.loads(r.content.decode('utf-8'))
        try:
            E_DATA = EIA['series'][0]['data']
            for i in range(0,len(E_DATA)):
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
                    if x == 0:
                        df = pandas.DataFrame({'DT':[DT],'HR':[HE],'DATA':[S_Data],'BA':[ba],'DataSet':[ft]})
                        x = 1
                    else:
                        df1 = pandas.DataFrame({'DT':[DT],'HR':[HE],'DATA':[S_Data],'BA':[ba],'DataSet':[ft]})
                        df = pandas.concat([df,df1])
                    #print(SeriesID+DT+HE)
        #d = {'DT':DT,'Hr':HE,'DATA':S_Data}        

                except:
                    print('ERROR-bad string'+SeriesID+DT+HE)
                    
        
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
            print(BA[a]+' '+FT[b])
            x = 0

        except:
            print('ERROR-bad data'+BA[a]+FT[b])

    

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


        
     
