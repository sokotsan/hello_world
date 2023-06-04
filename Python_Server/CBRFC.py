from urllib.request import urlopen as uReq
import requests
from bs4 import BeautifulSoup
import pyodbc
from sqlalchemy import create_engine
import urllib
import pandas as pd
import datetime

Locn = []
Locn =  ['BMDC2','CLFA3','GLDA3','LKSA3','CBDN2','MPSC2','GRZU1','BEAI1']
#'GLDA3'
dic = {
    "Loc" : [],
    "DT" : [],
    "MN" : [],
    "DN" : [],
    "YR" : [],
    "HR" : [],
    "O_FT" : [],
    "O_CFS" : [],
    "S_FT" : [],
    "S_CFS" : [],
    "Type" : [],
    "I" : []}

for x in range(0,len(Locn)):
    my_url = "https://www.cbrfc.noaa.gov/station/flowplot/flowplot.cgi?id="+Locn[x]+"&sim=on&stats=on&pdays=15&fdays=10&hsim=&swin=&showflow=on&showtflow=on"
    #my_url = "https://www.cbrfc.noaa.gov/station/flowplot/flowplot.cgi?id=GLDA3&sim=on&stats=on&pdays=30&fdays=10&hsim=&swin=&showflow=on&showtflow=on"
    try:
        #uClient = uReq(my_url)
        r =  requests.get(my_url)
    except:
        r =  requests.get(my_url, verify=False , timeout=10)
    try:
        page_html = r.content
    except http.client.IncompleteRead as e:
        page_html = e.partial
   # uClient.close()
    soup = BeautifulSoup(page_html, 'html.parser')


#str(tr) = soup.find_all('pre')
#sting=str(soup.find_all('pre'))

    lst=[]
    string=str(soup.find_all('pre'))
    lst = string.split('\n')
    for i in range(3,len(lst)-1):
    #for i in range(3,17):
        sts = lst[i].strip('</i>').split('               ')
        if len(sts) < 3:
            #ts = sts[0].split(' ')
            l=len(sts[0])
            dic["Loc"].append(Locn[x])
            dic["DT"].append(sts[0][0:8])
            dic["MN"].append(sts[0][0:2])
            dic["DN"].append(sts[0][3:5])
            if int(sts[0][0:2])> int(datetime.datetime.today().strftime("%m")):
                dic["YR"].append((datetime.datetime.now() - datetime.timedelta(days=365)).strftime("%Y"))
            else:
                dic["YR"].append(datetime.datetime.today().strftime("%Y"))
            dic["HR"].append(sts[0][6:8])
            dic["O_FT"].append(sts[0][8:(l-18)])
            dic["O_CFS"].append(sts[0][(l-18):(l-12)])
            dic["S_FT"].append(sts[0][(l-12):(l-6)])
            dic["S_CFS"].append(sts[0][(l-6):l])
            dic["Type"].append("Act")
            dic["I"].append(i)
            #ts = sts[1].split(' ')
            l=len(sts[1])
            dic["Loc"].append(Locn[x])
            dic["DT"].append(sts[1][0:8])
            dic["MN"].append(sts[1][0:2])
            dic["DN"].append(sts[1][3:5])
            if int(sts[0][0:2])> int(datetime.datetime.today().strftime("%m")):
                dic["YR"].append((datetime.datetime.now() - datetime.timedelta(days=365)).strftime("%Y"))
            else:
                dic["YR"].append(datetime.datetime.today().strftime("%Y"))
            dic["HR"].append(sts[1][6:8])
            dic["O_FT"].append(sts[1][8:(l-18)])
            dic["O_CFS"].append(sts[1][(l-18):(l-12)])
            dic["S_FT"].append(sts[1][(l-12):(l-6)])
            dic["S_CFS"].append(sts[1][(l-6):l])
            dic["Type"].append("fcst")
            dic["I"].append(i)
        else :
            #ts = sts[0].split(' ')
            l=len(sts[0])
            dic["Loc"].append(Locn[x])
            dic["DT"].append(sts[0][0:8])
            dic["MN"].append(sts[0][0:2])
            dic["DN"].append(sts[0][3:5])
            if int(sts[0][0:2])> int(datetime.datetime.today().strftime("%m")):
                dic["YR"].append((datetime.datetime.now() - datetime.timedelta(days=365)).strftime("%Y"))
            else:
                dic["YR"].append(datetime.datetime.today().strftime("%Y"))
            dic["HR"].append(sts[0][6:8])
            dic["O_FT"].append(sts[0][8:(l-18)])
            dic["O_CFS"].append(sts[0][(l-18):(l-12)])
            dic["S_FT"].append(sts[0][(l-12):(l-6)])
            dic["S_CFS"].append(sts[0][(l-6):l])
            dic["Type"].append("Act")
            dic["I"].append(i)
    CBRFC = pd.DataFrame(dic)
   # CBRFC['HR'] = 
#CBRFC = pd.DataFrame(dic)

#%%


#sts = lst[3].split('    ')

#lst = sting.split('\n')

#if len(lst[3]) > 70:#
#    sts = lst[3].split#('               ')

#sts = lst[10].split('               ')


#lst[10]



#with open ('\\\corp.dom\PM1\\G1\\POWER OPERATIONS\\FUNDIES\\Hydro\\ENSO.txt','w') as fid:
#r =  requests.get(my_url)


#with open ('\\\corp.dom\PM1\\G1\\FUNDIES\\Hydro Model\\ENSO.txt','w') as fid:
#    fid.write(str(soup))
#'\\\corp.dom\PM1\\S1\\FUNDIES\\Hydro Model\\ENSO.txt'
#book = '\\\corp.dom\PM1\\G1\\POWER OPERATIONS\\FUNDIES\\Hydro\\ENSO.txt'

#dt1 = pd.read_csv(book)



    params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")
    conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                        "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")
    engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

    CBRFC.to_sql('CBRFC_Landing', engine, schema = 'dbo', index = False, if_exists='replace')

    sql = 'EXEC Upload_CBRFC'
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    conn.close()


