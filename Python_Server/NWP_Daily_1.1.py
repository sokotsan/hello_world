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
err=0
month = datetime.datetime.now().month
year = datetime.datetime.now().year
day = datetime.datetime.now().day
dic = {
            "LocProp" : [],
            "LocName" : [],
            "Loc" : [],
            "LocPurpDesc": [],
            "FlowIndDesc" : [],
            "Loc/QTI" : [],
            "DesignCap" : [],
            "OperatingCapacity" : [],
            "TotalScheduledQty" : [],
            "OperationallyAvailableCapacity" : [],
            "IT" : [],
            "Cycle" :[],
            "PostDate" :[],
            "GasDay" :[]
            }
Date = str(month)+"/"+str(day)+"/"+str(year)
url = "http://www.northwest.williams.com/NWP_Portal/CapacityResultsScrollable.action"
#payload = {'TargetPage':'%2FCapacityResultsScrollable.action&RptType=OA&RptPart=SCROLL&StartGasFlowDate=7%2F17%2F2018&Retrieve=View+Report'}
payload = {'TargetPage':'/CapacityResultsScrollable.action',
            'RptType':'OA',
            'RptPart':'SCROLL',
            'StartGasFlowDate':Date,
            'Retrieve':'View Report'}
try:
    r = requests.post(url, payload)
    soup = BeautifulSoup(r.text, 'html.parser')
    y=soup.find_all('tr' ,attrs={'class':'y'})
    x=soup.find_all('tr' ,attrs={'class':'x'})
    finder=soup.find_all('table')[0]
    Cycle = finder.find_all('td')[5].get_text()
    PostDate=finder.find_all('td')[8].get_text()
    GasDay = finder.find_all('td')[10].get_text()
except:
    import webbrowser
    import time
    ie = webbrowser.get(webbrowser.iexplore)
    ie.open('http://www.northwest.williams.com/NWP_Portal/CapacityResultsScrollable.action')
    print('Restart Attempted')
    time.sleep(10)
    r = requests.post(url, payload)
    soup = BeautifulSoup(r.text, 'html.parser')
    y=soup.find_all('tr' ,attrs={'class':'y'})
    x=soup.find_all('tr' ,attrs={'class':'x'})
    finder=soup.find_all('table')[0]
    Cycle = finder.find_all('td')[5].get_text()
    PostDate=finder.find_all('td')[8].get_text()
    GasDay = finder.find_all('td')[10].get_text()
    pass
for i in range (0,len(y)):
               activetag = soup.find_all('tr' ,attrs={'class':'y'})[i]
               dic["LocProp"].append(activetag.find_all('td')[0].get_text())
               dic["LocName"].append(activetag.find_all('td')[1].get_text())
               dic["Loc"].append(activetag.find_all('td')[2].get_text())
               dic["LocPurpDesc"].append(activetag.find_all('td')[3].get_text())
               dic["FlowIndDesc"].append(activetag.find_all('td')[4].get_text())
               dic["Loc/QTI"].append(activetag.find_all('td')[5].get_text())
               dic["DesignCap"].append(activetag.find_all('td')[6].get_text())
               dic["OperatingCapacity"].append(activetag.find_all('td')[7].get_text())
               dic["TotalScheduledQty"].append(activetag.find_all('td')[8].get_text())
               dic["OperationallyAvailableCapacity"].append(activetag.find_all('td')[9].get_text())
               dic["IT"].append(activetag.find_all('td')[10].get_text())
               dic["Cycle"].append(Cycle)
               dic["PostDate"].append(PostDate)
               dic["GasDay"].append(GasDay)


for i in range (0,len(x)):
               activetag = soup.find_all('tr' ,attrs={'class':'x'})[i]
               dic["LocProp"].append(activetag.find_all('td')[0].get_text())
               dic["LocName"].append(activetag.find_all('td')[1].get_text())
               dic["Loc"].append(activetag.find_all('td')[2].get_text())
               dic["LocPurpDesc"].append(activetag.find_all('td')[3].get_text())
               dic["FlowIndDesc"].append(activetag.find_all('td')[4].get_text())
               dic["Loc/QTI"].append(activetag.find_all('td')[5].get_text())
               dic["DesignCap"].append(activetag.find_all('td')[6].get_text())
               dic["OperatingCapacity"].append(activetag.find_all('td')[7].get_text())
               dic["TotalScheduledQty"].append(activetag.find_all('td')[8].get_text())
               dic["OperationallyAvailableCapacity"].append(activetag.find_all('td')[9].get_text())
               dic["IT"].append(activetag.find_all('td')[10].get_text())
               dic["Cycle"].append(Cycle)
               dic["PostDate"].append(PostDate)
               dic["GasDay"].append(GasDay)
#print(d)
NWPdf = pd.DataFrame(dic)[['LocProp', 'LocName', 'Loc', 'LocPurpDesc', 'FlowIndDesc', 'Loc/QTI', 'DesignCap', 'OperatingCapacity', 'TotalScheduledQty', 'OperationallyAvailableCapacity', 'IT', 'Cycle', 'PostDate', 'GasDay']]
#SavePath= '\\\JWTCVDMEDB03\\Fundamentals\\Gas Scrapes\\NWP\\'
#PipeName = 'NWP_'
#SaveMonth= str(month)
#SaveYear=str(year)
#SaveDay = str(day)
#SaveName = SavePath+PipeName+SaveDay+'_'+SaveMonth+'_'+SaveYear+'.txt'
#np.savetxt(SaveName, NWPdf.values, fmt='%s',delimiter="\t")


params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

NWPdf.to_sql('NWP_Landing', engine, schema = 'dbo', index = False, if_exists='replace')

conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

sql = 'EXEC dbo.NWP_Upload'

cursor = conn.cursor()
cursor.execute(sql)
conn.commit()
conn.close()


        #cursor = connection.cursor()
        #connection.execute("EXEC dbo.NOAA_WX_FCST_Upload")


        #df2 = pd.DataFrame({"colw":[1],"col2":[2]})


        #conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
         #                                "SERVER=JWTCVPMEDB03;"
          #                               "DATABASE=Fundamentals;"
           #                              "trusted_connection=yes")
        
