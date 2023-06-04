from urllib.request import urlopen as uReq
import urllib
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import datetime 
from contextlib import suppress
from sqlalchemy import create_engine
from sqlalchemy.types import Float
from sqlalchemy.types import NVARCHAR
from sqlalchemy.types import DATETIME
from sqlalchemy.types import DATE
import pyodbc
from time import strptime
#http = urllib3.PoolManager()
LocNM = []
#Names of dams
LocNM = ['PRWW1'
         , 'LGSW1', 'TDAO3', 'IHDW1', 'LGDW1'
         , 'DWRI1', 'LMNW1', 'BONO3', 'GCDW1'
         , 'JDAO3', 'MCDW1', 'CHJW1', 'WANW1'
         , 'WELW1', 'LYDM8','MYDW1'
         ,'CHDW1','CABI1','ALFW1'
         ,'LLKW1','BRNI1','KERM8','PALI1','AMFI1','SWAI1','PLNM8','ESTO3','MODO3']

#Creates Dictionary for data
dic = {
    "Loc" : [],
    "RFCLocationID" : [],
    "IssuanceDate" : [],
    "UpDT" : [],
    "Month" : [],
    "WaterYear" : [],
    "DT" : [],
    "KAF_90" : [],
    "KAF_75" : [],
    "KAF_50" : [],
    "KAF_25" : [],
    "KAF_10" : [],
    "KAF_min" : [],
    "KAF_max" : [],
    "runoff" : [],
    "Avg_30Yr" : []}

dic2 = {
     "Loc" : [],
     "Month" : [],
     "WaterYear" : [],
     "Perc_Avg" : []}

upDT = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
upDTDT=datetime.datetime.today()
tdymnthnum = int(datetime.datetime.today().strftime('%m'))
fl = 0
page_html = bytearray()

#begin loop
for x in range (0,len(LocNM)):

##Get next water year data
    try:
        watercsv = pd.read_csv('https://www.nwrfc.noaa.gov/water_supply/monthly/monthly_forecasts.php?id='+LocNM[x]+'&datepick=&csv=ESP10&nextwy=1&csv_min_max=1',header=2)
        watercsv['Issuance Date'] = watercsv['Issuance Date'].astype('datetime64[ns]')
        watercsv['Ensemble Date'] = watercsv['Issuance Date'].astype('datetime64[ns]')

        tp = pd.read_csv('https://www.nwrfc.noaa.gov/water_supply/monthly/monthly_forecasts.php?id='+LocNM[x]+'&datepick=&csv=ESP10&nextwy=1&csv_min_max=1',delimiter = ' ',nrows=1)
        wateryear = int(tp.iloc[: , -1].name)

        
        for i in range (0,len(watercsv)):
            if watercsv["ID"].iloc[i] == LocNM[x]:
                dic["Loc"].append(LocNM[x])
                dic["IssuanceDate"].append(watercsv['Issuance Date'].iloc[0])
                dic["UpDT"].append(upDT)
                dic["WaterYear"].append(wateryear)
                mnthnum = strptime(watercsv['Month'].iloc[i],'%b').tm_mon
                if mnthnum<10:
                    yrnum = wateryear
                else:
                    yrnum = wateryear-1
                fcstmon = datetime.datetime(yrnum, mnthnum,1)
                dic["DT"].append(fcstmon.strftime('%Y-%m-%d %H:%M:%S'))
                dic["Month"].append(watercsv['Month'].iloc[i])
                try: dic["KAF_90"].append(watercsv['90% FCST'].iloc[i])
                except: dic["KAF_90"].append(None)
                try: dic["KAF_75"].append(watercsv['75% FCST'].iloc[i])
                except: dic["KAF_75"].append(None)
                try: dic["KAF_50"].append(watercsv['50% FCST'].iloc[i])
                except: dic["KAF_50"].append(None)
                try: dic["KAF_25"].append(watercsv['25% FCST'].iloc[i])
                except: dic["KAF_25"].append(None)
                try: dic["KAF_10"].append(watercsv['10% FCST'].iloc[i])
                except: dic["KAF_10"].append(None)
                try: dic["KAF_min"].append(watercsv['min'].iloc[i])
                except: dic["KAF_min"].append(None)
                try: dic["KAF_max"].append(watercsv['max'].iloc[i])
                except: dic["KAF_max"].append(None)
                try: dic["runoff"].append(watercsv['runoff'].iloc[i])
                except: dic["runoff"].append(None)
                try: dic["Avg_30Yr"].append(watercsv['30 year average'].iloc[i])
                except: dic["Avg_30Yr"].append(None)
                dic["RFCLocationID"].append(None)
        print("next year end "+LocNM[x])
    except:
        print("next year fail for "+LocNM[x])

####to get percent of average for next year
##    try:
##        url = "https://www.nwrfc.noaa.gov/water_supply/monthly/monthly_forecasts.php?id="+LocNM[x]+"&datepick=&nextwy=1&csv_min_max=1"
##        r =  requests.get(url , timeout=5)
##        page_html = r.content
##        soup = BeautifulSoup(page_html, 'html.parser')
##    except:
##        print(url+' SSL Failed')
##        page_html = bytearray()
##        try:
##            r =  requests.get(url, verify=False , timeout=10)
##
##            page_html = r.content
##            soup = BeautifulSoup(page_html, 'html.parser')
##            print('Recover')
##        except:
##            print('ERROR - No Connection'+url)
##    try:
##        div = soup.find('div', {'id':'tabular_data_1'})
##        tr = div.find_all('tr')
##        for i in range (4,len(tr)-2):
##            activetag = tr[i].find_all('td')
##            dic2["Loc"].append(LocNM[x])
##            dic2["WaterYear"].append(wateryear)
##            dic2["Month"].append(activetag[0].get_text())
##            try: dic2["Perc_Avg"].append(float(activetag[3].get_text()))
##            except: dic2["Perc_Avg"].append(None)
##        print("next year success for "+LocNM[x])
##    except:
##        print("next year fail for "+LocNM[x])

##Get current water year data
    try:
        watercsv = pd.read_csv('https://www.nwrfc.noaa.gov/water_supply/monthly/monthly_forecasts.php?id='+LocNM[x]+'&datepick=&csv=ESP10&nextwy=0&csv_min_max=1',header=2)
        watercsv['Issuance Date'] = watercsv['Issuance Date'].astype('datetime64[ns]')
        watercsv['Ensemble Date'] = watercsv['Issuance Date'].astype('datetime64[ns]')

        tp = pd.read_csv('https://www.nwrfc.noaa.gov/water_supply/monthly/monthly_forecasts.php?id='+LocNM[x]+'&datepick=&csv=ESP10&nextwy=0&csv_min_max=1',delimiter = ' ',nrows=1)
        wateryear = int(tp.iloc[: , -1].name)
        
        for i in range (0,len(watercsv)):
            if watercsv["ID"].iloc[i] == LocNM[x]:
                dic["Loc"].append(LocNM[x])
                dic["IssuanceDate"].append(watercsv['Issuance Date'].iloc[len(watercsv)-2])
                dic["UpDT"].append(upDT)
                dic["WaterYear"].append(wateryear)
                mnthnum = strptime(watercsv['Month'].iloc[i],'%b').tm_mon
                if mnthnum<10:
                    yrnum = wateryear
                else:
                    yrnum = wateryear-1
                fcstmon = datetime.datetime(yrnum, mnthnum,1)
                dic["DT"].append(fcstmon.strftime('%Y-%m-%d %H:%M:%S'))
                dic["Month"].append(watercsv['Month'].iloc[i])
                try: dic["KAF_90"].append(watercsv['90% FCST'].iloc[i])
                except: dic["KAF_90"].append(None)
                try: dic["KAF_75"].append(watercsv['75% FCST'].iloc[i])
                except: dic["KAF_75"].append(None)
                try: dic["KAF_50"].append(watercsv['50% FCST'].iloc[i])
                except: dic["KAF_50"].append(None)
                try: dic["KAF_25"].append(watercsv['25% FCST'].iloc[i])
                except: dic["KAF_25"].append(None)
                try: dic["KAF_10"].append(watercsv['10% FCST'].iloc[i])
                except: dic["KAF_10"].append(None)
                try: dic["KAF_min"].append(watercsv['min'].iloc[i])
                except: dic["KAF_min"].append(None)
                try: dic["KAF_max"].append(watercsv['max'].iloc[i])
                except: dic["KAF_max"].append(None)
                try: dic["runoff"].append(watercsv['runoff'].iloc[i])
                except: dic["runoff"].append(None)
                try: dic["Avg_30Yr"].append(watercsv['30 year average'].iloc[i])
                except: dic["Avg_30Yr"].append(None)
                dic["RFCLocationID"].append(None)
        print("current year end "+LocNM[x])
    except:
        print("current year fail for "+LocNM[x])

##to get percent of average current year
##    try:
##        url = "https://www.nwrfc.noaa.gov/water_supply/monthly/monthly_forecasts.php?id="+LocNM[x]+"&datepick=&nextwy=0&csv_min_max=1"
##        r =  requests.get(url , timeout=5)
##        page_html = r.content
##        soup = BeautifulSoup(page_html, 'html.parser')
##    except:
##        print(url+' SSL Failed')
##        page_html = bytearray()
##        try:
##            r =  requests.get(url, verify=False , timeout=10)
##
##            page_html = r.content
##            soup = BeautifulSoup(page_html, 'html.parser')
##            print('Recover')
##        except:
##            print('ERROR - No Connection'+url)
##    try:
##        div = soup.find('div', {'id':'tabular_data_1'})
##        tr = div.find_all('tr')
##        for i in range (4,len(tr)-2):
##            activetag = tr[i].find_all('td')
##            dic2["Loc"].append(LocNM[x])
##            dic2["WaterYear"].append(wateryear)
##            dic2["Month"].append(activetag[0].get_text())
##            try: dic2["Perc_Avg"].append(float(activetag[3].get_text()))
##            except: dic2["Perc_Avg"].append(None)
##        print("current year success for "+LocNM[x])
##    except:
##        print("current year fail for "+LocNM[x])


#dicdf=pd.DataFrame(dic)
#dic2df=pd.DataFrame(dic2)

#NWRFC_WSF = dicdf.merge(dic2df,how='right',on=['Loc','WaterYear','Month'])

NWRFC_WSF=pd.DataFrame(dic)

NWRFC_WSF["DT"]=NWRFC_WSF["DT"].astype('datetime64[ns]')
NWRFC_WSF["UpDT"]=NWRFC_WSF["UpDT"].astype('datetime64[ns]')

temp = NWRFC_WSF["KAF_50"]/NWRFC_WSF["Avg_30Yr"]*100
temp2 = pd.to_numeric(NWRFC_WSF["runoff"], errors='coerce')/NWRFC_WSF["Avg_30Yr"]*100
NWRFC_WSF["Perc_Avg"] = np.where(temp>0,temp,temp2)
NWRFC_WSF["KAF_maxPerc"] = NWRFC_WSF["KAF_max"]/NWRFC_WSF["Avg_30Yr"]*100
NWRFC_WSF["KAF_10Perc"] = NWRFC_WSF["KAF_10"]/NWRFC_WSF["Avg_30Yr"]*100
NWRFC_WSF["KAF_25Perc"] = NWRFC_WSF["KAF_25"]/NWRFC_WSF["Avg_30Yr"]*100
NWRFC_WSF["KAF_50Perc"] = NWRFC_WSF["KAF_50"]/NWRFC_WSF["Avg_30Yr"]*100
NWRFC_WSF["KAF_75Perc"] = NWRFC_WSF["KAF_75"]/NWRFC_WSF["Avg_30Yr"]*100
NWRFC_WSF["KAF_90Perc"] = NWRFC_WSF["KAF_90"]/NWRFC_WSF["Avg_30Yr"]*100
NWRFC_WSF["KAF_minPerc"] = NWRFC_WSF["KAF_min"]/NWRFC_WSF["Avg_30Yr"]*100

cols = NWRFC_WSF .columns.tolist()
col = cols[-1:] + cols[:-1]
NWRFC_WSF_df = NWRFC_WSF [col]

params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")
conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")
engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

NWRFC_WSF_df.to_sql('HY_RFCESPWaterSupplyForecastMonthly_Landing', engine, schema = 'dbo', index = False, dtype={'KAF_90':Float(),'KAF_75':Float(),'KAF_50':Float(),'KAF_25':Float(),'KAF_10':Float(),'KAF_min':Float(),'KAF_max':Float(),'Perc_Avg':Float(),'Avg_30Yr':Float(),'WaterYear':Float(),'Loc':NVARCHAR(),'Mon':NVARCHAR(),'ReportDate':DATE(),'DT':DATE(),'UpDT':DATETIME()}, if_exists='replace')

sql = 'EXEC spImport_HY_RFCESPWaterSupplyForecastMonthly'


import time

time.sleep(5)

cursor = conn.cursor()
cursor.execute(sql)
conn.commit()
conn.close()

time.sleep(2)


#SaveName= '\\\JWTCVDMEDB03\\Fundamentals\\Gas Scrapes\\Hydrofcst.txt'
#np.savetxt(SaveName, NWRFC.values, fmt='%s',delimiter="\t")









