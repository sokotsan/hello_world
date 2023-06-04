import os
import time
import datetime
import pandas as pd
import numpy as np
import win32com.client
import urllib
import pyodbc
from sqlalchemy import create_engine
import requests
import time
cols = ['DT','Flow','Tie']
a = ['01','02','03','04','05','06','07','08','09','10','11','12']
b = ['2017']
for i in range(0,2):
    for d in range(0,11):  
        url1 = 'https://transmission.bpa.gov/BUSINESS/Operations/Paths/Flowgates/monthly/Idaho-PacificNW/'+str(b[i])+'/Idaho-PacificNW_'+str(b[i])+'-'+str(a[d])+'.xls'
        url2 = 'https://transmission.bpa.gov/BUSINESS/Operations/Paths/Interties/monthly/AC/'+str(b[i])+'/AC_'+str(b[i])+'-'+str(a[d])+'.xls'
        url3 = 'https://transmission.bpa.gov/BUSINESS/Operations/Paths/Interties/monthly/BC/'+str(b[i])+'/BC_'+str(b[i])+'-'+str(a[d])+'.xls'
        url4 = 'https://transmission.bpa.gov/BUSINESS/Operations/Paths/Interties/monthly/DC/'+str(b[i])+'/DC_'+str(b[i])+'-'+str(a[d])+'.xls'
        url5 = 'https://transmission.bpa.gov/BUSINESS/Operations/Paths/Flowgates/monthly/Reno-Alturas/'+str(b[i])+'/Reno-Alturas_'+str(b[i])+'-'+str(a[d])+'.xls'
        url6 = 'https://transmission.bpa.gov/BUSINESS/Operations/Paths/Flowgates/monthly/Montana-PacificNW/'+str(b[i])+'/Montana-PacificNW_'+str(b[i])+'-'+str(a[d])+'.xls'
        url7 = 'https://transmission.bpa.gov/BUSINESS/Operations/Paths/Flowgates/monthly/Hemingway-SummerLake/'+str(b[i])+'/Hemingway-SummerLake_'+str(b[i])+'-'+str(a[d])+'.xls'
        #url = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=TRNS_USAGE&version=1&market_run_id=DAM&ti_id=ALL&ti_direction=I&startdatetime='+DED+'T07:00-0000&enddatetime='+dt2+'T07:00-0000'

        dfID = pd.read_excel(url1,
                            sheet_name = 'Data',
                            header = None,
                            usecols = "A,B",
                            skiprows = (0,1,2))

        dfID['Tie']='13'
        dfID.columns = cols


        dfAC = pd.read_excel(url2,
                            sheet_name = 'Data',
                            header = None,
                            usecols = "A,B",
                            skiprows = (0,1,2))

        dfAC['Tie']='1'
        dfAC.columns = cols


        dfBC = pd.read_excel(url3,
                            sheet_name = 'Data',
                            header = None,
                            usecols = "A,B",
                            skiprows = (0,1,2))

        dfBC['Tie']='4'
        dfBC.columns = cols


        dfDC = pd.read_excel(url4,
                            sheet_name = 'Data',
                            header = None,
                            usecols = "A,B",
                            skiprows = (0,1,2))

        dfDC['Tie']='9'
        dfDC.columns = cols


        dfRE = pd.read_excel(url5,
                            sheet_name = 'Data',
                            header = None,
                            usecols = "A,B",
                            skiprows = (0,1,2))

        dfRE['Tie']='31'
        dfRE.columns = cols

        dfMT = pd.read_excel(url6,
                            sheet_name = 'Data',
                            header = None,
                            usecols = "A,B",
                            skiprows = (0,1,2))

        dfMT['Tie']='17'
        dfMT.columns = cols

        dfHS = pd.read_excel(url7,
                            sheet_name = 'Data',
                            header = None,
                            usecols = "A,B",
                            skiprows = (0,1,2))

        dfHS['Tie']='12'
        dfHS.columns = cols

        upld = pd.concat([dfID,dfAC,dfBC,dfDC,dfRE,dfMT,dfHS])


        params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                                 "SERVER=JWTCVPMEDB03;"
                                                 "DATABASE=Fundamentals;"
                                                 "trusted_connection=yes")
        conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                                 "SERVER=JWTCVPMEDB03;"
                                                 "DATABASE=Fundamentals;"
                                                 "trusted_connection=yes")
        engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

        upld.to_sql('BPA_Hist_Ties', engine, schema = 'dbo', index = False, if_exists='replace')
        time.sleep(10)

        sql = 'EXEC BPA_Tie_Hist'

        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        time.sleep(5)
        conn.close()

