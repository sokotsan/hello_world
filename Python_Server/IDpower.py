#from urllib.request import urlopen as uReq
import urllib
import json
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import pandas
from sqlalchemy import create_engine
import pyodbc
#import datetime 
#from contextlib import suppress
#from sqlalchemy import create_engine
#import pyodbc
#url2 = "https://api.idahopower.com/IdaStream/Api/V1/GenerationAndDemand/list?filter.date=20190718&hr=all"
url2 = "https://api.idahopower.com/IdaStream/Api/V1/GenerationAndDemand/subset"
r =  requests.get(url2,verify=False)
ID= json.loads(r.content.decode('utf-8'))

# try passing page size
#DT = ID['page']['date']
#ID = json.loads(uClient.content.decode('utf-8'))
DT=ID['list']
DF = pandas.DataFrame(DT)

params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")
conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")
engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

DF.to_sql('IDGN_Landing', engine, schema = 'dbo', index = False, if_exists='replace')

import time
time.sleep(5)
sql = 'EXEC dbo.Upload_ID_Gen'

cursor = conn.cursor()
cursor.execute(sql)
conn.commit()
conn.close()
