from urllib.request import urlopen as uReq
from bs4 import BeautifulSoup
import pandas as pd
import pyodbc
from datetime import datetime as dt
from sqlalchemy import create_engine
import urllib
import traceback
#display_errors = On


url = 'https://transmission.bpa.gov/Business/Operations/Wind/baltwg.txt'
uClient = uReq(url)
page_html = uReq(url)
uClient.close()
BPA_Raw = BeautifulSoup(page_html, 'html.parser')
BPA_C = BPA_Raw.contents
lst= str(BPA_C).split('\\r\\n')
colnames = str(lst[6]).replace(" ", "").split("\\t")
df = pd.DataFrame(columns=colnames)

for i in range(7,len(lst)-1):
    ln = str(lst[i]).split("\\t")
    df2 = pd.DataFrame([[ln[0],ln[1],ln[2],ln[3],ln[4],ln[5]]], columns = colnames)
    df = df.append(df2)

params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

df.to_sql('BPA_Gen_Landing', engine, schema = 'dbo', index = False, if_exists='replace')



conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

sql = 'EXEC dbo.BPA_Gen_Upload'

cursor = conn.cursor()
cursor.execute(sql)
conn.commit()
conn.close()
