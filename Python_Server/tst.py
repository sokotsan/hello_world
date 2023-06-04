import pandas as pd
import datetime
import numpy as np
from sqlalchemy import create_engine
import pyodbc
import urllib

import requests
import io
from zipfile import ZipFile
from requests.auth import HTTPBasicAuth
#from requests.auth import HTTPDigestAuth
import pyodbc

import time
url = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_LMP&version=1&startdatetime=20181201T07:00-0000&enddatetime=20181202T07:00-0000&market_run_id=DAM&node=TH_NP15_GEN_OFFPEAK-APND'
#                url2 = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_LMP&version=1&startdatetime='+YR+MN+DN+'T07:00-0000&enddatetime='+YR+MN+DM+'T07:00-0000&market_run_id=DAM&node=TH_NP15_GEN_ONPEAK-APND'
#                url3 = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_LMP&version=1&startdatetime='+YR+MN+DN+'T07:00-0000&enddatetime='+YR+MN+DM+'T07:00-0000&market_run_id=DAM&node=TH_SP15_GEN_OFFPEAK-APND'
#                url4 = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_LMP&version=1&startdatetime='+YR+MN+DN+'T07:00-0000&enddatetime='+YR+MN+DM+'T07:00-0000&market_run_id=DAM&node=TH_SP15_GEN_ONPEAK-APND'
#                url5 = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_LMP&version=1&startdatetime='+YR+MN+DN+'T07:00-0000&enddatetime='+YR+MN+DM+'T07:00-0000&market_run_id=DAM&node=TH_ZP26_GEN_OFFPEAK-APND'
#                url6 = 'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_LMP&version=1&startdatetime='+YR+MN+DN+'T07:00-0000&enddatetime='+YR+MN+DM+'T07:00-0000&market_run_id=DAM&node=TH_ZP26_GEN_ONPEAK-APND'
response = requests.get(url)
thezip = ZipFile(io.BytesIO(response.content))
a = thezip.namelist()
thefile = thezip.extract(a[0])
dt1 = pd.read_csv(thefile)
time.sleep(5)
