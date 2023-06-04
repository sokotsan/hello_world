# -*- coding: utf-8 -*-
"""
Created on Wed Jun 22 14:16:19 2022

@author: zpfundisql
"""

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

#URLTXT = "http://api.eia.gov/series/?api_key=642c17f567bb726530c47a1e2e0ef433&series_id=EBA.AVA-ALL.NG.WAT.HL"
URLS = "https://api.eia.gov/series/?api_key=0QwNcD8sqbirjZKTHZmscfH01ElLFFvTfVg08TS5&series_id="


URLTXT = "https://api.eia.gov/series/?api_key=642c17f567bb726530c47a1e2e0ef433&series_id=EBA.SRP-ALL.NG.SUN.HL"
r = requests.get(URLTXT)
EIA = json.loads(r.content.decode('utf-8'))
EIA
    #%%