#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb  9 23:13:41 2023

@author: skotsan
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
#%%
url = "https://www.nwrfc.noaa.gov/station/flowplot/textPlot.cgi?id=MODO3&pe=HG"

giurl = "https://www.nwrfc.noaa.gov/station/flowplot/textPlot.cgi?id=MODO3&pe=HG"
response = requests.get(url)

soup = BeautifulSoup(response.text, "html.parser")

# Find all rows in the table starting from line 6

# Print the parsed HTML
print(soup.prettify())
#%%




# Find all rows in the table
rows = soup.find_all("tr")

# Keep track of whether we have found the "Date" row
found_date_row = False

# Process each row
for row in rows:
    cols = row.find_all("td")
    if len(cols) >= 1:
        date = cols[0].text.strip()
        if date == "Date":
            # We have found the "Date" row
            found_date_row = True
            continue
        
        if found_date_row:
            # We have found the "Date" row, so process the data
            if len(cols) >= 3:
                value1 = cols[1].text.strip()
                value2 = cols[2].text.strip()
                
                value3 = ""
                value4 = ""
                value5 = ""
                if len(cols) >= 6:
                    value3 = cols[3].text.strip()
                    value4 = cols[4].text.strip()
                    value5 = cols[5].text.strip()
                
                # Do something with the date and values
                print(date, value1, value2, value3, value4, value5)