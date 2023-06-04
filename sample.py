#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 20 19:35:04 2023

@author: skotsan
"""

import pandas as pd
import json

json_data = '{"response":{"warnings":[{"warning":"incomplete return","description":"The API can only return 5000 rows in JSON format.  Please consider constraining your request with facet, start, or end, or using offset to paginate results."}],"total":40776,"dateFormat":"YYYY-MM-DD\\"T\\"HH24TZH","frequency":"local-hourly","data":[{"period":"2023-03-20T00-07","respondent":"SRP","respondent-name":"Salt River Project Agricultural Improvement and Power District","fueltype":"NUC","type-name":"Nuclear","value":3998,"value-units":"megawatthours"},{"period":"2023-03-19T23-07","respondent":"SRP","respondent-name":"Salt River Project Agricultural Improvement and Power District","fueltype":"NUC","type-name":"Nuclear","value":4000,"value-units":"megawatthours"}]},"description":"Hourly net generation by balancing authority and energy source.  \\n    Source: Form EIA-930\\n    Product: Hourly Electric Grid Monitor","id":"fuel-type-data"},"request":{"command":"\\/v2\\/seriesid\\/EBA.SRP-ALL.NG.NUC.HL","params":{"frequency":"local-hourly","data":["value"],"facets":{"respondent":["SRP"],"fueltype":["NUC"]},"start":null,"end":null,"sort":[{"column":"period","direction":"desc"}],"offset":0,"length":5000,"api_key":"642c17f567bb726530c47a1e2e0ef433"},"route":"electricity\\/rto\\/fuel-type-data"},"apiVersion":"2.1.4"}'

data = json.loads(json_data)

df = pd.json_normalize(data, record_path=['response', 'data'])
df
#%%