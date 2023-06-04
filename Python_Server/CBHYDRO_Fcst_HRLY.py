from sklearn import linear_model
import urllib
from sqlalchemy import create_engine
import pyodbc
import pandas as pd
from sklearn.metrics import r2_score
import matplotlib.pyplot as plt
import numpy as np
#reg = linear_model.LinearRegression()
#x=[0,1,2]
#x=x.reshape(-1,1)
#y=[0,2,4]
#reg.fit(x,y)
#r_sq = reg.score(x,y)
#reg.coef_
import time
#exec dbd.prep_solardata

params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")
conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                        "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                          "trusted_connection=yes")
sql = 'EXEC dbo.Prep_hydrodata'
cursor = conn.cursor()
cursor.execute(sql)
conn.commit()
conn.close()                                         

time.sleep(10)

#%%



params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                       "trusted_connection=yes")
#cursor = conn.cursor()
#sql = 'EXEC Upload_BA_SOLAR_CAPfact'
#cursor.execute(sql)
#conn.commit()
#time.sleep(20)
#cursor = conn.cursor()
#sql = 'EXEC prep_solardata'
#cursor = conn.cursor()
#cursor.execute(sql)
#conn.commit()
#time.sleep(20)



#time.sleep(20)

#conn.close()
                                       


#cursor = conn.cursor()
#cursor.execute('select * from dbo.solar_data')
DF_LC = pd.read_sql_query('select distinct LC from dbo.hydro_data',conn)
LC = DF_LC.values.tolist()
sql_query = pd.read_sql_query('select * from dbo.hydro_data',conn)
#%%
print(sql_query)
#%%
Flow = pd.DataFrame({'DT':[''],'HE':[''],'LC':[''],'MW':['']})
reg = linear_model.LinearRegression()
#type(cursor)
#df = pd.dataframe(cursor)
#IPC=sql_query.loc[sql_query['LC']=='IPC']
dic = {
    "Loc" : [],
    "HE" : [],
    'Flow_1':[],
    'Flow_2':[],
    'Flow_3':[],
    'Flow_4':[],
    'Intercept':[]
    }
time.sleep(.5)
#%%
# for l in range(0,len(LC)):
#     time.sleep(.5)
#     df=sql_query.loc[sql_query['LC']==str(LC[l]).strip("['").strip("']")]
#     for i in range (1,25):
#         dff=df.loc[df['HE']== i ]
#         if pd.isnull(dff['Flow_4'].iloc[0])==False:
#             dff = dff[dff.Flow_2.notnull()]
#             dff = dff[dff.Flow_3.notnull()]
#             dff = dff[dff.Flow_4.notnull()]
#             x=dff[['Flow_1','Flow_2','Flow_3','Flow_4']]
#             r=4
#            # print('4')
#         elif pd.isnull(dff['Flow_3'].iloc[0])==False:
#             dff = dff[dff.Flow_2.notnull()]
#             dff = dff[dff.Flow_3.notnull()]        
#             x=dff[['Flow_1','Flow_2','Flow_3']]
#             r=3
#             #print('3')
#         elif pd.isnull(dff['Flow_2'].iloc[0])==False:
#             dff = dff[dff.Flow_2.notnull()]
#             x=dff[['Flow_1','Flow_2']]
#             r=2
#            # print('2')
#         else:
#             x=dff[['Flow_1']]
#             r=1
#            # print('1')
#         y=dff[['MW']]
#         #x=dff[['Skyc_1','Skyc_2','Skyc_3','Skyc_4','precip_1','Precip_2','Precip_3','Precip_4']]
#         reg.fit(x,y)
#         y_p=reg.predict(x)
#         #dic["Loc"].append(dff.iloc[:,3])
#         #dic["DT"].append(dff.iloc[:,1])
#         #dic["HE"].append(dff.iloc[:,2])
#         #dic["MW"].append(dff.iloc[:,16])
#         #dic["Cap_fact"].append(dff.iloc[:,17])
#         df_test = dff[['DT','HE','LC','MW']]
#         df_test = df_test.reset_index().merge(pd.DataFrame(y_p, columns = ['pred']).reset_index(), left_index=True, right_index=True, how='left')
#         Flow = pd.concat([Flow,df_test])
#         dic["Loc"].append(str(LC[l]).strip("['").strip("']"))
#         dic["HE"].append(float(i))
#         dic["Intercept"].append(float(reg.intercept_[0]))
#         dic["Flow_1"].append(float(reg.coef_[0][0]))
#         if r-1 > 0:
#             dic["Flow_2"].append(float(reg.coef_[0][1]))
#         else:
#             dic["Flow_2"].append(0)
#         if r-2 > 0:
#             dic["Flow_3"].append(float(reg.coef_[0][2]))
#         else:
#             dic["Flow_3"].append(0)
#         if r-3 > 0:
#             dic["Flow_4"].append(float(reg.coef_[0][3]))
#         else:
#             dic["Flow_4"].append(0)


#         print(str(LC[l])+'__'+str(i)+'__'+str(round(r2_score(y,y_p),2)))                
# Coeffs = pd.DataFrame(dic)
#%%
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")
conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                        "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")
engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
#Flow.to_sql('Flow_lm', engine, schema = 'dbo', index = False, if_exists='replace')
#Coeffs.to_sql('Flow_Coeffs', engine, schema = 'dbo', index = False, if_exists='replace')


#time.sleep(5)


sql = 'EXEC dbo.East_Hydro_fcst'
cursor = conn.cursor()
cursor.execute(sql)
conn.commit()
conn.close()

   # fig,ax=plt.subplots()
  #  ax.scatter(y,y_p)
#plt.show()
#plt.close
#IPC=IPC.loc[IPC['HE']==8]
#S1=IPC[['Skyc_1']]
#S2=IPC[['Skyc_2']]
#S3=PAC[['Skyc_3']]
#S4=PAC[['Skyc_4']]
