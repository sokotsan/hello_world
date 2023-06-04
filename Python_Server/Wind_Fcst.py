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

#sql = 'EXEC dbo.Prep_winddata'
#cursor = conn.cursor()
#cursor.execute(sql)
#conn.commit()
#conn.close()                                         

time.sleep(10)

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
DF_LC = pd.read_sql_query('select distinct LC from dbo.Wind_data',conn)
LC = DF_LC.values.tolist()
sql_query = pd.read_sql_query('select * from dbo.Wind_data',conn)
wind = pd.DataFrame({'DT':[''],'HE':[''],'LC':[''],'MW':['']})
reg = linear_model.LinearRegression()
#type(cursor)
#df = pd.dataframe(cursor)
#IPC=sql_query.loc[sql_query['LC']=='IPC']
dic = {
    "Loc" : [],
    'Wind_1':[],
    'Wind_2':[],
    'Wind_3':[],
    'Wind_4':[],
    'Intercept':[]
    }
for l in range(0,len(LC)):
    df=sql_query.loc[sql_query['LC']==str(LC[l]).strip("['").strip("']")]
    df = df[df.Wind_1.notnull()]
   # for i in range (6,23):
        #dff=df.loc[df['HE']== i ]
    if np.isnan(df['Wind_4'].iloc[0])==False:
        df = df[df.Wind_2.notnull()]
        df = df[df.Wind_3.notnull()]
        df = df[df.Wind_4.notnull()]
        x=df[['Wind_1','Wind_2','Wind_3','Wind_4']]
        r=4
           # print('4')
    elif np.isnan(df['Wind_3'].iloc[0])==False:
        df = df[df.Wind_2.notnull()]
        df = df[df.Wind_3.notnull()]        
        x=df[['Wind_1','Wind_2','Wind_3']]
        r=3
            #print('3')
    elif np.isnan(df['Wind_2'].iloc[0])==False:
        df = df[df.Wind_2.notnull()]
        x=df[['Wind_1','Wind_2']]
        r=2
           # print('2')
    else:
        x=df[['Wind_1']]
        r=1
           # print('1')
    y=df[['MW']]
        #x=dff[['Skyc_1','Skyc_2','Skyc_3','Skyc_4','precip_1','Precip_2','Precip_3','Precip_4']]
    reg.fit(x,y)
    y_p=reg.predict(x)
        #dic["Loc"].append(dff.iloc[:,3])
        #dic["DT"].append(dff.iloc[:,1])
        #dic["HE"].append(dff.iloc[:,2])
        #dic["MW"].append(dff.iloc[:,16])
        #dic["Cap_fact"].append(dff.iloc[:,17])
    df_test = df[['DT','HE','LC','MW']]
    df_test = df_test.reset_index().merge(pd.DataFrame(y_p, columns = ['pred']).reset_index(), left_index=True, right_index=True, how='left')
    wind = pd.concat([wind,df_test])
    dic["Loc"].append(str(LC[l]).strip("['").strip("']"))
   # dic["HE"].append(float(i))
    dic["Intercept"].append(float(reg.intercept_[0]))
    dic["Wind_1"].append(float(reg.coef_[0][0]))
    
    #dic["Precip_1"].append(float(reg.coef_[0][r]))
    if r-1 > 0:
        dic["Wind_2"].append(float(reg.coef_[0][1]))
     #       dic["Precip_2"].append(float(reg.coef_[0][r+1]))
    else:
        dic["Wind_2"].append(0)
           # dic["Precip_2"].append(0)
    if r-2 > 0:
        dic["Wind_3"].append(float(reg.coef_[0][2]))
          #  dic["Precip_3"].append(float(reg.coef_[0][r+2]))
    else:
        dic["Wind_3"].append(0)
         #   dic["Precip_3"].append(0)
    if r-3 > 0:
        dic["Wind_4"].append(float(reg.coef_[0][3]))
          #  dic["Precip_4"].append(float(reg.coef_[0][r+3]))
    else:
        dic["Wind_4"].append(0)
         #   dic["Precip_4"].append(0)


    print(str(LC[l])+'__'+str(round(r2_score(y,y_p),2)))                
Coeffs = pd.DataFrame(dic)

#% print(wind)

params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                        "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
wind.to_sql('Wind_lm', engine, schema = 'dbo', index = False, if_exists='replace')
Coeffs.to_sql('Wind_Coeffs', engine, schema = 'dbo', index = False, if_exists='replace')
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
