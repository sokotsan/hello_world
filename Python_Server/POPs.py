import os
import time
import datetime
import pandas as pd
import numpy as np
import win32com.client

import urllib
import pyodbc
from sqlalchemy import create_engine

##find Filename
FilePath1= 'S:\\RISKMGT\\PowerOps\\2019 POPS\\'
a = []
b = [] 
a.append(os.listdir(FilePath1))
FileName1 = [item for item in a[0] if item.startswith('2019 POPS Current')]
bk = FileName1[0]
#bk = len(FileName1)-1
##save down newest version
book = FilePath1+bk
#book = FilePath1+FileName1[bk]
xlapp = win32com.client.DispatchEx("Excel.Application")
print("Opening Master")
wb = xlapp.workbooks.open(book,0,1)
xlapp.visible = False 
#, constants.ReadOnly=true
#Savedown new file and refresh
xlapp.DisplayAlerts = False
#wb.SaveAs(Filename='S:\\FUNDIES\\Gas Scrapes\\POPs Refresh\\POP.xlsx') #overwrites file each time, need to substitute 'file'
wb.SaveAs(Filename='C:\\Users\\E09138\\AppData\\Local\\Programs\\Python\\Python37\\TSK\\POPs.xlsm') #overwrites file each time, need to substitute 'file'
print("Book Saved")
wb.Close(True)
xlapp.Quit()
os.system("taskkill /f /im  EXCEL.EXE")
print("Opening Book")
xlapp = win32com.client.DispatchEx("Excel.Application")
wb1= xlapp.workbooks.open('C:\\Users\\E09138\\AppData\\Local\\Programs\\Python\\Python37\\TSK\\POPs.xlsm',1,0,1,0,0,1)

# 'S:\\FUNDIES\\Gas Scrapes\\POPs Refresh\\POP.xlsx',1,0,1,0,0,1)
print("Starting Refresh")
wb1.RefreshAll()
print("Starting Sleep")	
time.sleep(280)
print("Savingbook")	
wb1.Save()
wb1.Close(True)
xlapp.DisplayAlerts = True
print("xlapp Quit")	
xlapp.Quit()
a = []
b = []
dt = []
names = []
#scrape new file
print("Grabbing Data")	
FileName2= 'C:\\Users\\E09138\\AppData\\Local\\Programs\\Python\\Python37\\TSK\\POPs.xlsm'
#C:\\Users\\E09138\\AppData\\Local\\Programs\\Python\\Python37\\TSK\\POP.xlsx
Colnames = ['LineItem', '1/1/2019', '2/1/2019', '3/1/2019', '4/1/2019', '5/1/2019', '6/1/2019', '7/1/2019'
            , '8/1/2019', '9/1/2019', '10/1/2019', '11/1/2019', '12/1/2019']
Date = pd.read_excel(FileName2,
            sheet_name = 'Summary',
            header = 0,
	    usecols = "B,C",
            Index_col = "B"
            #skiprows = (1,2)
                     )
#table = pd.read_excel(FileName2,
#            sheet_name = 'Summary',
#            #range = ['D5:P5'])
#            header = None,
#            names = Colnames,
#            #index_col = 3,
#            usecols = "C,D,E, F,G,h,i,j,k,l,m,n,o",
#            skiprows = (0,1,2,3,4,5,6,10,15,16,17,26,29,33,34,36,39,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73),
#            convert_float = False)Budget:      ...                NaN

table = pd.read_excel(FileName2,
            sheet_name = 'Summary',
            #range = ['D5:P5'])
            header = None,
            names = Colnames,
            #index_col = 3,
            usecols = "C,D,E, F,G,h,i,j,k,l,m,n,o",
            skiprows = (0,1,2,3,4,5,6),
	    convert_float = False)

table2 = table.iloc[:50]

table2 = table2.dropna(subset=['LineItem'])

RD= Date['Data As Of:'].iloc[0]
SaveName1 = RD
SaveName = SaveName1.replace('-','')+' POPS v20191017'
#SaveName1 =RD= Date['Data As Of:'].iloc[0]
print("Formating data")
#table.fillna(0)
df=pd.DataFrame(table2)
df=df.replace('/','',regex = True)
df=df.replace(' ','_',regex = True)
df2=df.fillna(0)
df1=df2.reset_index().pivot_table(values = ['1/1/2019', '2/1/2019', '3/1/2019', '4/1/2019', '5/1/2019', '6/1/2019', '7/1/2019'
        , '8/1/2019', '9/1/2019', '10/1/2019', '11/1/2019', '12/1/2019'], columns='LineItem')


#df1=df.reset_index(drop = True).pivot_table(values = ['1/1/2019', '2/1/2019', '3/1/2019', '4/1/2019', '5/1/2019', '6/1/2019', '7/1/2019'
 #       , '8/1/2019', '9/1/2019', '10/1/2019', '11/1/2019', '12/1/2019'], columns='LineItem')

b=[]
for d in range (0,12):
    dt = [RD]
    b.extend(dt)
#RunDate = b
df1['RunDate'] = b
df1['MonthDate']=df1.index
df1['MonthDate']=pd.to_datetime(df1['MonthDate'])
print("savingdata data")	
#df['Rundate'] = RunDate
#np.savetxt('\\\JWTCVDMEDB03\\Fundamentals\\PopData\\'+SaveName+'.txt', df1,delimiter="\t")
# df1.to_csv('\\\JWTCVDMEDB03\\Fundamentals\\PopData\\'+SaveName+'.csv',  index=True, header=True)
df1.to_csv('S:\POWER OPERATIONS\FUNDIES\\Python\\'+SaveName+'.csv',  index=True, header=True)
#df1.to_csv('\\\JWTCVDMEDB03\\Fundamentals\\PopData\\'+SaveName+'.csv',  index=True, header=True)
#dfn.to_csv('S:\POWER OPERATIONS\FUNDIES\\Python\\FILELIST.csv', index= False, header=False )
#dfn.to_csv('\\\JWTCVDMEDB03\\Fundamentals\\PoPData\\FILELIST.csv', index= False, header=False )
print(SaveName)
names.append(SaveName)
os.system("taskkill /f /im  EXCEL.EXE")


params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")
conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

df1.to_sql('PoPs_Landing_2', engine, schema = 'dbo', index = False, if_exists='replace')





import time
time.sleep(5)
sql = 'EXEC dbo.upload_pops'

cursor = conn.cursor()
cursor.execute(sql)
conn.commit()
conn.close()


##file_mod_time=os.path.getmtime('C:\\Users\\E09138\\Documents\\POPS v20180627.xlsx')
##print(datetime.datetime.fromtimestamp(file_mod_time))
#FilePath= 'S:\\RISKMGT\\PowerOps\\2019 POPS\\Daily POPS copy'
#a = []
#b = []
#dt = []
#names = []
#for root, dirs, files in os.walk(FilePath):  
#    for filename in files:
#        a.append(filename)

#Colnames = ['LineItem', '1/1/2019', '2/1/2019', '3/1/2019', '4/1/2019', '5/1/2019', '6/1/2019', '7/1/2019'
#            , '8/1/2019', '9/1/2019', '10/1/2019', '11/1/2019', '12/1/2019']
#x=len(a)
#for i in range (max(0,x-10),x):
#    FileName= a[i]
#    b = []
#    dt = []
#    Date = pd.read_excel(FilePath+'\\'+FileName,
#                sheet_name = 'Summary',
#                header = 1,
# 		 usecols = "B,C",
#                Index_col = "B",
#                skiprows = (1))
#    table = pd.read_excel(FileName2,
#            sheet_name = 'Summary',
#            #range = ['D5:P5'])
#            header = None,
#            names = Colnames,
#            #index_col = 3,
#            usecols = "C,D,E, F,G,h,i,j,k,l,m,n,o",
#            skiprows = (0,1,2,3,4,5,6),
#	    convert_float = False)
#    SaveName = a[i][:-5]
#    RD= Date['Data As Of:'][0]

#    table2 = table.iloc[:41]
#    table2 = table2.dropna(subset=['LineItem'])

#    df=pd.DataFrame(table2)
#    df=df.replace('/','',regex = True)  
#    df=df.replace(' ','_',regex = True)
#    df2=df.fillna(0)

#    df1=df2.reset_index().pivot_table(values = ['1/1/2019', '2/1/2019', '3/1/2019', '4/1/2019', '5/1/2019', '6/1/2019', '7/1/2019'
#        , '8/1/2019', '9/1/2019', '10/1/2019', '11/1/2019', '12/1/2019'], columns='LineItem')
#    for d in range (0,12):
#        dt = [RD]
#        b.extend(dt)
#    #RunDate = b
#    df1['RunDate'] = b
#    df1['MonthDate']=df1.index
#    df1['MonthDate']=pd.to_datetime(df1['MonthDate'])
#    #df['Rundate'] = RunDate
#    #np.savetxt('\\\JWTCVDMEDB03\\Fundamentals\\PopData\\'+SaveName+'.txt', df1,delimiter="\t")
#    df1.to_csv('\\\JWTCVDMEDB03\\Fundamentals\\PopData\\'+SaveName+'.csv',  index=True, header=True)
#    df1.to_csv('S:\POWER OPERATIONS\FUNDIES\\Python\\'+SaveName+'.csv',  index=True, header=True)
#    print(SaveName)
#    names.append(SaveName)
#dfn=pd.DataFrame(names)

#dfn.to_csv('S:\POWER OPERATIONS\FUNDIES\\Python\\FILELIST.csv', index= False, header=False )
##dfn.to_csv('\\\JWTCVDMEDB03\\Fundamentals\\PoPData\\FILELIST.csv', index= False, header=False )
##a[len(a)-5:len(a)]

##FilePath= 'S:\\RISKMGT\\PowerOps\\2018 POPS\\Daily POPS copy'
##FileName=

##a = []
##b = []
##for root, dirs, files in os.walk('S:\\RISKMGT\\PowerOps\\2018 POPS\\Daily POPS copy'):  
# #   for filename in files:
# #       a.append(filename)

##a[len(a)-5:len(a)]


##RunDate = pd.read_excel('C:\\Users\\E09138\\Documents\\POPS v20180627.xlsx',
 #                sheet_name = 'Summary',
  #              header = 0,
   #             usecols = "C,D,E, F,G,h,i,j,k,l,m,n,o",
    #            skiprows = (1),
     #           skip_footer=(53),
      #          convert_float = False)

##table = pd.read_excel('C:\\Users\\E09138\\Documents\\POPS v20180627.xlsx',
   #               sheet_name = 'Summary',
                   #range = ['D5:P5'])
    #              header = 0,
     #               index_col = 4,
      #          usecols = "C,D,E, F,G,h,i,j,k,l,m,n,o",
       #         skiprows = (2,3,5,6,10,15,16,25,32,33,45,46,47,48,49,50,51,52,53,54,55,56,57,58),
        #          convert_float = False)

##df=pd.DataFrame(table)
#df['Rundate'] = RunDate

#np.savetxt('C:\\Users\\E09138\\Documents\\POPsTest.txt', df, fmt='%s',delimiter="\t")

#os.walk('S:\\RISKMGT\\PowerOps\\2018 POPS\\Daily POPS copy')

#a = []
#b = []
#for root, dirs, files in os.walk('S:\\RISKMGT\\PowerOps\\2018 POPS\\Daily POPS copy'):  
 #   for filename in files:
 #       a.append(filename)

