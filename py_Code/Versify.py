
"""
Spyder Editor

This is a temporary script file.
"""

#pip install opencv-python
#import cv
import urllib
from selenium import webdriver
from time import sleep

from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
import glob, os
import pandas as pd
import numpy as np
import datetime as dt
#%%
download_path='C:\\Users\\zpfundisql\\Downloads\\'
export_path='D:\\DASH\\Pics\\'
ymd_str=dt.date.today().strftime('%Y%m%d')
#%%
login='sergei.kotsan@pgn.com'
passw='Meteor$2007'
PATH='D:\\DASH\\selenium\\chromedriver.exe'
driver=webdriver.Chrome(PATH)
#action = ActionChains(driver)
 #%


driver.get('https://por.oms.mcgware.com/app/login')
driver.maximize_window()
#%
print(driver.title)
driver.find_element(By.ID,"username").send_keys(login)
driver.find_element(By.ID,"pwd").send_keys(passw)
sleep(13)

#driver.find_element(By.XPATH,"//*[@id="signin"]).click()
driver.find_element(By.CLASS_NAME,"btn__text").click()
sleep(15)
driver.find_element(By.XPATH,'//*[@id="app-leftmenu"]/div/kendo-menu/ul/li[1]/a/a/span').click()
sleep(10)
#Press Export Formatted
driver.find_element(By.XPATH,'//*[@id="page-body"]/main/app-csgitool/div/div[1]/div/div[2]/div/app-buttons/ul[2]/li/a/div').click()
sleep(10)
#%
driver. quit() 
#%%

os.chdir(download_path)
list_xls=glob.glob('*.xls')
print(list_xls[0])

#%%
sub_list=[
           'DEL_Biglow Canyon AWS P50', 'Biglow Canyon Met P50'
          , 'DEL_Tucannon  AWS P50', 'Tucannon  Met P50'
          , 'DEL_Wheatridge 1 AWS P50', 'Wheatridge 1 Met P50'
          ]
#%%
df_in=pd.read_excel(download_path+list_xls[0])
df_sub=df_in[df_in['Description'].isin(sub_list)].copy()
df_sub['DESCRIPTION']=df_in['Description'].str[:-7]
df2=df_sub.groupby(['DESCRIPTION']).mean().copy()
#%%
df3=df2[df2.index==df2.index[2]]*1.9
df3.reset_index(inplace=True)
#%%
df3['DESCRIPTION']='Wheatridge 2 '
df4=pd.concat([df2.reset_index(),df3],ignore_index=True)
df4.loc['Total']=df4.sum(numeric_only=True)
df4['DESCRIPTION'].fillna('Agg',inplace=True)
df5=df4.sort_values('DESCRIPTION')
df6=df5.round(0)
print(df6)

#%% Export
df6.to_excel(export_path+'Day_Ahead_'+ymd_str+'.xlsx', sheet_name='Sheet1', index=False)





#%%
#Delete existing files after Download
# =============================================================================
    
try:
    os.remove(download_path + list_xls[0])
except:
    print('no files are there')
