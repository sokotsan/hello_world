# -*- coding: utf-8 -*-
"""
Created on Sun Nov 20 20:45:41 2022

@author: zpfundisql
"""

#%%
import selenium
from selenium import webdriver
from time import sleep

from selenium.webdriver.common.by  import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

#%%
login='sergei.kotsan@pgn.com'
passw='Meteor2007'
PATH='D:\\DASH\\selenium\\chromedriver.exe'
driver=webdriver.Chrome(PATH)

#options = Options()
#options.add_argument("start-maximized")
#driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)


#%


driver.get('https://xtraders.meteologica.com/')
driver.maximize_window()
#%
print(driver.title)
driver.find_element(By.ID,"username").send_keys(login)
driver.find_element(By.ID,"password").send_keys(passw)
sleep(3)
#driver.find_element(By.XPATH,"//*[@id="signin"]).click()
driver.find_element(By.ID,"signin").submit()
sleep(12)
driver.find_element(By.ID,"tab_241943").click()
sleep(10)
# get the image source
image=driver.find_element(By.ID,"W_CONTENT1441221")
image.screenshot("D:\\DASH\\Pics\\Wind_fcst.png")
#%%
driver. quit() 