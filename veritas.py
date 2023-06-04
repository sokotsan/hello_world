# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import cv
#pip install opencv-python
import cv2
import urllib
from selenium import webdriver
from time import sleep

from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains

#%%
login='sergei.kotsan@pgn.com'
passw='Meteor$2007'
PATH='/media/skotsan/762AF66D2AF62A2F1/Program Files (x86)/chromedriver.exe'
driver=webdriver.Chrome(PATH)
action = ActionChains(driver)
 #%


driver.get('https://por.oms.mcgware.com/app/login')
driver.maximize_window()
#%
print(driver.title)
driver.find_element(By.ID,"username").send_keys(login)
driver.find_element(By.ID,"pwd").send_keys(passw)
sleep(3)

#driver.find_element(By.XPATH,"//*[@id="signin"]).click()
driver.find_element(By.CLASS_NAME,"btn__text").click()
sleep(5)
driver.find_element(By.XPATH,'//*[@id="app-leftmenu"]/div/kendo-menu/ul/li[1]/a/a/span').click()
sleep(10)

driver.find_element(By.XPATH,'//*[@id="page-body"]/main/app-csgitool/div/div[1]/div/div[2]/div/app-buttons/ul[2]/li/a/div').click()
sleep(10)
