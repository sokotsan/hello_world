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
#%%
login='sergei.kotsan@pgn.com'
passw='Meteor2007'
PATH='/media/skotsan/762AF66D2AF62A2F1/Program Files (x86)/chromedriver.exe'
driver=webdriver.Chrome(PATH)
#%


driver.get('https://xtraders.meteologica.com/')
driver.maximize_window()
#
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
image.screenshot("image_2.png")
#loc = img.location
#print(loc)
#image = cv.LoadImage('screenshot.png', True)
#out = cv.CreateImage((150,60), image.depth, 3)
#cv2.SetImageROI(image, (loc['x'],loc['y'],150,60))
#cv2.Resize(image, out)
#cv2.SaveImage('out.jpg', out)



# download the image
#img.screenshot_as_png("image.png")
#%%%
#driver.close()

#%%
#//*[@id="W_CONTENT1441221"]
