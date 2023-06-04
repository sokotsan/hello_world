from selenium import webdriver
chromedriver = 'C:/Users/E09138/AppData/Local/Programs/Python/Python37/TSK/chromedriver'
driver = webdriver.Chrome(chromedriver)
driver.get("https://forecast.weather.gov/MapClick.php?w0=t&w1=td&w2=wc&w3=sfcwind&w3u=1&w4=sky&w5=pop&w6=rh&w7=rain&w8=thunder&w9=snow&w10=fzg&w11=sleet&w13u=0&w16u=1&AheadHour=0&Submit=Submit&FcstType=graphical&textField1=45.764636&textField2=-120.24008&site=all&unit=0&dd=&bw=")
driver.quit()
