from urllib.request import urlopen as uReq
from bs4 import BeautifulSoup
import pandas as pd
import pyodbc
from datetime import datetime as dt
from sqlalchemy import create_engine
import urllib
import traceback
#display_errors = On
fl = 0
for f in range(1,24):
    try:

        TM = [0,48,96]
        loc = ['Roosevelt',
        'Naselle',
        'Vanscyle,KUJ/Butler',
        'Wasco', 
        'Shaniko',
        'Goodnoe Hill',
        'Kennewick',
        'Cal _6',
        'Cal _1',
        'Cal _2',
        'Cal _3',
        'Cal _4',
        'Cal _5',
        'Cal _7',
        'Cal _8',
        'Cal _9',
        'Cal _10',
        'Cal _11',
        'Cal _12',
        'Cal _13',
        'Cal _14',
        'AZPS_S_1',
        'AZPS_S_2',
        'AZPS_S_3',
        'AZPS_S_4',
        'AZPS_S_5',
        'AZPS_S_6',
        'LADWP_S1',
        'LADWP_S2',
        'SRP_S_1',
        'SRP_S_2',
        'SRP_S_3',
        'PSCO_S_1',
        'PSCO_S_2',
        'PSCO_S_3',
        'PSCO_S_4',
        'NEVP_S_1',
        'NEVP_S_2',
        'NEVP_S_3',
        'NEVP_S_4',
        'PAC_S_1',
        'PAC_S_2',
        'PAC_S_3',
        'PAC_S_4',
        'PAC_S_5',
        'IPCO_S_1',
        'IPCO_S_2',
        'SPP_S_1',
        'SPP_S_2',
        'SPP_S_3',
        'SPP_S_4',
        'EPE_S_1',
        'EPE_S_2',
        'TEPC_S_1',
        'TEPC_S_2',
        'WACM_S_1',
        'WACM_S_2',
        'PNM_S_1',
        'PNM_S_2',
        'WACM2_S_1',
        'WACM3_S_2',
        'NWMT_S_1',
        'AZPS_W_1',
        'AZPS_W_2',
        'AZPS_W_3',
        'AZPS_W_4',
        'PSCO_W_1',
        'PSCO_W_2',
        'PSCO_W_3',
        'PSCO_W_4',
        'PSCO_W_5',
        'PSCO_W_6',
        'NEVP_W_1',
        'PAC_W_1',
        'PAC_W_2',
        'PAC_W_3',
        'PAC_W_4',
        'PAC_W_5',
        'PAC_W_6',
        'PAC_W_7',
        'IPCO_W_1',
        'IPCO_W_2',
        'IPCO_W_3',
        'IPCO_W_4',
        'SPP_W_1',
        'SPP_W_2',
        'SPP_W_3',
        'SPP_W_4',
        'TEPC_W_1',
        'TEPC_W_2',
        'WACM_W_1',
        'WACM_W_2',
        'WACM_W_3',
        'WACM_W_4',
        'PNM_W_1',
        'PNM_W_2',
        'PNM_W_3',
        'NWMT_W_1',
        'NWMT_W_2',
        'NWMT_W_3',
        'NWMT_W_4',
        'NWMT_W_5',
        'LDWP_S_1',
        'LDWP_S_2',
        'BANC_S_1',
        'LDWP_S_3',
        'LDWP_S_4',
        'PCW_S_1',       
        'PCW_S_2',
        'AVA_S_1',
        'PGE_S_1',       
        'PGE_S_2',
        'PGE_S_3',
        'IPCO_S_3',
        'AVA_W_1',
        'PSEI_W_1',
        'Tuc_W_1',
        'Cal_15',
        'Cal_16',
        'Cal_17',
        'Cal_18',
        'Cal_19',
        'Cal_20',
        'Cal_21',
        'Cal_22']           


        lat=['45.764636',
        '46.421801',
        '45.950084',
        '45.500263',
        '45.02515',
        '45.783358',
        '46.10007',
        '33.08053',
        '34.007436',
        '36.949103',
        '35.979662',
        '35.22467',
        '34.669895',
        '36.792073',
        '33.236057',
        '36.065817',
        '32.845986',
        '35.688929',
        '33.643287',
        '34.863682',
        '35.254006',
        '33.188846',
        '35.213868',
        '32.95036',
        '32.66279',
        '34.812273',
        '33.306992',
        '36.036797',
        '36.506083',
        '33.188846',
        '32.95036',
        '33.306992',
        '37.690755',
        '38.268517',
        '39.709087',
        '39.889275',
        '36.036797',
        '38.23951',
        '39.554522',
        '38.5807',
        '37.969431',
        '38.5125',
        '39.13987',
        '37.64007',
        '41.62828',
        '43.236837',
        '42.82696',
        '33.374617',
        '36.042386',
        '36.042386',
        '34.98278',
        '32.808628',
        '32.025505',
        '32.155144',
        '32.051446',
        '39.889275',
        '37.36726',
        '34.958626',
        '32.808628',
        '32.051446',
        '35.233865',
        '46.75317',
        '34.62735',
        '34.481373',
        '35.485275',
        '35.14357',
        '39.257311',
        '40.925607',
        '40.921374',
        '37.803047',
        '39.2928',
        '37.923674',
        '39.10709',
        '42.938754',
        '41.753219',
        '38.522804',
        '41.284398',
        '43.402403',
        '43.568887',
        '37.89592',
        '42.910599',
        '42.702472',
        '42.514546',
        '43.182915',
        '33.967732',
        '34.807625',
        '33.155695',
        '39.335747',
        '32.55173',
        '32.29912',
        '39.335747',
        '40.925607',
        '37.803047',
        '41.135205',
        '34.918595',
        '35.2687',
        '34.81102',
        '48.600004',
        '46.56667',
        '46.135146',
        '47.415017',
        '46.200536',
        '35.2513',
        '35.1706',
        '37.7371',
        '35.8894',
        '36.50608',
        '44.31068',     
        '43.07607',
        '46.98333',     
        '43.83695',
        '43.25536',
        '45.13409',
        '43.320437',
        '47.161912',
        '47.02036',
        '46.431343',
        '34.86368',
        '37.96475',
        '45.76052',
        '45.6473',
        '32.84599',
        '34.73877',
        '38.5228',
        '35.01538']

        lon=['-120.24008',
        '-123.79690',
         '-118.68341',
        '-120.76687',
        '-120.83532',
        '-120.55023',
        '-119.11692',
        '-115.4724',
        '-116.965667',
        '-121.0289',
        '-119.243254',
        '-118.952362',
        '-117.286829',
        '-120.095137',
        '-113.042638',
        '-119.920269',
        '-115.870352',
        '-115.222315',
        '-114.85683',
        '-118.195333',
        '-119.837893',
        '-112.924458',
        '-114.028977',
        '-111.390735',
        '-114.464875',
        '-112.181136',
        '-111.981643',
        '-115.04639',
        '-114.766188',
        '-112.924458',
        '-111.390735',
        '-111.981643',
        '-105.975903',
        '-104.557163',
        '-104.262635',
        '-104.792576',
        '-115.04639',
        '-117.36217',
        '-118.661444',
        '-118.19341',
        '-113.076683',
        '-113.032',
        '-112.332065',
        '-113.62941',
        '-109.68329',
        '-116.08137',
        '-112.74394',
        '-104.436188',
        '-104.905278',
        '-104.905278',
        '-103.37833',
        '-107.353673',
        '-106.79067',
        '-110.958888',
        '-110.071248',
        '-104.792576',
        '-104.47299',
        '-106.697341',
        '-107.353673',
        '-110.071248',
        '-114.745865',
        '-112.156215',
        '-110.22515',
        '-105.966311',
        '-112.300873',
        '-114.0649',
        '-103.789304',
        '-103.037082',
        '-103.969424',
        '-102.669456',
        '-103.38396',
        '-104.663653',
        '-114.51802',
        '-105.937468',
        '-106.168916',
        '-112.942958',
        '-110.63785',
        '-111.812179',
        '-111.804857',
        '-109.374231',
        '-115.176674',
        '-112.763729',
        '-113.89064',
        '-116.472781',
        '-103.644491',
        '-103.257691',
        '-103.273765',
        '-102.283161',
        '-107.51284',
        '-110.09826',
        '-102.283161',
        '-103.037082',
        '-102.669456',
        '-104.933652',
        '-103',
        '-107',
        '-105',
        '-112.097814',
        '-109.76214',
        '-109.487501',
        '-111.038034',
        '-110.058666',
        '-117.9683',
        '-118.0516',
        '-120.9928',
        '-114.95791',
        '-114.766188',
        '-120.82422',     
        '-119.67597',
        '-118.61503',
        '-120.8555',     
        '-120.458625',
        '-123.075656',
        '-120.042159',
        '-117.348669',
        '-120.22394',
        '-118.06668',
        '-118.195',
        '-121.96',
        '-120.155',
        '-120.652',
        '-115.87',
        '-103.124',
        '-112.943',
        '-105.565'
             ]
        
        url1 = "https://forecast.weather.gov/MapClick.php?w0=t&w1=td&w2=wc&w3=sfcwind&w3u=1&w4=sky&w5=pop&w6=rh&w7=rain&w9=snow&w10=fzg&w11=sleet&w12=fog&w13u=0&w16u=1&AheadHour="
        url2 = "&Submit=Submit&FcstType=digital&textField1="
        url3= "&textField2="
        url4 = "&site=all&unit=0&dd=&bw="
        dic = {
              "DT":[],
              "HB":[],
              "Loc":[],
              "Met":[],
              "Value":[]}
        Cols = ['DT','HE','Loc','Met','Value']
        #WX = pd.DataFrame(columns=Cols)
        
        for l in range(0,len(loc)):
        #for l in range(0,1):    
            try:
            
                for t in TM:
                    url = url1+str(t)+url2+str(lat[l])+url3+str(lon[l])+url4
                    #url = "https://forecast.weather.gov/MapClick.php?w0=t&w1=td&w2=wc&w3=sfcwind&w3u=1&w4=sky&w5=pop&w6=rh&w7=rain&w8=thunder&w9=snow&w10=fzg&w11=sleet&w13u=0&w16u=1&AheadHour=0&Submit=Submit&FcstType=digital&textField1=45.21&textField2=-123.75&site=all&unit=0&dd=&bw="
                    uClient = uReq(url)
                    page_html = uReq(url)
                    uClient.close()
                    soup = BeautifulSoup(page_html, 'html.parser')
                    #body = soup('table')[7]
                    tr = soup.find_all('tr', attrs={"align":"center"})
                    #tr = body.find_all('tr')
                    HE = []
                    DT = []
                    MET = ['Temp','DewPnt','WndChl','Wind','WindDir','Gust','SkyCvr'
                      ,'PrecipPtnl','RltHum','Rain','Snow','FrzRain','Sleet','Fog'] 
                    active = tr[1]
                    active2 = active.find_all('td')
                    for i in range(1,25):
                        HE.append(active2[i].get_text())
                    active = tr[0]
                    active2 = active.find_all('td')

                    for i in range(1,25):
                        if active2[i].get_text().lstrip():
                            DT.append(active2[i].get_text())
                        else:
                            b= i-2
                            DT.append(DT[b])

                    for i in range(0,len(MET)):
                        b=i+2
                        active = tr[b]
                        active2 = active.find_all('td')
                        for c in range (1,25):
                            if int((DT[c-1])[:2])< dt.now().month:
                                dty = str((dt.now().year)+1)
                            else:
                                dty = str(dt.now().year)
                            if active2[c].get_text().lstrip():
                                dic["DT"].append((DT[c-1])+'/'+dty)            
                                dic["HB"].append(HE[c-1])
                                dic["Loc"].append(l)
                                dic["Met"].append(MET[i])
                                dic["Value"].append(active2[c].get_text())
                    LD = DT[23]        
                #WXfcst = pd.DataFrame(dic)


                    HE = []
                    DT = []

                    active = tr[17]
                    active2 = active.find_all('td')
                    for i in range(1,25):
                        HE.append(active2[i].get_text())
                    active = tr[16]
                    active2 = active.find_all('td')

                    for i in range(1,25):
                        if active2[i].get_text().lstrip():
                            DT.append(active2[i].get_text())
                            LD=active2[i].get_text()
                        else:
                            DT.append(LD)

                    for i in range(0,len(MET)):
                        b=i+18
                        active = tr[b]
                        active2 = active.find_all('td')
                        for c in range (1,25):
                            if int((DT[c-1])[:2])< dt.now().month:
                                dty = str((dt.now().year)+1)
                            else:
                                dty = str(dt.now().year)
                            if active2[c].get_text().lstrip():
                                dic["DT"].append((DT[c-1])+'/'+dty)            
                                dic["HB"].append(HE[c-1])
                                dic["Loc"].append(l)
                                dic["Met"].append(MET[i])
                                dic["Value"].append(active2[c].get_text())
                print(loc[l])
            except: 
                print('Failed' + str(f)+str(loc[l]))
                from selenium import webdriver
                chromedriver = 'C:/Users/E09138/AppData/Local/Programs/Python/Python37/TSK/chromedriver'
                driver = webdriver.Chrome(chromedriver)
                driver.get("https://forecast.weather.gov/MapClick.php?w0=t&w1=td&w2=wc&w3=sfcwind&w3u=1&w4=sky&w5=pop&w6=rh&w7=rain&w8=thunder&w9=snow&w10=fzg&w11=sleet&w13u=0&w16u=1&AheadHour=0&Submit=Submit&FcstType=graphical&textField1=45.764636&textField2=-120.24008&site=all&unit=0&dd=&bw=")
                fl = 1
                pass
            
               
        WXfcst = pd.DataFrame(dic)



#dtm = dt.now().month
#dty = (dt.now().year)+1

#engine = sqlalchemy.create_engine('mssql://JWTCVPMEDB03/Fundamentals?trusted_connection=yes')

        params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

        engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

        WXfcst.to_sql('NOAA_HRLY_WX_FC_Landing', engine, schema = 'dbo', index = False, if_exists='replace')

        conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")

        sql = 'EXEC dbo.NOAA_WX_FCST_Upload'

        #cursor = connection.cursor()
        #connection.execute("EXEC dbo.NOAA_WX_FCST_Upload")


        #df2 = pd.DataFrame({"colw":[1],"col2":[2]})


        #conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
         #                                "SERVER=JWTCVPMEDB03;"
          #                               "DATABASE=Fundamentals;"
           #                              "trusted_connection=yes")
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        conn.close()



        #WXfcst = pd.DataFrame(dic)
        #print(f)
        #f=4
        print('success')
        break
    except:
        print('FailedCritical' + str(f)+str(loc[1]))
        from selenium import webdriver
        chromedriver = 'C:/Users/E09138/AppData/Local/Programs/Python/Python37/TSK/chromedriver'
        driver = webdriver.Chrome(chromedriver)
        driver.get("https://forecast.weather.gov/MapClick.php?w0=t&w1=td&w2=wc&w3=sfcwind&w3u=1&w4=sky&w5=pop&w6=rh&w7=rain&w8=thunder&w9=snow&w10=fzg&w11=sleet&w13u=0&w16u=1&AheadHour=0&Submit=Submit&FcstType=graphical&textField1=45.764636&textField2=-120.24008&site=all&unit=0&dd=&bw=")
        fl = 1
        
        pass

if fl == 1:
    driver.quit()
#if 

 
#cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
#                      "Server=JWTCVPMEDB03;"
#                      "Database=Fundamentals;"
#                      "Trusted_Connection=yes;")


#cursor = cnxn.cursor()
#cursor.execute('SELECT * FROM Table')





        
    

