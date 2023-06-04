# Import smtplib library to send email in python.
import smtplib
# Import MIMEText, MIMEImage and MIMEMultipart module.
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
import pandas as pd
from pretty_html_table import build_table
import sqlalchemy
from contextlib import suppress
from sqlalchemy import create_engine
import pyodbc
import os, sys
import datetime as dt
#%%
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 1000)

ymd_str=dt.date.today().strftime('%Y%m%d')
print(ymd_str)
#%%
import cx_Oracle
try:
    cx_Oracle.init_oracle_client(lib_dir=r"D:\Oracle\instantclient_21_6")
except:
    print('Oracle Client intialized')    
print(sys.version)
#%%

dsn_tns = cx_Oracle.makedsn('xpmedbe02', '1521', service_name='merch01p')
conn = cx_Oracle.connect(user=r'zprmrcmotr', password='w28sdLMswa23$pQSRB', dsn=dsn_tns)


sql_Latest_TEMP="""
select PGE_DATE
, MAX("Forecast_value") as High_Temp 
, MIN("Forecast_value") as Low_Temp
from motrview.KPDX_TEMP 
where to_char(current_date)=to_char(CAST(("Last_change") as DATE))
and TO_CHAR("Last_change", 'HH24')='03'
and PGE_DATE>=current_date
and PGE_DATE<=current_date+5
group by PGE_DATE
order by 1
"""
TEMP_forw=pd.read_sql_query(sql_Latest_TEMP,conn)
TEMP_forw['PGE_DATE']=TEMP_forw['PGE_DATE'].dt.strftime('%d-%b-%y')
TEMP_forw.columns=['Forecast Date', 'High Temp', 'Low Temp']

print(TEMP_forw)

def get_gdp_data():
  
    data = pd.DataFrame(TEMP_forw)
    return data

#%%
def send_mail(output):
    """
    Construct email body to send
    """

    # Define the source and target email address.
    strFrom = 'fundamentals@pgn.com'
    recipients =  ['ruth.burris@pgn.com', 'sergei.kotsan@pgn.com', 
                   'christopher.white@pgn.com', 'sophiya.vhora@pgn.com', 'barrett.goudeau@pgn.com' ]
                                      
    #recipients =  ['sergei.kotsan@pgn.com']

    # Create an instance of MIMEMultipart object, pass 'related' as the constructor parameter.
    msgRoot = MIMEMultipart('mixed')
    
    msgRoot['Subject'] = 'Day-Ahead Wind Forecast on ' + dt.date.today().strftime('%d-%b-%Y')
    msgRoot['From'] = strFrom
    msgRoot['To'] = ", ".join(recipients)

    # Set the multipart email preamble attribute value. Please refer https://docs.python.org/3/library/email.message.html to learn more.
    msgRoot.preamble = '=====================================================' 
    msgAlternative = MIMEMultipart('alternative')
    msgText = MIMEText('''<h1 style="text-align: left;">Good Morning, </h1>
                        <p>Automated morning forecast is attached below </p>  {0} 
                        <p>==================================================</p>
                        <br><img src="cid:image1"><br>
                       <b>Best wishes from PYTHON,</b>
                       <p>on behalf of Sergei Kotsan</p>
                       '''.format(output), 'html')


    msgAlternative.attach(msgText)
    msgRoot.attach(msgAlternative)


    fp = open('D:\DASH\Pics\\Wind_fcst.png', 'rb')
    msgImage = MIMEImage(fp.read())
    fp.close()

    msgImage.add_header('Content-ID', '<image1>')
    msgRoot.attach(msgImage)

    file_name = 'D:\DASH\Pics\\'+'Day_Ahead_'+ymd_str+'.xlsx'
    file_attach='Day_Ahead_'+ymd_str+'.xlsx'
    part = MIMEBase('application', "octet-stream")
    part.set_payload(open(file_name, "rb").read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment; filename={0}'.format(file_attach))
    msgRoot.attach(part)


    smtp = smtplib.SMTP()
    smtp.connect('smtp.corp.dom')
    smtp.sendmail(strFrom, recipients, msgRoot.as_string())
    smtp.quit()

if __name__ == '__main__':

    # your dataframe building
    gdp_data = get_gdp_data()
    output = build_table(gdp_data, 'blue_light', width_dict=['110px','auto','auto'], text_align='center')
    try:
        send_mail(output)
        print("Mail sent successfully.")
    except:
        print("Mail failed to sent!")