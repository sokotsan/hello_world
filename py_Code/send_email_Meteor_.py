# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import smtplib,ssl
import sys
import datetime as dt
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders
from pathlib import Path  
import pandas as pd
from tabulate import tabulate
import csv

date_today1=dt.date.today()#.strftime('%Y%m%d')
file_attach=Path('D:\DASH\Pics\\Sample.xlsx')
file_=Path('D:\DASH\Pics\\Sample.xlsx').name
print(file_)
TO = ['sergei.kotsan@pgn.com']

#%%
#from pretty_html_table import build_table
#df_1 = ([1,2,3,5])
#df_2 = ([10,20,30,50])
#df_test =pd.concat([pd.DataFrame(df_1),pd.DataFrame(df_2)],axis=1)
#df_test.to_csv('C:\\Users\\e77231\\OneDrive - Portland General Electric Company\\Dual Trigger\\df.csv')
#%%
def send_mail(send_from,send_to,subject,text,file_name,date_today,username='fundamentals@pgn.com',password='',isTls=True):

    sender = 'fundamentals@pgn.com'
    msg = MIMEMultipart()
    msg['Subject'] = "PYTHON from sergei  {}".format(date_today) 
    msg['From'] = sender
    #recipients = ['sergei.kotsan@pgn.com']
    recipients =  ['ruth.burris@pgn.com', 'sergei.kotsan@pgn.com', 'christopher.white@pgn.com']
    # recipients = 'Chad.Croft@pgn.com,Brandon.Humble@pgn.com,sergei.kotsan@pgn.com'
    # recipients = 'Brandon.Humble@pgn.com;sergei.kotsan@pgn.com;Chad.Croft@pgn.com'
    # recipients = 'RPM_Center@pgn.com'
    print("Sending email to " +  ", ".join(recipients))
    print(date_today)
    # sys.exit()
    # msg['To'] = recipients
    msg['To'] = ", ".join(recipients)
    with open('D:\DASH\Pics\\Wind_fcst.png', 'rb') as fp:
        img = MIMEImage(fp.read())
        img.add_header('Content-Disposition', 'attachment', filename='hours_plot.png')
        img.add_header('X-Attachment-Id', '0')
        img.add_header('Content-ID', '<0>')
        fp.close()
        msg.attach(img)

    # Attach HTML body
    msg.attach(MIMEText(
        '''
        <html>
            <body>
                <h1 style="text-align: left;">Day Ahead Wind Forecast</h1>
                <p>Testing Meteologic Wind forecast Automation.</p>
                <p><img src="cid:0"></p>
            </body>
        </html>'
        ''',
        'html', 'utf-8'))



    s = smtplib.SMTP('smtp.corp.dom')
    s.sendmail(sender, recipients, msg.as_string())
    s.quit()
#%%
send_mail('fundamentals@pgn.com', TO, 'Test', 'Test email PRB Tribes',file_attach, date_today1 )

