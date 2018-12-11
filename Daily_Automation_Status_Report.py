import win32com.client as win32
from datetime import datetime as dt

def Daily_Message():
    date = str(dt.today().month) + '/' + str(dt.today().day) + '/' + str(dt.today().year)
    outlook = win32.Dispatch('outlook.application')
    mail = outlook.CreateItem(0)
    mail.SentOnBehalfOfName = 'HS_WFM_FieldSupport@homedepot.com'
    mail.To = 'HS_WFM_FieldSupport@homedepot.com'
    mail.CC = "PATRICK_M_CALLAHAN@homedepot.com; MATTHEW_MURR@homedepot.com"
    mail.Subject = 'Automated Update'
    mail.Body = f'The automated reporting system is still online as of {date}'
    mail.Send()