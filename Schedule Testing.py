import win32com.client as win32


def Email_Sender():
    outlook = win32.Dispatch('outlook.application')
    mail = outlook.CreateItem(0)
    mail.SentOnBehalfOfName = 'HS_WFM_FieldSupport@homedepot.com'
    mail.To = 'patrick_m_callahan@homedepot.com'
    mail.Subject = 'TEST TEST TEST'
    mail.Body = "This is only a test"
    mail.Send()
    

print('Test')
Email_Sender()