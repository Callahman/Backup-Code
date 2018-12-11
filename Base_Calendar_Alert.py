import pandas as pd
from datetime import datetime as dt
import salesforce_bulk as sf
from time import sleep
import os
import win32com.client as win32
import pickle
from datetime import timedelta
import dateutil.parser
from datetime import timezone
import configparser

os.environ['REQUESTS_CA_BUNDLE'] = 'C:\\Users\\pmc1104\\OneDrive - The Home Depot\\Documents\\RootCA.pem'


## PRODUCTION ENV
config = configparser.ConfigParser()  
config.read('C:/Users/pmc1104/OneDrive - The Home Depot/Documents/Projects/salesforce_config.ini')

username = config['Info']['Username']
password = config['Info']['Password']
security_token = config['Info']['Security_Token']
client = config['Info']['Client']
secret = config['Info']['Secret']

###############################################################################
###############################################################################

def SF_Dataframe(query_list, object_list):
    print('Establishing Connection to Salesforce')
    bulk = sf.SalesforceBulk(username = username, password = password,
                             security_token = security_token)
    print('Connection Established')
    
    def Run_SF_Query(query, object_name):
        job_id = bulk.create_query_job(object_name = object_name)
        
        batch_id = bulk.query(job_id = job_id, soql = query)
        
        bulk.close_job(job_id)
        
        while not bulk.is_batch_done(batch_id):
            sleep(1)
        
        results = bulk.get_all_results_for_query_batch(batch_id)
        
        for result in results:
            df = pd.read_csv(result)
            break
        
        return df
    
    dfs = []
    for query, obj in zip(query_list, object_list):
        df = Run_SF_Query(query, obj)
        print('-------------------------------------------')
        print('%s object has been queried.' % obj)
        print('-------------------------------------------')
        print()
        
        df.drop_duplicates(inplace = True)
        dfs.append(df)
    
    return dfs


def Clean_Date(x):
    d = dateutil.parser.parse(x)
    d = d.replace(tzinfo=timezone.utc).astimezone(tz=None)
    return dt(d.year, d.month, d.day)

def Clean_Time(x):
    d = dateutil.parser.parse(x)
    d = d.replace(tzinfo=timezone.utc).astimezone(tz=None)
    return dt(d.year, d.month, d.day, d.hour, d.minute, d.second)
    

def BASE_CALENDAR_DELETED():
    subject = 'URGENT: CALENDAR DELETED!!!'
    body = """<p><b>URGENT!!!</b></p>

<p>Our automated system has detected that the Salesforce Base Calendar has been deleted.</p>

<p>Please take immediate action and bring the Base Calendar back online.</p>
<p>Check the <a href = "https://homeservices.my.salesforce.com/search/UndeletePage">Salesforce Recycle Bin</a> to see if it can be restored from there.</p>

<p>Be sure to reach out to management & IT resources to establish that the calendar is back online.</p>

<p>Thank you!</p>
"""
    return subject, body


def BASE_CALENDAR_MODIFIED(user, email, time):
    time = time - timedelta(hours = 4)
    subject = 'URGENT: CALENDAR MODIFIED!!!'
    body = f"""<p><b>URGENT!!!</b></p>

<p>Our automated system has detected that the Salesforce Base Calendar has been Modified.</p>

<p>The changes were made by {user} at the following time: {time}</p>

<p>Please reach out to {user} ({email}) and validate that these changes are correct. Otherwise, remove any changes made to the calendar.</p>

<p>Thank you!</p>
"""
    return subject, body


def Report():
    query = ["""
    select Name, CreatedDate, LastModifiedDate, SystemModstamp, CKSW_BASE__Description__c, LastModifiedBy.Name, LastModifiedBy.Email
    from CKSW_BASE__Calendar__c
    where Name like 'Base Calendar'
    """]
    obj = ['CKSW_BASE__Calendar__c']
    
    df = SF_Dataframe(query, obj)[0]
    
    df.columns = ['Name','Created','Modified','System','Description','User','Email']
    df['Created Time'] = df['Created'].apply(Clean_Time)
    df['Created'] = df['Created'].apply(Clean_Date)
    df['Modified Time'] = df['Modified'].apply(Clean_Time)
    df['Modified'] = df['Modified'].apply(Clean_Date)
    df['System Time'] = df['System'].apply(Clean_Time)
    df['System'] = df['System'].apply(Clean_Date)
    
    
    try:
        last_error_info = pickle.load(open('C:/Users/pmc1104/OneDrive - The Home Depot/Documents/Projects/Workforce Automation/Production/error_record.pickle','rb'))
    except:
        last_error_info = {'Time':dt(1990, 1, 1), 'Culprit':'Nobody Odyseus'}
        
        
        
    
    ### Error: Calendar Deleted
    if len(df)<1:
        subject, body = BASE_CALENDAR_DELETED()
        
        text = f"""<body>
        {body}
        </body>"""
        
        outlook = win32.Dispatch('outlook.application')
        mail = outlook.CreateItem(0)
        mail.SentOnBehalfOfName = 'HS_WFM_FieldSupport@homedepot.com'
        mail.To = 'HS_WFM_FieldSupport@homedepot.com; MATTHEW_MURR@homedepot.com'
        mail.BCC = 'Patrick_M_Callahan@homedepot.com'
        
        mail.Subject = subject
        mail.HTMLBody = text
        mail.Importance = 2
        mail.Send()
        
        last_error_time = dt(dt.today().year, dt.today().month, dt.today().day, dt.today().hour, dt.today().minute)
        last_error_info = {'Time':last_error_time, 'Culprit':'Calendar Deleted'}
        pickle.dump(last_error_info, open('C:/Users/pmc1104/OneDrive - The Home Depot/Documents/Projects/Workforce Automation/Production/error_record.pickle', 'wb'))
        return None
        
    
    
    
    
    modified_date = df.loc[0, 'Modified']
    modified_date = dt(modified_date.year, modified_date.month, modified_date.day, modified_date.hour)
    today = dt.today()
    
    error_time = last_error_info['Time']
    
    
    ### Error: Calendar Altered
    if modified_date.year == today.year \
        and modified_date.month == today.month \
        and modified_date.day == today.day \
        and error_time != modified_date:
        
        
        email = df.loc[0, 'Email']
        time = df.loc[0, 'Modified Time']
        user = df.loc[0, 'User']
        subject, body = BASE_CALENDAR_MODIFIED(user, email, time)
        
        
        
        text = f"""<body>
        {body}
        </body>"""
        
        outlook = win32.Dispatch('outlook.application')
        mail = outlook.CreateItem(0)
        mail.SentOnBehalfOfName = 'HS_WFM_FieldSupport@homedepot.com'
        mail.To = 'HS_WFM_FieldSupport@homedepot.com; MATTHEW_MURR@homedepot.com'
        mail.CC = email
        mail.BCC = 'Patrick_M_Callahan@homedepot.com'
        
        mail.Subject = subject
        mail.HTMLBody = text
        mail.Importance = 2
        mail.Send()
        
        last_error_info = {'Time':modified_date, 'Culprit':'Calendar Deleted'}
        pickle.dump(last_error_info, open('C:/Users/pmc1104/OneDrive - The Home Depot/Documents/Projects/Workforce Automation/Production/error_record.pickle', 'wb'))
        return None
    
    
    print('No Errors Found: On Hold')
    return None