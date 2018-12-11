import pandas as pd
from datetime import datetime as dt
import salesforce_bulk as sf
from time import sleep
import os
import win32com.client as win32
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
    y = x.split('T')[0].split('-')
    y = [int(t) for t in y]
    return dt(y[0], y[1], y[2])


def Email_Check(df):
    active = df['Active']
    
    if active:
        return df['Email']
    else:
        return df['Boss Email']


def Address(df):
    address = str(df['Street'])
    city = str(df['City'])
    state = str(df['State'])
    zip_code = str(df['Zip'])
    
    result = address +', '+ city +' '+ state +' '+ zip_code
    return result
    


def Report():
    query = ["""
    SELECT CKSW_BASE__Appointment_Start__c, Opportunity__r.IsClosed, CKSW_BASE__Location__r.Name, CKSW_BASE__Geolocation__Latitude__s, CKSW_BASE__Geolocation__Longitude__s, Lead__r.Product_Name__c, Sales_Consultant__c, CKSW_BASE__Location__r.Branch__r.Name, Name,
    Lead__r.City, Lead__r.Country, Lead__r.PostalCode, Lead__r.State, Lead__r.Street
    FROM CKSW_BASE__Service__c
    WHERE Opportunity__r.IsClosed = false AND CKSW_BASE__Appointment_Start__c >= TODAY AND Appointment_Start_Time__c <= NEXT_N_DAYS:7 AND (CKSW_BASE__Location__r.LOB__c like '%HDE%' or CKSW_BASE__Location__r.LOB__c like '%HDI%')
    """,
    """
    select Sales_Manager__r.Name, Sales_Manager__r.Email__c, Sales_Manager__r.CKSW_BASE__Parent_Location__r.Email__c, Sales_Manager__r.Owner.IsActive
    from Store_Manager__c
    """]
    obj = ['CKSW_BASE__Service__c','Store_Manager__c']
    
    dfs = SF_Dataframe(query, obj)
    df = dfs[0]
    emails = dfs[1]
    emails.drop_duplicates(inplace = True)
    
    del dfs
    
    df.columns = ['Date','Closed','Location','Branch','Lat','Long','Product','City','Country','Zip','State','Street','SC','Appt']
    emails.columns = ['Location','Email','Boss Email','Active']
    
    df['Address'] = df.apply(Address, axis = 1)
    df['Date'] = df['Date'].apply(Clean_Date)
    df.drop_duplicates(inplace = True)
    df.drop(['City','Country','Zip','State','Street'], axis = 1, inplace = True)
    
    
    temp = df[['Date','Location','Branch','Lat','Long','SC']].groupby(['Date','Location','Branch','Lat','Long']).count().reset_index()
    temp.columns = ['Date','Location','Branch','Lat','Long','Count']
    
    
    df = df.merge(temp, on = ['Date','Location','Branch','Lat','Long'], how = 'left')
    
    
    
    df = df[df['Count']>1]
    df['Count'] = 1
    
    
    
    df.drop_duplicates(inplace = True, subset = ['Date','Branch','Location','Lat','Long','SC','Count'], keep = 'first')
    temp = df[['Date','Location','Branch','Lat','Long','SC']].groupby(['Date','Location','Branch','Lat','Long']).count().reset_index()
    temp.columns = ['Date','Location','Branch','Lat','Long','Count']
    
    df.drop('Count', axis = 1, inplace = True)
    df = df.merge(temp, on = ['Date','Location','Branch','Lat','Long'], how = 'left')
    
    df.sort_values(['Date','Branch','Location','Lat','Long','SC'], inplace = True)
    
    df = df[df['Count']>1]
    
    emails = emails[emails['Location'].isin(df['Location'].unique().tolist())]
    emails['Email'] = emails.apply(Email_Check, axis = 1)
    emails = emails[['Location','Email']]
    df = df.merge(emails, on = 'Location', how = 'left')
    df.fillna('', inplace = True)
    df.drop(['Count','Closed'], axis = 1, inplace = True)
    
    if len(df) > 0:
        df.to_excel(os.path.dirname(os.path.abspath(__file__)).replace("\\", '/')+'/Duplicate Location Warning.xlsx', index = False)
        text = """<p>Hello Workforce Team,</p>
        
        <p>Your friendly neighborhood WFM Robot has identified some errors that need checking!</p>
        
        <p>The attached document identifies situations in which 2 or more SCs will be traveling to the same location in the same day.</p>
        
        <p>Please confirm that these errors will happen, and check with the associated Sales Manager on what action should be taken.</p>
        
        <p>Thank you!</p>
        """
    
        outlook = win32.Dispatch('outlook.application')
        mail = outlook.CreateItem(0)
        mail.SentOnBehalfOfName = 'HS_WFM_FieldSupport@homedepot.com'
        mail.To = 'HS_WFM_FieldSupport@homedepot.com'
        mail.Subject = 'WFM: Duplicate Location Warning'
        mail.HTMLBody = text
        mail.Attachments.Add(os.path.dirname(os.path.abspath(__file__)).replace("\\", '/')+'/Duplicate Location Warning.xlsx')
        mail.Send()
        
        os.remove(os.path.dirname(os.path.abspath(__file__)).replace("\\", '/')+'/Duplicate Location Warning.xlsx')
    
    try:
        container = pd.read_pickle(os.path.dirname(os.path.abspath(__file__)).replace("\\", '/')+'/Data Container.pickle')
        df['Created Date'] = dt(dt.today().year, dt.today().month, dt.today().day)
        df = df[~df['Appt'].isin(container['Appt'].unique().tolist())]
        if len(df) > 0:
            container = container.append(df, ignore_index = True)
            container.to_pickle(os.path.dirname(os.path.abspath(__file__)).replace("\\", '/')+'/Data Container.pickle')
        
    except:
        df['Created Date'] = dt(dt.today().year, dt.today().month, dt.today().day)
        df.to_pickle(os.path.dirname(os.path.abspath(__file__)).replace("\\", '/')+'/Data Container.pickle')    