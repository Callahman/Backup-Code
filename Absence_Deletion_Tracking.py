import pandas as pd
from datetime import datetime as dt
import os
import win32com.client as win32
from time import sleep
import salesforce_bulk as sf
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
    x = [int(y) for y in x.split('T')[0].split('-')]
    date = dt(x[0], x[1], x[2])
    return date

def Clean_LOB(x):
    if 'HDI' in x:
        return 'HDI'
    elif 'HDE' in x:
        return 'HDE'
    else:
        return ''
    

def Report():
    query_list = ["""
    select CKSW_BASE__Start__c, Name, CKSW_BASE__Location__c, CKSW_BASE__Resource__r.RecordType.Name
    from CKSW_BASE__Employee_Absence__c
    where CKSW_BASE__Resource__r.CKSW_BASE__Active__c = true and (CKSW_BASE__Resource__r.RecordType.Name like '%HDE%' or CKSW_BASE__Resource__r.RecordType.Name like '%HDI%') and CKSW_BASE__Start__c >= TODAY and CKSW_BASE__Start__c <= NEXT_N_DAYS:30
    """,
    """
    select CKSW_BASE__Start__c, Name, CKSW_BASE__Location__c, CKSW_BASE__Resource__r.RecordType.Name
    from CKSW_BASE__Employee_Absence__c
    where CKSW_BASE__Resource__r.CKSW_BASE__Active__c = true and (CKSW_BASE__Resource__r.RecordType.Name like '%HDE%' or CKSW_BASE__Resource__r.RecordType.Name like '%HDI%') and CKSW_BASE__Start__c >= YESTERDAY and CKSW_BASE__Start__c <= NEXT_N_DAYS:29
    """]
    object_list = ['CKSW_BASE__Employee_Absence__c', 'CKSW_BASE__Employee_Absence__c']
    
    
    dfs = SF_Dataframe(query_list, object_list)
    
    new = dfs[0]
    new.columns = ['Date','ID','Location','LOB']
    new['Date'] = new['Date'].apply(Clean_Date)
    new['LOB'] = new['LOB'].apply(Clean_LOB)
    
    old_check = dfs[1]
    old_check.columns = ['Date','ID','Location','LOB']
    old_check['Date'] = old_check['Date'].apply(Clean_Date)
    old_check['LOB'] = old_check['LOB'].apply(Clean_LOB)
    
    old = pd.read_pickle('C:/Users/pmc1104/OneDrive - The Home Depot/Documents/Projects/Workforce Automation/Production/Yesterday Snapshot.pickle')
    
    
    old_check['Still Exists'] = 1
    old_check = old_check[['ID','Still Exists']]
    df = old.merge(old_check, on = 'ID', how = 'left')
    df = df[df['Still Exists'] != 1]
    
    if len(df)>0:
        df.to_excel('C:/Users/pmc1104/OneDrive - The Home Depot/Documents/Projects/Workforce Automation/Production/Absences Deleted.xlsx', index = False)
        
        outlook = win32.Dispatch('outlook.application')
        mail = outlook.CreateItem(0)
        mail.SentOnBehalfOfName = 'HS_WFM_FieldSupport@homedepot.com'
#        mail.To = 'James_C_Barger@homedepot.com'
        mail.To = 'Patrick_M_Callahan@homedepot.com'
        mail.Subject = 'Absences Deleted'
        mail.Body = "See attached for a list of absences that were deleted between today and yesterday"
        mail.Attachments.Add('C:/Users/pmc1104/OneDrive - The Home Depot/Documents/Projects/Workforce Automation/Production/Absences Deleted.xlsx')
        mail.Send()
        
        os.remove('C:/Users/pmc1104/OneDrive - The Home Depot/Documents/Projects/Workforce Automation/Production/Absences Deleted.xlsx')
    
    new.to_pickle('C:/Users/pmc1104/OneDrive - The Home Depot/Documents/Projects/Workforce Automation/Production/Yesterday Snapshot.pickle')