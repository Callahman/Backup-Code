import pandas as pd
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


def Clean_LOB(x):
    if 'HDE' in x:
        return 'HDE'
    elif 'HDI' in x:
        return 'HDI'


def Report():
    excluded_scs = ['HDE SM Homer','SC Hawaii HDE']
    
    query_list = ["""
    select CKSW_BASE__Location__r.Name, Name, RecordType.Name, CKSW_BASE__Location__r.RecordType.name
    from CKSW_BASE__Resource__c
    where CKSW_BASE__Active__c = true and (RecordType.Name like '%HDI%' or RecordType.Name like '%HDE%')
    """]
    
    object_list = ['CKSW_BASE__Resource__c']
    
    df = SF_Dataframe(query_list, object_list)[0]
    df.columns = ['Location','Location LOB','SC','SC LOB']
    
    df['Location LOB'] = df['Location LOB'].apply(Clean_LOB)
    df['SC LOB'] = df['SC LOB'].apply(Clean_LOB)
    
    df = df[(df['SC LOB'] != df['Location LOB']) & (~df['SC'].isin(excluded_scs))].reset_index(drop = True)
    
    if len(df) > 0:
        df.to_excel('C:/Users/pmc1104/OneDrive - The Home Depot/Documents/Projects/Workforce Automation/SC LOB Errors.xlsx', index = False)
        
        outlook = win32.Dispatch('outlook.application')
        mail = outlook.CreateItem(0)
        mail.SentOnBehalfOfName = 'HS_WFM_FieldSupport@homedepot.com'
        mail.To = 'James_C_Barger@homedepot.com'
        mail.Subject = 'SC LOB Report'
        mail.Body = "See attached for today's LOB error Report. We have identified that some SCs do not have a Line of Business that matches their working location."
        mail.Attachments.Add('C:/Users/pmc1104/OneDrive - The Home Depot/Documents/Projects/Workforce Automation/SC LOB Errors.xlsx')
        mail.Send()
        
        os.remove('C:/Users/pmc1104/OneDrive - The Home Depot/Documents/Projects/Workforce Automation/SC LOB Errors.xlsx')