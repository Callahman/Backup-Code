import pandas as pd
import salesforce_bulk as sf
from time import sleep
import os
from geopy.distance import vincenty
from copy import deepcopy as dc
from datetime import datetime as dt
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


def Distance(df):
    lat1 = df['Base Store Lat']
    long1 = df['Base Store Long']
    
    lat2 = df['SC Lat']
    long2 = df['SC Long']
    
    miles = vincenty((lat1, long1), (lat2, long2)).miles
    miles = round(miles, 2)
    return miles

def Clean_LOB(x):
    if 'HDE' in x:
        return 'HDE'
    elif 'HDI' in x:
        return 'HDI'
    elif 'HDMS' in x:
        return 'HDMS'
    else:
        return ''
    

def Prep_Store_Numbers(x):
    x = str(x)
    x = x.split('.')[0]
    if len(x) < 4:
        x = '0' + x
    
    return x


def Gantt_Label_Check(df):
    label = df['Gantt Label']
    store = df['Store']
    if str(store) != str(label):
        return label
    else:
        return ''
    
    

def Report():
    query_list = ["""
    select CKSW_BASE__Gantt_Label__c, CKSW_BASE__Homebase__Latitude__s, CKSW_BASE__Homebase__Longitude__s, Base_Store__r.Geolocation__Latitude__s, Base_Store__r.Geolocation__Longitude__s, Name, CKSW_BASE__Location__r.Name, CKSW_BASE__Location__r.Branch__r.Name, RecordType.Name, Base_Store__r.Store_Code__c
    from CKSW_BASE__Resource__c
    where CKSW_BASE__Active__c = true and (RecordType.Name like '%HDE%' or RecordType.Name like '%HDI%') and CKSW_BASE__User__r.is_SCN__c = false and (not CKSW_BASE__Location__r.Name like '%Hawaii%') and (not Name like '%Test%')
    """,
    """
    select Sales_Manager__r.Name, Sales_Manager__r.Owner.Name, Sales_Manager__r.Owner.Email, Sales_Manager__r.LOB__c
    from Store_Manager__c
    """]
    
    object_list = ['CKSW_BASE__Resource__c','Store_Manager__c']
    
    dfs = SF_Dataframe(query_list, object_list)
    
    df = dc(dfs[0])
    df.columns = ['Gantt Label','SC Lat','SC Long','Base Store Lat','Base Store Long','Store', 'SC','Location','Branch','LOB']
    df.dropna(inplace = True)
    df['LOB'] = df['LOB'].apply(Clean_LOB)
    
    temp = dc(dfs[1])
    temp.columns = ['Location','SM Name','SM Email','LOB']
    temp['Drop'] = 1
    temp = temp.groupby(['Location','SM Name','SM Email','LOB']).sum().reset_index()
    temp.drop('Drop', axis = 1, inplace = True)
    temp['LOB'] = temp['LOB'].apply(Clean_LOB)
    
    sms = temp['SM Name'].unique().tolist()
    
    df = df.merge(temp, on = ['Location','LOB'], how = 'left')
    df = df[~df['SC'].isin(sms)]
    
    
    df['Distance'] = df.apply(Distance, axis = 1)
    df.sort_values('Distance', ascending = False, inplace = True)
    
    df = df[df['Distance'] > .05]
    
    df['Store'].astype(str)
    df['Store'] = df['Store'].apply(Prep_Store_Numbers)
    df['Gantt Label'] = df.apply(Gantt_Label_Check, axis = 1)
    
    
    year = dt.today().year
    month = dt.today().month
    day = dt.today().day
    
    writer = pd.ExcelWriter(f'C:/Users/pmc1104/OneDrive - The Home Depot/Documents/Projects/Workforce Automation/SC Lat-Long Check ({month}-{day}-{year}).xlsx')
    df.to_excel(writer, 'Homebase vs Base Store', index = False, columns = ['LOB', 'Branch', 'Location', 'SC', 'Store', 'Gantt Label', 'SM Name', 'SM Email', 'SC Lat', 'SC Long', 'Base Store Lat', 'Base Store Long', 'Distance'])
    
    
    
    
    
    df = dc(dfs[0])
    df.columns = ['Gantt Label','SC Lat','SC Long','Base Store Lat','Base Store Long','Store', 'SC','Location','Branch','LOB']
    
    df['LOB'] = df['LOB'].apply(Clean_LOB)
    df = df[~df['SC'].isin(sms)]
    
    df.fillna('Null', inplace = True)
    
    df = df[(df['SC Lat'] == 'Null') | (df['SC Long'] == 'Null') | (df['Base Store Lat'] == 'Null') | (df['Base Store Long'] == 'Null')]
    df.to_excel(writer, 'Geocode Null', index = False, columns = ['LOB','Branch','Location','SC','Store','Gantt Label','Base Store Lat','Base Store Long','SC Lat','SC Long'])
    
    writer.save()
    
    
    outlook = win32.Dispatch('outlook.application')
    mail = outlook.CreateItem(0)
    mail.SentOnBehalfOfName = 'HS_WFM_FieldSupport@homedepot.com'
    mail.To = 'James_C_Barger@homedepot.com'
    mail.Subject = 'SC Homebase Lat/Long Report'
    mail.Body = "See attached for this week's Homebase Lat/Long report"
    mail.Attachments.Add(f'C:/Users/pmc1104/OneDrive - The Home Depot/Documents/Projects/Workforce Automation/SC Lat-Long Check ({month}-{day}-{year}).xlsx')
    mail.Send()
    
    os.remove(f'C:/Users/pmc1104/OneDrive - The Home Depot/Documents/Projects/Workforce Automation/SC Lat-Long Check ({month}-{day}-{year}).xlsx')