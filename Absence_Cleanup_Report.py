import pandas as pd
from datetime import datetime as dt
from datetime import timedelta
import salesforce_bulk as sf
from time import sleep
import os
import win32com.client as win32
from copy import deepcopy as dc
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
    else:
        return ''

def Clean_Time(x):
    t = x.split('T')[1].split('.')[0].split(':')
    t = [int(y) for y in t]
    
    y = x.split('T')[0].split('-')
    y = [int(m) for m in y]
    time = dt(y[0], y[1], y[2], t[0], t[1], t[2]) - timedelta(hours = 4)
    return time
    
def Clean_Date(x):
    y = x.split('T')[0].split('-')
    y = [int(m) for m in y]
    return dt(y[0], y[1], y[2])
    

def Plus_365(x):
    new = x + timedelta(days = 365)
    return new


def Check_Dates(df):
    if df['Recurrence Start'] > df['Recurrence End']:
        return False
    else:
        return True
    

def Purge_Paradoxes(df):
    if df['Start Time'] >= df['Finish Time']:
        return 1
    else:
        return 0


def Adjust_Dates(date, end_or_start, greater_than = False):
    if greater_than:
        if date > end_or_start:
            return end_or_start
        else:
            return date
        
    else:
        if date < end_or_start:
            return end_or_start
        else:
            return date
        
def Calculate_Duration(df, column1 = 'Recurrence Start', column2 = 'Recurrence End'):
    start = df[column1]
    end = df[column2]
    duration = end - start
    duration = duration.days
    return duration
    


def Report():
    fiscal = pd.read_pickle('C:/Users/pmc1104/OneDrive - The Home Depot/Documents/Projects/Data Storage/Fiscal Info.pickle')
    fiscal = fiscal[['Date','Fiscal Week','Fiscal Period','Fiscal Year']]
    
    
    query_list = ["""
    select CKSW_BASE__Resource__r.Record_Type_Name__c, CKSW_BASE__Start__c, CKSW_BASE__Finish__c, CKSW_BASE__Gantt_Label__c, CKSW_BASE__Geolocation__Latitude__s, CKSW_BASE__Geolocation__Longitude__s, CKSW_BASE__Type__c, CKSW_BASE__Resource__r.Name, CKSW_BASE__Resource__r.Base_Store__r.Store_Code__c, CKSW_BASE__RecurrenceKey__c, CreatedBy.Name, CKSW_BASE__Resource__r.CKSW_BASE__Manager__c, Name
    from CKSW_BASE__Employee_Absence__c
    where CKSW_BASE__Resource__r.CKSW_BASE__Active__c = true and CKSW_BASE__Start__c >= TODAY and (CKSW_BASE__Resource__r.Record_Type_Name__c like '%HDE%' or CKSW_BASE__Resource__r.Record_Type_Name__c like '%HDI%')
    """,
    """
    select Store_Code__c, Sales_Manager__r.LOB__c, Sales_Manager__r.Name, Sales_Manager__r.Branch__r.Name
    from Store_Manager__c
    where Sales_Manager__r.LOB__c like '%HDE%' or Sales_Manager__r.LOB__c like '%HDI%'
    """]
    
    
    # =============================================================================
    # ########################################################
    # query_list[0] += ' limit 10000'###########################
    # ########################################################
    # =============================================================================
    
    
    object_list = ['CKSW_BASE__Employee_Absence__c','Store_Manager__c']
    
    
    
    dfs = SF_Dataframe(query_list, object_list)
    
    df = dfs[0]
    temp = dfs[1]
    
    
    
    df.columns = ['LOB','SC','Store','Manager','Start Data','Finish Data','Gantt','Lat','Long','Type','Recurrence','Created By', 'Absence ID']
    temp.columns = ['Store','LOB','Location','Branch']
    
    df['Start'] = df['Start Data'].apply(Clean_Date)
    df['Finish'] = df['Finish Data'].apply(Clean_Date)
    df['Start Time'] = df['Start Data'].apply(Clean_Time)
    df['Finish Time'] = df['Finish Data'].apply(Clean_Time)
    df['Recurrence'].fillna(-99999, inplace = True)
    
    df['LOB'] = df['LOB'].apply(Clean_LOB)
    temp['LOB'] = temp['LOB'].apply(Clean_LOB)
    
    df.drop(['Start Data','Finish Data'], axis = 1, inplace = True)
    
    df = df.merge(temp, on = ['Store','LOB'], how = 'left')
    del temp, dfs
    
    
    #### IDENTIFY RECURRENCES THAT OCCUR FOR MORE THAN 365 DAYS
    temp = df[df['Recurrence'] != -99999]
    temp = temp[['SC','Location','Branch','Start','LOB','Recurrence']].groupby(['LOB','SC','Location','Branch','Recurrence']).agg({'Start':['min','max']}).reset_index()
    temp.columns = ['LOB','SC','Location','Branch','Recurrence','Recurrence Start','Recurrence End']
    temp['Recurrence Start'] = temp['Recurrence Start'].apply(Plus_365)
    temp['Clean Recurrence'] = temp.apply(Check_Dates, axis = 1)
    temp = temp[temp['Clean Recurrence'] == True]
    temp['Days'] = temp.apply(Calculate_Duration, axis = 1)
    temp = temp[temp['Days'] > 365]
    temp1 = df.drop_duplicates(subset = 'Recurrence')[['Absence ID','Recurrence']]
    temp = temp.merge(temp1, on = 'Recurrence', how = 'left')
    ##########################################
    ###   IDENTIFIED RECURRENCE DATAFRAME  ###
    recurrence = temp.drop(['Clean Recurrence','Days'], axis = 1)
    recurrence = recurrence.merge(fiscal, left_on = 'Recurrence Start', right_on = 'Date', how = 'left')
    ##########################################
    print('Identified Recurrence Issues')
    
    
    temp = temp[['LOB','Branch','Location','Days','Recurrence']].groupby(['LOB','Branch','Location']).agg({'Days':'sum', 'Recurrence':'count'}).reset_index()
    x = temp[['LOB','Branch','Days']].groupby(['LOB','Branch']).sum().reset_index()
    x = x.sort_values('Days', ascending = False).reset_index(drop = True)
    x.reset_index(inplace = True)
    x.drop('Days', inplace = True, axis = 1)
    reccurence_analysis = temp.merge(x, on = ['LOB','Branch'], how = 'left').sort_values('index', ascending = False)
    del x
    ##########################################
    ###  IDENTIFIED WORST LOCATION/BRANCH  ###
    reccurence_analysis = reccurence_analysis.drop('index', axis = 1)
    ##########################################
    print('Completed Analysis of Recurrence Issues')
    
    
    
    
    
    ### IDENTIFY OVERLAP AMONG ABSENCES
    temp = dc(df[['LOB','SC','Start Time','Finish Time','Start','Location','Branch','Absence ID']])
    temp['Drop'] = temp.apply(Purge_Paradoxes, axis = 1)
    temp = temp[temp['Drop'] != 1]
    temp.drop('Drop', inplace = True, axis = 1)
    
    
    temp['Overlap'] = 0
    final = [1]
    count = 1
    for location in temp['Location'].unique():
        temp1 = temp[temp['Location'] == location]
        
        for sc in temp1['SC'].unique():
            count += 1
            temp2 = temp1[temp1['SC'] == sc].sort_values('Start Time', ascending = True)
            
            end = dt(1990,1,1)
            for time in temp2['Start Time'].unique():
                try:
                    time = pd.Timestamp(time).to_pydatetime()
                except:
                    print(location, sc, time)
                if end <= time:
                    temp3 = temp2[temp2['Start Time'] >= time]
                        
                    i = temp3.index.tolist()[0]
                    
                    end = temp3.loc[i, 'Finish Time']
                    
                    final = temp3[temp3['Finish Time']<=end]
                    
                    if len(final) > 1:
                            temp.loc[final.index.tolist(), 'Overlap'] = 1
    
    temp1 = dc(temp[['LOB','SC','Location','Branch','Overlap','Start Time','Finish Time']])
    ##########################################
    ###    IDENTIFIED OVERLAP DATAFRAME    ###
    temp.sort_values(['LOB','Branch','Location','SC','Start'],ascending = True, inplace = True)
    overlap = temp[temp['Overlap']>=1].drop('Overlap', axis = 1)
    overlap = overlap.merge(fiscal, left_on = 'Start', right_on = 'Date', how = 'left')
    ##########################################
    print('Identified overlap issues')
    
    
    temp1['Overlap'] = 1
    temp['Days'] = temp.apply(Calculate_Duration, axis = 1, args = ('Start Time','Finish Time',))
    temp = temp[['LOB','Location','Branch','Overlap','Days']].groupby(['LOB','Location','Branch']).sum().reset_index()
    temp.columns = ['LOB','Location','Branch','Overlap','Days']
    x = temp[['LOB','Branch','Days']].groupby(['LOB','Branch']).sum().reset_index()
    x = x.sort_values('Days', ascending = False).reset_index(drop = True)
    x.reset_index(inplace = True)
    x.drop('Days', inplace = True, axis = 1)
    temp = temp.merge(x, on = ['LOB','Branch'], how = 'left')
    del x
    ##########################################
    ###    OVERLAP ANALYTICS DATAFRAME     ###
    overlap_analysis = temp.sort_values('index', ascending = False)
    overlap_analysis.drop('index', inplace = True, axis = 1)
    ##########################################
    print('Performed analysis of overlap issues')
    
    
    writer = pd.ExcelWriter('C:/Users/pmc1104/OneDrive - The Home Depot/Documents/Projects/Workforce Automation/Calendar Reporting.xlsx')
    reccurence_analysis.to_excel(writer, 'Recurrence Analysis', index = False, columns = ['LOB','Branch','Location','Days','Recurrence'])
    recurrence.to_excel(writer, 'Recurrence Hit-list', index = False, columns = ['LOB','Branch','Location','SC','Recurrence','Absence ID','Recurrence Start','Recurrence End','Fiscal Year','Fiscal Period','Fiscal Week'])
    overlap_analysis.to_excel(writer, 'Overlap Analysis', index = False, columns = ['LOB','Branch','Location','Overlap','Days'])
    overlap.to_excel(writer, 'Overlap Hit-list', index = False, columns = ['LOB','Branch','Location','SC','Absence ID','Start','Start Time','Finish Time','Fiscal Year','Fiscal Period','Fiscal Week'])
    writer.save()
    
    
    outlook = win32.Dispatch('outlook.application')
    mail = outlook.CreateItem(0)
    mail.SentOnBehalfOfName = 'HS_WFM_FieldSupport@homedepot.com'
    mail.To = 'James_C_Barger@homedepot.com'
    mail.Subject = 'SC Homebase Lat/Long Report'
    mail.Body = "See attached for this week's Homebase Lat/Long report"
    mail.Attachments.Add(f'C:/Users/pmc1104/OneDrive - The Home Depot/Documents/Projects/Workforce Automation/Calendar Reporting.xlsx')
    mail.Send()
    
    os.remove(f'C:/Users/pmc1104/OneDrive - The Home Depot/Documents/Projects/Workforce Automation/Calendar Reporting.xlsx')