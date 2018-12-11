import pandas as pd
import os
from time import sleep
import salesforce_bulk as sf
from datetime import datetime as dt
from datetime import timedelta
from copy import deepcopy as dc
import pickle
from geopy.distance import vincenty
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
    
def Clean_Boolean(x):
    if x:
        return 1
    else:
        return 0
    
def Clean_Sales(x):
    if x == 1:
        return 0
    else:
        return x
    
    
def Clean_Date(x):
    x = [int(y) for y in x.split('T')[0].split('-')]
    return dt(x[0], x[1], x[2])


def Clean_Time(x):
    d = [int(y) for y in x.split('T')[0].split('-')]
    t = [int(y) for y in x.split('T')[1].split('.')[0].split(':')]
    return dt(d[0], d[1], d[2], t[0], t[1], t[2]) - timedelta(hours = 4)


def Duration(df):
    start = df['Start']
    end = df['Finish']
    time = end - start
    time = time.seconds
    return time/60




def Geolocate(df, appts):
    store1 = df['Store1']
    store2 = df['Store2']
    store3 = df['Store3']
    
    appts = appts[appts['Store'].isin([store1, store2, store3])]
    lat = appts['Lat'].mean()
    long = appts['Long'].mean()
    return lat, long


def Calculate_Inclusion(df, scs):
    lat = df['Lat']
    long = df['Long']
    branch = df['Branch']
    store = df['Store']
    
    included_10 = 0
    included_20 = 0
    included_25 = 0
    included_30 = 0
    included_50 = 0
    
    green_included_10 = 0
    green_included_20 = 0
    green_included_25 = 0
    green_included_30 = 0
    green_included_50 = 0
    
    green_dot = 0
    
    
    scs = scs[scs['Branch'] == branch]
    for i in scs.index.tolist():
        base_lat = scs.loc[i, 'Lat']
        base_long = scs.loc[i, 'Long']
        
        store1 = scs.loc[i, 'Store1']
        store2 = scs.loc[i, 'Store2']
        store3 = scs.loc[i, 'Store3']
        stores = [store1, store2, store3]
        
        distance = vincenty((base_lat, base_long), (lat, long)).miles
        
        
        if distance <= 10 and store in stores:
            green_included_10 = 1
        elif distance <= 10:
            included_10 = 1
            
        if distance <= 20 and store in stores:
            green_included_20 = 1
        elif distance <= 20:
            included_20 = 1
            
        if distance <= 25 and store in stores:
            green_included_25 = 1
        elif distance <= 25:
            included_25 = 1
            
        if distance <= 30 and store in stores:
            green_included_30 = 1
        elif distance <= 30:
            included_30 = 1
            
        if distance <= 50 and store in stores:
            green_included_50 = 1
        elif distance <= 50:
            included_50 = 1
            
        if store in stores:
            green_dot = 1
    
    return included_10, included_20, included_25, included_30, included_50, green_included_10, green_included_20, green_included_25, green_included_30, green_included_50, green_dot
    

# =============================================================================
# queries = ["""
# select Opportunity__r.Base_Store_Number__c, CKSW_BASE__Geolocation__Latitude__s, CKSW_BASE__Geolocation__Longitude__s, RecordType.name
# from CKSW_BASE__Service__c
# where CKSW_BASE__Appointment_Start__c = LAST_N_MONTHS:12 and CKSW_BASE__Appointment_Start__c != TODAY and Opportunity__r.IsClosed = true and CKSW_BASE__Status__c != 'Canceled' and RecordType.name like '%HDI%'
# """,
# """
# select Store_Code__c, Sales_Manager__r.Branch__r.name, Sales_Manager__r.LOB__c
# from Store_Manager__c
# where (Sales_Manager__r.Branch__r.Name like '%Atlanta%' and Sales_Manager__r.LOB__c like '%HDI%') or (Sales_Manager__r.Branch__r.Name like '%Boston%' and Sales_Manager__r.LOB__c like '%HDI%') or (Sales_Manager__r.Branch__r.Name like '%Jersey%' and Sales_Manager__r.LOB__c like '%HDE%')
# """,
# """
# select User_LDAP__c, CKSW_BASE__Homebase__Latitude__s, CKSW_BASE__Homebase__Longitude__s, CKSW_BASE__User__r.User_Role_Name__c
# from CKSW_BASE__Resource__c
# where CKSW_BASE__Active__c = true and Record_Type_Name__c like '%HDI%' and CKSW_BASE__User__r.User_Role_Name__c like '%Sales Con%'
# """]
# 
# objects = ['CKSW_BASE__Service__c','Store_Manager__c','CKSW_BASE__Resource__c']
# 
# dfs = SF_Dataframe(queries, objects)
# 
# 
# 
# pickle.dump(dfs, open('DELETE ME.pickle','wb'))
# =============================================================================




dfs = pickle.load(open('DELETE ME.pickle','rb'))

df = dfs[0]
df.columns = ['Store','Lat','Long','LOB']
df['LOB'] = df['LOB'].apply(Clean_LOB)

temp = dfs[1]
temp.columns = ['Store','Branch','LOB']
temp['LOB'] = temp['LOB'].apply(Clean_LOB)

sc_sf = dfs[2]
sc_sf.columns = ['LDAP','OG Lat','OG Long','Role']
sc_sf.drop('Role', axis = 1, inplace = True)


df = df.merge(temp, on = ['Store','LOB'], how = 'left')
df.dropna(inplace = True)
del temp


scs = pd.read_excel('Assignment Template.xlsx')

scs['Lat'], scs['Long'] = zip(*scs.apply(Geolocate, axis = 1, args = (df, )))
scs = scs.merge(sc_sf, on = 'LDAP', how = 'left')
del sc_sf

###
scs.dropna(subset = ['OG Lat'], inplace = True)
###

df['Included 10'], df['Included 20'], df['Included 25'], df['Included 30'], df['Included 50'], df['Included 10 - Green Dot'], df['Included 20 - Green Dot'], df['Included 25 - Green Dot'], df['Included 30 - Green Dot'], df['Included 50 - Green Dot'], df['Green Dot'] = zip(*df.apply(Calculate_Inclusion, axis = 1, args = (scs, )))

df = df.drop(['Store','Lat','Long'], axis = 1)
df['Appts'] = 1
df = df.groupby('Branch').sum().reset_index()

for column in df.columns:
    if 'Included' in column and 'Green' in column:
        radius = column.split(' ')[1]
        df[f'% Coverage {radius} - Green Dot'] = df[column] / df['Green Dot']
        
    elif 'Included' in column:
        radius = column.split(' ')[1]
        df[f'% Coverage {radius}'] = df[column] / df['Appts']


writer = pd.ExcelWriter('Multi-Base Output.xlsx')
scs.to_excel(writer, 'SC Bases', index = False)
df.to_excel(writer, 'Preliminary Analytics', index = False)
writer.save()