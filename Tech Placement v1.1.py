import pandas as pd
import os
from time import sleep
import salesforce_bulk as sf
from copy import deepcopy as dc
import pickle
import geopy
from geopy.distance import VincentyDistance as Vincenty
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



def Clean_Max_Travel(x):
    try:
        x = x.split(' - ')[1]
        return int(x)
    except:
        return 50
    
    
def Circle_Geolocations(df):
    lat = df['Lat']
    long = df['Long']
    degree = df['Degree']
    
    center = geopy.Point(lat, long)
    mile_distance = df['Max Travel']
    d = Vincenty(miles = mile_distance)
    destination = d.destination(point=center, bearing=degree)
    return destination.latitude, destination.longitude
    
    
    
    
    

# =============================================================================
# query_list = ["""
# select CKSW_HDMS_Tech_Id__c, Name, CKSW_BASE__Homebase__Latitude__s, CKSW_BASE__Homebase__Longitude__s, CKSW_BASE__Location__r.Maximum_Travel_Limit__c, CKSW_BASE__Location__r.name, CKSW_BASE__Location__r.CKSW_BASE__Parent_Location__r.name
# from CKSW_BASE__Resource__c
# where CKSW_HDMS_Tech_Id__c != null and CKSW_BASE__Active__c = true and (not Name like '%ghost%') and CKSW_BASE__User__r.User_Role_Name__c like '%Tech%' and CKSW_BASE__Homebase__Latitude__s != null
# """,
# """
# select Name, CKSW_BASE__Geolocation__Latitude__s, CKSW_BASE__Geolocation__Longitude__s, Zip_Code__r.Name, CKSW_BASE__Resource__r.Name, CKSW_BASE__Resource__r.CKSW_HDMS_Tech_Id__c
# from CKSW_BASE__Service__c
# where CKSW_BASE__Appointment_Start__c < TODAY and CKSW_BASE__Appointment_Start__c = LAST_N_MONTHS:12 and RecordType.name like '%HDMS%' and CKSW_BASE__Status__c != 'Canceled'
# """,
# """
# select Name, CKSW_HDMS_Market__r.name, CKSW_HDMS_Market__r.CKSW_BASE__Parent_Location__r.Name
# from CKSW_BASE__Zip_Code__c
# where CKSW_HDMS_Market__r.CKSW_BASE__Parent_Location__r.name != null and (not Name like '%a%')
# """]
# 
# object_list = ['CKSW_BASE__Resource__c','CKSW_BASE__Service__c','CKSW_BASE__Zip_Code__c']
# 
# dfs = SF_Dataframe(query_list, object_list)
# 
# pickle.dump(dfs, open('DELETE ME.pickle', 'wb'))
# =============================================================================



dfs = pickle.load(open('DELETE ME.pickle', 'rb'))

dfs[0].columns = ['Tech ID','Tech','Lat','Long','Max Travel','Market','Parent Market']
dfs[1].columns = ['ID','Lat','Long','Zip','Tech','Tech ID']
dfs[2].columns = ['Zip','Market','Parent Market']









###############################################################################
#                   Tech Base/Coverage Calculations
df = dfs[0]

df['Max Travel'] = df['Max Travel'].apply(Clean_Max_Travel)
df['Travel Type'] = 'Default'
df['Degree'] = 0
df['Label'] = 'Circle'

degrees = [15,30,45,60,75,90,105,
120,135,150,165,180,195,210,225,
240,255,270,285,300,315,330,345,360]

output = dc(df)
for degree in degrees:
    temp = dc(df)
    temp['Degree'] = degree
    output = output.append(temp, ignore_index = True, sort = False)



output['Circle Lat'], output['Circle Long'] = zip(*output.apply(Circle_Geolocations, axis = 1))

df['Circle Lat'] = dc(df['Lat'])
df['Circle Long'] = dc(df['Long'])
df['Label'] = 'Homebase'

output = output.append(df, ignore_index = True, sort = False)
###############################################################################



###############################################################################
#                   Appointment Data Formatting
# =============================================================================
# temp1 = dc(dfs[1].dropna(subset = ['Zip']))
# temp2 = dc(dfs[2].dropna(subset = ['Zip']))
# =============================================================================
appts = dfs[1].merge(dfs[2], on = 'Zip', how = 'left')
temp = dc(output[['Circle Lat','Circle Long','Tech ID','Tech','Market','Parent Market','Degree','Label']])
temp['ID'] = 'SC'
temp.columns = ['Lat','Long','Tech ID','Tech','Market','Parent Market','ID','Degree','Label']
appts = dc(appts[['ID','Lat','Long','Tech','Tech ID','Market','Parent Market']])
appts = appts.append(temp, ignore_index = True, sort = False)
###############################################################################




###############################################################################
#                   Appointment Data Formatting
zips = dfs[2]
zips['Join_Key'] = 1
output['Join_Key'] = 1
### Join these on market, parent, and join key
###Fill na values in the circle lat/long with 0
###############################################################################




writer = pd.ExcelWriter('Tableau Input.xlsx')
output.to_excel(writer, 'Tech Bases', index = False)
appts.to_excel(writer, 'Measures', index = False)
zips.to_excel(writer, 'Zipcodes', index = False)
writer.save()
