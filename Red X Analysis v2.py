import pandas as pd
import os
from time import sleep
import salesforce_bulk as sf
from geopy.distance import vincenty
from copy import deepcopy as dc
import tkinter as tk
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

def Find(name, path):
    for root, dirs, files in os.walk(path):
        if name in files:
            return os.path.join(root, name)

def SF_Dataframe(query_list, object_list):
    bulk = sf.SalesforceBulk(username = username, password = password,
                             security_token = security_token)
    
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


def Clean_Max_Travel(x):
    x = x.split(' - ')
    if len(x) == 2:
        x = x[1]
    else:
        x = x[0]
    return int(x)

def Clean_Distance(x):
    return round(x, 2)


def Clean_Product(x):
    try:
        x = x.split('Sell ')[1]
    except IndexError:
        x = 'Product Not Identified'
    return x

def Lat(x):
    return x.latitude

def Long(x):
    return x.longitude


def Distance_To_Lead(df, lead_lat, lead_long):
    lat = df['Home Lat']
    long = df['Home Long']
    
    distance = vincenty((lead_lat, lead_long), (lat, long)).miles
    return distance


def Distance_Acceptable(x, parameter):
    if x <= parameter:
        return True
    else:
        return False
    
class Input_Widget:
    def __init__(self):
        self.main = tk.Tk()
        self.main.geometry("500x200")
        self.main.title('Appointment ID Intake')
        
        var = tk.StringVar()
        self.label = tk.Label(self.main, textvariable = var)
        var.set('Appointment ID:')
        self.label.pack()
        
        self.entry = tk.Entry(self.main)
        self.entry.pack()
        
        run_button = tk.Button(self.main, text = 'Run Checks', command = self.Run)
        run_button.pack()
        
        self.appt_id = ''
        
    def Run(self):
        self.appt_id = self.entry.get()
        try:
            if self.appt_id[0] == 's':
                self.appt_id = 'S' + self.appt_id[1:]
            test = self.appt_id.split('S-')[1]
            try:
                int(test)
                self.main.destroy()
            except ValueError:
                self.entry.delete(0,'end')
        except IndexError:
            self.entry.delete(0,'end')
            
        
    def mainloop(self):
        self.main.mainloop()
        return self.appt_id
    
    
class Complete_Widget:
    def __init__(self):
        self.main = tk.Tk()
        self.main.geometry("750x250")
        self.main.title('Check Complete')
        
        var1 = tk.StringVar()
        self.label = tk.Label(self.main, textvariable = var1)
        var1.set('The check has completed.')
        self.label.pack()
        
        var2 = tk.StringVar()
        self.label = tk.Label(self.main, textvariable = var2)
        var2.set('Please open the output file on your desktop to confirm the results.')
        self.label.pack()
        
        var3 = tk.StringVar()
        self.label = tk.Label(self.main, textvariable = var3)
        self.label.pack()
        
        var4 = tk.StringVar()
        self.label = tk.Label(self.main, textvariable = var4)
        var4.set('If no errors found, check the Gantt for the cause.')
        self.label.pack()
        
        run_button = tk.Button(self.main, text = 'Ok', command = self.Run)
        run_button.pack()
        self.main.mainloop()
        
    def Run(self):
        self.main.destroy()
        
        
class Error_Widget():
    def Error_Message(self, error_message):
        self.main = tk.Tk()
        self.main.geometry("500x250")
        self.main.title('Check Complete')
        
        var1 = tk.StringVar()
        self.label1 = tk.Label(self.main, textvariable = var1)
        var1.set(error_message)
        self.label1.pack()
        
        self.label2 = tk.Label(self.main)
        self.label2.pack()
        
        var2 = tk.StringVar()
        self.label3 = tk.Label(self.main, textvariable = var2)
        var2.set('If this continues to occur, contact Mason Callahan.')
        self.label3.pack()
        
        var3 = tk.StringVar()
        self.label4 = tk.Label(self.main, textvariable = var3)
        var3.set('Patrick_M_Callahan@homedepot.com')
        self.label4.pack()
        
        run_button = tk.Button(self.main, text = 'Ok', command = self.Button)
        run_button.pack()
        
        self.main.mainloop()
        
    def Button(self):
        self.main.destroy()
        

class Reason_Widget():
    def Reason_Message(self, reason_message):
        self.main = tk.Tk()
        self.main.title('Check Complete')
        
        var1 = tk.StringVar()
        self.label1 = tk.Label(self.main, textvariable = var1)
        var1.set(reason_message)
        self.label1.pack()
        
        run_button = tk.Button(self.main, text = 'Ok', command = self.Button)
        run_button.pack()
        
        self.main.mainloop()
        
    def Button(self):
        self.main.destroy()


pathing = Find('RootCA.pem', os.path.abspath(''))
if pathing == None:
    print('RootCA not found locally')
    print('Attempting to find Root CA somewhere in C:\\Users')
    pathing = Find('RootCA.pem', 'C:\\Users')


if not os.path.isfile(pathing):
    Error_Widget().Error_Message('Error: Certificate Pathing')


os.environ['REQUESTS_CA_BUNDLE'] = pathing


widget = Input_Widget()
appt_id = widget.mainloop()


query_list = [
### Grabs all the relevant information from the appointment
f"""
select CKSW_BASE__Service_Type__r.name, Name, CKSW_BASE__Geolocation__Latitude__s, CKSW_BASE__Geolocation__Longitude__s, CKSW_BASE__Zip__c, CKSW_BASE__Location__r.name, RecordType.Name
from CKSW_BASE__Service__c
where name like '%{appt_id}%' and (RecordType.Name like '%HDI%' or RecordType.Name like '%HDE%')
""",

### Can roll the appointment up to a store with this query
"""
select name, Store_Code__c
from CKSW_BASE__Zip_Code__c
where Store_Code__c != null
""",

### Can roll the appointment up to the location level and get max travel from this query
"""
select Store_Code__c, Sales_Manager__r.Name, Sales_Manager__r.Maximum_Travel_Limit__c, Sales_Manager__r.LOB__c
from Store_Manager__c
where (Sales_Manager__r.LOB__c like '%HDI%' or Sales_Manager__r.LOB__c like '%HDE%')
""",

############################ SAVE ALL OF THE FOLLOWING QUERIES FOR AFTER THE INITIAL 3? USE THEM TO FILTER THESE DOWN?
### Get the skills of all of the resources
"""
select CKSW_BASE__Resource__r.User_LDAP__c, Skill_Name__c
from CKSW_BASE__Resource_Skill__c
where CKSW_BASE__Resource__r.CKSW_BASE__Active__c = true and (CKSW_BASE__Resource__r.Record_Type_Name__c like '%HDI%' or CKSW_BASE__Resource__r.Record_Type_Name__c like '%HDE%')
""",

### Determine the working locations of all the SCs
"""
select CKSW_BASE__Resource__r.User_LDAP__c, CKSW_BASE__Location__r.Name
from CKSW_BASE__Working_Location__c
where CKSW_BASE__Resource__r.CKSW_BASE__Active__c = true and (CKSW_BASE__Resource__r.Record_Type_Name__c like '%HDE%' or CKSW_BASE__Resource__r.Record_Type_Name__c like '%HDI%')
""",

### Get the lat/long of the SCs
"""
select User_LDAP__c, Name, CKSW_BASE__Homebase__Latitude__s, CKSW_BASE__Homebase__Longitude__s, CKSW_BASE__User__r.User_Role_Name__c
from CKSW_BASE__Resource__c
where CKSW_BASE__Active__c = true and (Record_Type_Name__c like '%HDE%' or Record_Type_Name__c like '%HDI%') and CKSW_BASE__User__r.User_Role_Name__c like '%Sales Consultant%'
"""]

object_list = ['CKSW_BASE__Service__c','CKSW_BASE__Zip_Code__c','Store_Manager__c','CKSW_BASE__Resource_Skill__c','CKSW_BASE__Working_Location__c','CKSW_BASE__Resource__c']
dfs = SF_Dataframe(query_list, object_list)


########################################################################################
## Extracting DFs
appts = dfs[0]

if len(appts.index.tolist()) < 1:
    Error_Widget().Error_Message('Error: Appointment Not Found')

zips = dfs[1]
locations = dfs[2]

skills = dfs[3]
working = dfs[4]
homes = dfs[5]


## Column Naming
appts.columns = ['Product','ID','Lat','Long','Zip','Location','LOB']
zips.columns = ['Zip','Store']
locations.columns = ['Store','Location','Max Travel','LOB']

skills.columns = ['LDAP','Skill']
working.columns = ['LDAP','Location']
homes.columns = ['LDAP','Name','Home Lat','Home Long','Role']
scs = dc(homes[['LDAP','Name']])
scs.columns = ['LDAP','SC']
########################################################################################


########################################################################################
## Data Management
appts['LOB'] = appts['LOB'].apply(Clean_LOB)
appts['Product'] = appts['Product'].apply(Clean_Product)

locations['Max Travel'].fillna('50', inplace = True)
locations['Max Travel'] = locations['Max Travel'].apply(Clean_Max_Travel)

locations = locations.merge(zips, on = 'Store', how = 'inner')
del zips


## Checks to make sure the single Lead has been identified
if len(appts) != 1:
    Error_Widget().Error_Message('Please check that there is only 1 appointment with that appointment ID.')
########################################################################################



########################################################################################
## Gather information relevant to appt checks
appt_lat = appts.loc[appts.index.tolist()[0], 'Lat']
appt_long = appts.loc[appts.index.tolist()[0], 'Long']
appt_location = appts.loc[appts.index.tolist()[0], 'Location']

zipcode = appts.loc[appts.index.tolist()[0], 'Zip']
lob = appts.loc[appts.index.tolist()[0], 'LOB']
product = appts.loc[appts.index.tolist()[0], 'Product']

locations = locations[(locations['LOB'] == lob) & (locations['Zip'] == zipcode)]
location = locations.loc[locations.index.tolist()[0], 'Location']
max_travel = locations.loc[locations.index.tolist()[0], 'Max Travel']
########################################################################################



########################################################################################
checks = {'Step':[],
          'Check':[],
          'Result':[],
          'Details':[]}

## Checks that Appt Location IS working location
check = 'Ok'
if location != appt_location:
    print('The locations do not match')
    check = 'Failed'
    Reason_Widget().Reason_Message('Zipcode Location does not matchappointment location')

checks['Check'].append('Location')
checks['Result'].append(check)
checks['Step'].append(len(checks['Result']))
checks['Details'].append('-')





## Check SCs assigned to working location - Based on Appointment Zip
working = working[working['Location'] == location]
ldaps = working['LDAP'].unique().tolist()

check = 'Ok'
if not ldaps:
    print('There are no SCs assigned to the working location')
    check = 'Failed'
    Reason_Widget().Reason_Message('There are no SCs assigned to the working location')

checks['Check'].append('Working Location')
checks['Result'].append(check)
checks['Step'].append(len(checks['Result']))
checks['Details'].append(location)




## Check skilling within working location
skills = skills[(skills['LDAP'].isin(ldaps)) & (skills['Skill'] == product)]
ldaps = skills['LDAP'].unique().tolist()

check = 'Ok'
if not ldaps:
    print(f'There are no SCs skilled for {product} in the working location')
    check = 'Failed'
    Reason_Widget().Reason_Message(f'There are not SCs in the working location with {product} skilling')

checks['Check'].append('Skilling')
checks['Result'].append(check)
checks['Step'].append(len(checks['Result']))
checks['Details'].append(product)
    



## Determine range from each capable SC
homes = dc(homes[homes['LDAP'].isin(ldaps)])
if len(homes) >= 1:
    homes['Distance'] = homes.apply(Distance_To_Lead, args = (appt_lat, appt_long,), axis = 1)
    homes['Acceptable Distance'] = homes['Distance'].apply(Distance_Acceptable, args = (max_travel, ))
    homes['Distance'] = homes['Distance'].apply(Clean_Distance)
    homes.sort_values('Distance', ascending = True, inplace = True)
    homes['Travel Parameter'] = max_travel
    homes.drop('Role', axis = 1, inplace = True)

    ldaps = homes[homes['Acceptable Distance'] == True]['LDAP'].unique().tolist()

check = 'Ok'
if not ldaps:
    print('There are no SCs within distance of this appointment "As the crow flies"')
    check = 'Failed'
    Reason_Widget().Reason_Message(f'There are no {product} skilled SCs within {max_travel} miles of the appointment')





checks['Check'].append('Travel Parameter')
checks['Result'].append(check)
checks['Step'].append(len(checks['Result']))
checks['Details'].append(max_travel)
    

working = working.merge(scs, on = 'LDAP', how = 'left')
skills = skills.merge(scs, on = 'LDAP', how = 'left')

desktop = os.path.join(os.path.join(os.path.join(os.environ['USERPROFILE']), 'OneDrive - The Home Depot'), 'Desktop')
filepath = os.path.join(desktop, f'Red X Analysis ({appt_id}).xlsx')
writer = pd.ExcelWriter(filepath)

overview = pd.DataFrame(checks)
overview.to_excel(writer, 'Overview', index = False)
working.to_excel(writer, '1. Working Location Check', index = False)
skills.to_excel(writer, '2. Skilling Check', index = False)
homes.to_excel(writer, '3. Travel Parameter Check', index = False)

writer.save()


Complete_Widget()