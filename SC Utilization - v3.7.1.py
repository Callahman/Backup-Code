import pandas as pd
import numpy as np
from datetime import datetime as dt
from datetime import timedelta
from datetime import timezone
import dateutil
import os
from time import sleep
import salesforce_bulk as sf
from copy import deepcopy as dc
import pickle
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



start = dt(2018, 11, 7)
end = dt(2018, 11, 12)
LOB = 'HDI'

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
    try:
        d = dateutil.parser.parse(x)
        d = d.replace(tzinfo=timezone.utc).astimezone(tz=None)
        
        return dt(d.year, d.month, d.day, d.hour, d.minute, d.second)
    except TypeError:
        return np.nan

def Time_To_Start(df):
    x = df['Start'] - timedelta(seconds = df['Travel To'])
    return x

def Time_From_End(df):
    x = df['Finish'] + timedelta(seconds = df['Travel From'])
    return x

def Appointment_Finish_Time(df):
    start = df['Start']
    duration = df['Duration']
    
    return start + timedelta(hours = duration)
    

def Lead_Time(df):
    time = df['Start'] - df['Created']
    return (time.days) + (86400 / time.seconds)

    
def Clean_Dict(x, key):
    return x[key]

def Clean_Duration(df):
    duration = df['Duration']
    duration_type = df['Duration Units']
    
    if duration_type == 'Hours':
        return duration
    elif duration_type == 'Minutes':
        return duration / 60
    else:
        return 0
        


### Designed to be used for groupby().apply(Utilization)
def Utilization(df, Actual_Times = False, Absence_Type = False):
    appointment_duration = 2.5
    
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    data = {'Date':[],
            'Total Day':[],
            'Raw Free Time':[],
            'Free Time Slots':[],
            'SD Appt Time':[],
            'ND Appt Time':[],
            'Other Appt Time':[],
            'Absence Time':[]}
    
    
    for date in df['Date'].unique():
        date = pd.Timestamp(date)
        dow = days[date.weekday()]
        
        if dow == 'Saturday' or dow == 'Sunday':
            day_start = dt(date.year, date.month, date.day, 8)
            day_end = dt(date.year, date.month, date.day, 21)
            total_day = 13
            
        else:
            day_start = dt(date.year, date.month, date.day, 8)
            day_end = dt(date.year, date.month, date.day, 22)
            total_day = 14
            
        # Creates dataframe representing all of the appointments in the given day
        day_window = df[(df['Start'] < day_end) & (df['Finish'] > day_start)]
        day_window.loc[day_window[day_window['Start'] < day_start].index.tolist(), 'Start'] = day_start
        day_window.loc[day_window[day_window['Finish'] > day_end].index.tolist(), 'Finish'] = day_end
        day_window.sort_values('Start', inplace = True)
        day_window.reset_index(drop = True, inplace = True)
        
        starts = day_window[['Start','Actual Start','Product','ID','Date','Lead Time','Fiscal Week','Fiscal Year','Type','Reason']]
        finishes = day_window[['Finish','Actual Finish','Product','ID','Date','Lead Time','Fiscal Week','Fiscal Year','Type','Reason']]
        
        
        if Actual_Times:
            starts.columns = ['Scheduled Event','Event','Product','ID','Date','Lead Time','Fiscal Week','Fiscal Year','Type','Reason']
            finishes.columns = ['Scheduled Event','Event','Product','ID','Date','Lead Time','Fiscal Week','Fiscal Year','Type','Reason']
        else:
            starts.columns = ['Event','Actual Event','Product','ID','Date','Lead Time','Fiscal Week','Fiscal Year','Type','Reason']
            finishes.columns = ['Event','Actual Event','Product','ID','Date','Lead Time','Fiscal Week','Fiscal Year','Type','Reason']
        
        
        starts['Event Type'] = 'Start'
        finishes['Event Type'] = 'Finish'
        starts['Ordering'] = 2
        finishes['Ordering'] = 1
        
        day_window = pd.concat([starts, finishes], sort = False, ignore_index = True)
        day_window.sort_values(['Event','Ordering'], ascending = True, inplace = True)
        day_window.reset_index(drop = True, inplace = True)
        del starts, finishes
        
        
        if day_window.index.tolist():
            if day_window.loc[day_window.index.tolist()[-1], 'Event'] != day_end:
                i = day_window.index.tolist()[-1] + 1
                day_window.loc[i, 'Type'] = 'End of Day'
                day_window.loc[i, 'Event Type'] = 'End of Day'
                day_window.loc[i, 'Event'] = day_end
                    
            
            if len(day_window) >= 1:
                last_time = dc(day_start)
                last_type = 'Start of Day'
                last_event_type = 'Start of Day'
                last_lead_time = ''
                
                ########################################################
                ## Buckets for recording durations of different types ##
                free_time = []
                sd_time = []
                nd_time = []
                other_appt_time = []
                absence_time = []
                ########################################################
                
                
                ##############################################################
                ## Buckets for keeping track of what items are still 'open' ##
                open_ids = []               ## Record IDs that are still open
                open_starts = []            ## Start Times that are still open
                open_types = []                     ## Absence vs Appointment
                open_lead_times = []                ## Lead Times
                open_reasons = []                   ## Absence Reasons
                ##############################################################
                
                for i in day_window.index.tolist():
                    record_id = day_window.loc[i, 'ID']
                    record_time = day_window.loc[i, 'Event']
                    record_event_type = day_window.loc[i, 'Event Type']
                    record_type = day_window.loc[i, 'Type']
                    record_lead_time = day_window.loc[i, 'Lead Time']
                    record_reason = day_window.loc[i, 'Reason']
                    
                    duration = record_time - last_time
                    duration = duration.seconds
                    
                    if record_id in ['EA-2169779','S-2851668','S-2948553','S-2853606','S-2853606','S-2778595','EA-2170292']:
                        print(record_id)
                        print(record_time)
                        print(record_event_type)
                        print(record_type)
                        print(record_lead_time)
                        print()
                        print('Free',free_time)
                        print('SD',sd_time)
                        print('ND',nd_time)
                        print('Other',other_appt_time)
                        print('Abs',absence_time)
                        print()
                        print()
                        
                    
                    ########################################
                    ## Deciding how to allocate the duration
                    if last_type == 'Start of Day':
                        if record_event_type == 'Start':
                            free_time.append(duration)
                        
                        elif record_event_type == 'Finish':
                            print(record_id)
                            raise KeyError('This record should not be a "Finish" time')
                            
                        
                    
                    elif record_event_type == 'Start':
                        if last_event_type == 'Start':
                            if last_type == 'Absence':
                                absence_time.append(duration)
                            
                            elif last_type == 'Appointment':
                                if last_lead_time < 1:
                                    sd_time.append(duration)
                                elif last_lead_time < 2:
                                    nd_time.append(duration)
                                elif last_lead_time >= 2:
                                    other_appt_time.append(duration)
                                
                        
                        elif last_event_type == 'Finish':
                            ### Could be refined to help for wierd overlap issues
                            if open_ids:
                                last_open_type = open_types[-1]
                                last_open_lead_time = open_lead_times[-1]
                                
                                if last_open_type == 'Absence':
                                    absence_time.append(duration)
                                
                                elif last_open_type == 'Appointment':
                                    if last_open_lead_time < 1:
                                        sd_time.append(duration)
                                    elif last_open_lead_time < 2:
                                        nd_time.append(duration)
                                    else:
                                        other_appt_time.append(duration)
                                
                            else:
                                free_time.append(duration)
                        
                    
                    
                    elif record_event_type == 'Finish':
                        if last_event_type == 'Start':
                            ranking_list = ['Appointment0','Appointment1','Appointment','Absence']
                        
                            ## Ranking the current record
                            if record_lead_time < 0 or record_lead_time > 1:
                                current_rank = ranking_list.index(record_type)
                            else:
                                current_rank = ranking_list.index(record_type + str(int(record_lead_time)))
                            
                            ## Ranking the previous record
                            if last_lead_time < 0 or last_lead_time > 1:
                                last_rank = ranking_list.index(last_type)
                            else:
                                last_rank = ranking_list.index(last_type + str(int(last_lead_time)))
                                
                            ## Evaluating ranks
                            if current_rank <= last_rank:
                                priority_type = record_type
                                priority_lead_time =record_lead_time
                                
                            else:
                                priority_type = last_type
                                priority_lead_time =last_lead_time
                        
                            
                            if priority_type == 'Absence':
                                absence_time.append(duration)
                                
                            elif priority_type == 'Appointment':
                                if priority_lead_time < 1:
                                    sd_time.append(duration)
                                elif priority_lead_time < 2:
                                    nd_time.append(duration)
                                elif priority_lead_time >= 2:
                                    other_appt_time.append(duration)
                                
                        
                        elif last_event_type == 'Finish':
                            if record_type == 'Absence':
                                absence_time.append(duration)
                            
                            elif record_type == 'Appointment':
                                if record_lead_time < 1:
                                    sd_time.append(duration)
                                elif record_lead_time < 2:
                                    nd_time.append(duration)
                                elif priority_lead_time >= 2:
                                    other_appt_time.append(duration)
                    
                    
                    
                    elif record_event_type == 'End of Day':
                        if last_event_type == 'Start':
                            raise KeyError('The last record should not be a "Start" time')
                        
                        elif last_event_type == 'Finish':
                            free_time.append(duration)
                            
                    else:
                        raise ValueError('Something was unaccounted for...')
                    ########################################
                    
                    
                    
                    
                        
                    #################################################
                    ### Maintain 'open' records
                    if record_event_type == 'Start':
                        open_ids.append(record_id)
                        open_starts.append(record_time)
                        open_types.append(record_type)
                        open_lead_times.append(record_lead_time)
                        open_reasons.append(record_reason)
                        
                    elif record_event_type == 'Finish':
                        i = open_ids.index(record_id)
                        open_ids.remove(open_ids[i])
                        open_starts.remove(open_starts[i])
                        open_types.remove(open_types[i])
                        open_lead_times.remove(open_lead_times[i])
                        open_reasons.remove(open_reasons[i])
                    #################################################
                    
                    
                    
                    #################################################
                    ### Maintain last record ###
                    last_time = dc(record_time)
                    last_type = dc(record_type)
                    last_event_type = dc(record_event_type)
                    last_lead_time = dc(record_lead_time)
                    #################################################
                    
                    
                    
                    
                    
            free_time = [x/(60*60) for x in free_time]
            sd_time = [x/(60*60) for x in sd_time]
            nd_time = [x/(60*60) for x in nd_time]
            other_appt_time = [x/(60*60) for x in other_appt_time]
            absence_time = [x/(60*60) for x in absence_time]
            time_slots = [x//appointment_duration for x in free_time]
            
            
            data['Date'].append(date)
            data['Total Day'].append(total_day)
            data['Raw Free Time'].append(sum(free_time))
            data['Free Time Slots'].append(sum(time_slots))
            data['SD Appt Time'].append(sum(sd_time))
            data['ND Appt Time'].append(sum(nd_time))
            data['Other Appt Time'].append(sum(other_appt_time))
            data['Absence Time'].append(sum(absence_time))
            
    
    if data['Date']:
        return data





###############################################################################
###############################################################################
###############################################################################
end_of_range = str(end.isoformat()).split('.')[0]+'.000-00:00'
start_of_range = str(start.isoformat()).split('.')[0]+'.000-00:00'

query_list = [f"""
SELECT CKSW_BASE__Resource__r.Name, CKSW_BASE__Resource__r.User_LDAP__c, CKSW_BASE__Appointment_Start__c, CreatedDate, CKSW_Actual_Start__c, CKSW_Actual_Finish__c, CKSW_BASE__Travel_Time_To__c, CKSW_BASE__Travel_Time_From__c, CKSW_BASE__Location__c, CKSW_BASE__Service_Type__r.CKSW_BASE__Duration__c, CKSW_BASE__Service_Type__r.CKSW_BASE__Duration_Type__c, CKSW_BASE__Service_Type__r.Name, Name
FROM CKSW_BASE__Service__c
WHERE Opportunity__r.IsClosed = true AND CKSW_BASE__Appointment_Start__c < TODAY AND RecordType.Name like '%{LOB}%' AND CKSW_BASE__Status__c !='Canceled' AND Opportunity__r.is_SCN__c != true
AND (CKSW_BASE__Start__c <= {end_of_range}) AND (CKSW_BASE__Finish__c >= {start_of_range})
""",

f"""
select CKSW_BASE__Resource__r.Name, CKSW_BASE__Resource__r.User_LDAP__c, CKSW_BASE__Start__c, CKSW_BASE__Type__c, CKSW_BASE__Finish__c, CKSW_BASE__Travel_Time_To__c, CKSW_BASE__Travel_Time_From__c, CKSW_BASE__Resource__r.CKSW_BASE__Location__r.id, Name
from CKSW_BASE__Employee_Absence__c
where CKSW_BASE__Resource__r.Record_Type_Name__c like '%{LOB}%'
and (CKSW_BASE__Start__c <= {end_of_range}) AND (CKSW_BASE__Finish__c >= {start_of_range})
""",

f"""
select Sales_Manager__c, Sales_Manager__r.Branch__r.Name, Sales_Manager__r.Owner.name, Store__r.Time_Zone__c
from Store_Manager__c
where Sales_Manager__r.LOB__c like '%{LOB}%'
"""]

object_list = ['CKSW_BASE__Service__c','CKSW_BASE__Employee_Absence__c','Store_Manager__c']



dfs = SF_Dataframe(query_list, object_list)

################
pickle.dump(dfs, open('DELETE THESE DFS.pickle','wb'))
################
################
dfs = pickle.load(open('DELETE THESE DFS.pickle','rb'))
################

appointments = dfs[0]
absences = dfs[1]
sms = dfs[2]

appointments.columns = ['SC','LDAP','Start','Created', 'Actual Start', 'Actual Finish','Travel To','Travel From','Location','Duration','Duration Units','Product','ID']
absences.columns = ['SC','LDAP','Location','Start','Reason','Finish','Travel To','Travel From','ID']
sms.columns = ['Location','Branch','SM','Timezone']


appointments = appointments.merge(sms, on = 'Location', how = 'left')
appointments.drop('Location' ,axis = 1, inplace = True)
absences = absences.merge(sms, on = 'Location', how = 'left')
absences.drop('Location' ,axis = 1, inplace = True)

appointments['Timezone'] = 
appointments['Duration'] = appointments.apply(Clean_Duration, axis = 1)
appointments['Date'] = appointments['Start'].apply(Clean_Date)
appointments['Actual Start'].fillna(appointments['Start'], inplace = True)
appointments['Start'] = appointments['Start'].apply(Clean_Time)
appointments['Finish'] = appointments.apply(Appointment_Finish_Time, axis = 1)
appointments['Created'] = appointments['Created'].apply(Clean_Time)
appointments['Actual Start'] = appointments['Actual Start'].apply(Clean_Time)
appointments['Actual Finish'] = appointments['Actual Finish'].apply(Clean_Time)
appointments['Actual Finish'].fillna(appointments['Finish'], inplace = True)
appointments['Travel To'].fillna(0, inplace = True)
appointments['Travel From'].fillna(0, inplace = True)
appointments['Travel To Start'] = appointments.apply(Time_To_Start, axis = 1)
appointments['Travel From End'] = appointments.apply(Time_From_End, axis = 1)
appointments['Lead Time'] = appointments.apply(Lead_Time, axis = 1)
appointments.drop(['Duration','Created','Duration Units'], axis = 1, inplace = True)


absences['Date'] = absences['Start'].apply(Clean_Date)
absences['Start'] = absences['Start'].apply(Clean_Time)
absences['Finish'] = absences['Finish'].apply(Clean_Time)
absences['Actual Start'] = dc(absences['Start'])
absences['Actual Finish'] = dc(absences['Finish'])
absences['Travel To'].fillna(0, inplace = True)
absences['Travel From'].fillna(0, inplace = True)
absences['Travel To Start'] = absences.apply(Time_To_Start, axis = 1)
absences['Travel From End'] = absences.apply(Time_From_End, axis = 1)
absences['Reason'].fillna('None', inplace = True)



fiscal = pd.read_pickle('C:/Users/pmc1104/OneDrive - The Home Depot/Documents/Projects/Data Storage/Fiscal Info.pickle')
fiscal = fiscal[['Date','Fiscal Week','Fiscal Year']]

appointments = appointments.merge(fiscal,  on = 'Date', how = 'left')
absences = absences.merge(fiscal, on = 'Date', how = 'left')
del fiscal, sms





appointments.to_pickle('Appts.pickle')
absences.to_pickle('Abs.pickle')

# =============================================================================
# appointments = pd.read_pickle('Appts.pickle')
# absences = pd.read_pickle('Abs.pickle')
# 
# 
# 
# 
# 
# appointments['Type'] = 'Appointment'
# absences['Type'] = 'Absence'
# 
# appointments['Reason'] = 'None'
# absences['Lead Time'] = -99999
# absences['Product'] = 'None'
# 
# 
# df = pd.concat([appointments, absences], ignore_index = True, sort = False)
# df.drop(['Travel To','Travel From'], axis = 1, inplace = True)
# 
# 
# df.sort_values(['Branch','SM','SC','Start'], ascending = True).reset_index(inplace = True, drop = True)
# 
# 
# ### DROP NULL VALUES - BETTER TO FIND A WAY TO CLEAN DATA ###
# df.dropna(inplace = True)
# #############################################################
# 
# # =============================================================================
# # df = df[(df['LDAP'] == 'DRB1Q7B')]
# # =============================================================================
# # =============================================================================
# # df = df[(df['LDAP'] == 'MXD0970')]
# # =============================================================================
# 
# df = df.groupby(['Branch','SM','SC','LDAP']).apply(Utilization).reset_index()
# 
# series = []
# keys = ['Date','Total Day','Raw Free Time','Free Time Slots','SD Appt Time','ND Appt Time','Other Appt Time','Absence Time']
# for key in keys:
#     temp = df[0].apply(Clean_Dict, args = (key, )).apply(pd.Series, 1).stack()
#     temp.index = temp.index.droplevel(-1)
#     temp.name = key
#     series.append(temp)
#     
#     
# temp = pd.concat(series, axis = 1)
# df = df.join(temp).drop(0, axis = 1).reset_index(drop = True)
# df['Total Appointment Time'] = df['SD Appt Time'] + df['ND Appt Time'] + df['Other Appt Time']
# 
# 
# 
# #df.to_excel('DELETE ME TOO3.xlsx', index = False)
# =============================================================================



# =============================================================================
# #### Notes: ####
# 1. Some items seem unaccounted for...?
#     1a. Check each error message's cause
# 2. Travel Times need to be included
# 3. Allow for 'Actuals' vs defaults
# 4. Need to clean up data inputs
# =============================================================================
