import numpy as np
import pandas as pd
from geopy.distance import vincenty
from copy import deepcopy as dc
from datetime import datetime as dt
from datetime import timezone
from math import ceil
import dateutil
import os
from time import sleep
import salesforce_bulk as sf
import pickle
import configparser

output_filename = 'HDE New England North Redistrict v1'
LOB = 'HDE'
k_value = 5
branches = ['new england north']
target_productivity = 8

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

def Joint_Potential(df, columns, weights, means, stds):
    potential = 0
    for column, weight, mean, std in zip(columns, weights, means, stds):
        try:
            potential += ((df[column]-mean)/std) * weight
        except ZeroDivisionError:
            potential += 0
        
    return potential

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
    d = dateutil.parser.parse(x)
    d = d.replace(tzinfo=timezone.utc).astimezone(tz=None)
    return dt(d.year, d.month, d.day)

class K_Means:
    def __init__(self, k=3, tol=0.001, max_iter=300):
        self.k = k
        self.tol = tol
        self.max_iter = max_iter

    def fit(self,data):

        self.centroids = {}

        for i in range(self.k):
            self.centroids[i] = data[i]

        for i in range(self.max_iter):
            self.classifications = {}

            for i in range(self.k):
                self.classifications[i] = []

            for geolocation in data:
                lat = geolocation[0]
                long = geolocation[1]
                
                distances = []
                for centroid in self.centroids:
                    centroid_lat = self.centroids[centroid][0]
                    centroid_long = self.centroids[centroid][1]
                    distances.append(vincenty((lat, long), (centroid_lat, centroid_long)).miles)
                    
                classification = distances.index(min(distances))
                self.classifications[classification].append(geolocation)

            prev_centroids = dict(self.centroids)

            for classification in self.classifications:
                self.centroids[classification] = np.average(self.classifications[classification],axis=0)

            optimized = True
            
            count = 0
            for c in self.centroids:
                original_centroid = prev_centroids[c]
                current_centroid = self.centroids[c]
                
                tolerance_check = np.sum((current_centroid-original_centroid)/original_centroid*100.0)
                if tolerance_check > self.tol:
                    count += 1
                    if count == 1 or count % 10 == 0:
                        print(tolerance_check)
                    
                    optimized = False
                    break

            if optimized:
                break

def Prediction(df, clf, lat_column, long_column, cluster = True):
    lat = df[lat_column]
    long = df[long_column]
    centroids = clf.centroids
    
    distances = [vincenty((lat, long),(centroids[centroid][0], centroids[centroid][1])).miles for centroid in centroids]
    classification = (distances.index(min(distances)))
    if cluster:
        return classification
    else:
        return min(distances)
    
    
def Visual(df, clf, lat_column, long_column):
    lat = df[lat_column]
    long = df[long_column]
    
    location = str(lat) + str(long)
    
    try:
        return clf.assignments[location]
    except KeyError:
        return -1
            
            
            

class Center_of_Mass:
    def __init__(self, x=3):
        self.x = x
        self.assignments = {}

    def fit(self, data):

        self.centroids = {}
        self.appts_per_sc = len(data) // self.x
            
        for i in range(self.x):
            print('Starting Centroid %s of %s' % (i+1, self.x))
            centroid_index = np.argmax(data, axis = 0)[0]
            centroid = data[centroid_index]
            included_points = [centroid]
            data = np.delete(data, centroid_index, 0)
            
            for _ in range(self.appts_per_sc-1):
                best_distance = 9999999999
                best_location = ''
                
                for geolocation in data:
                    lat = geolocation[0]
                    long = geolocation[1]
                    
                    centroid_lat = centroid[0]
                    centroid_long = centroid[1]
                    
                    distance = vincenty((lat, long), (centroid_lat, centroid_long)).miles
                    
                    if distance < best_distance:
                        best_distance = distance
                        best_location = geolocation
                
                
                included_points.append(best_location)
                self.assignments[str(best_location[0]) + str(best_location[1])] = i
                
                data = np.delete(data, np.argwhere(data==best_location)[0][0], 0)
                
            print()
            
            self.centroids[i] = centroid
    
    
###############################################################################
###############################################################################
###############################################################################
###############################################################################
# =============================================================================
# branch_insert = ''
# or_included = False
# for branch in branches:
#     x = f"Sales_Manager__r.Branch__r.name like '%{branch}%'"
#     
#     if or_included:
#         x = ' or ' + x
#     else:
#         or_included = True
#         
#     branch_insert += x
# 
# branch_insert = '(' + branch_insert + ')'
# 
# 
# query_list = [
# f"""
# select Opportunity__c, CKSW_BASE__Geolocation__Latitude__s, CKSW_BASE__Geolocation__Longitude__s, Zip_Code__r.Name, Opportunity__r.IsWon, Opportunity__r.Amount, CKSW_BASE__Appointment_Start__c, Opportunity__r.Store__r.Geolocation__Latitude__s, Opportunity__r.Store__r.Geolocation__Longitude__s
# from CKSW_BASE__Service__c
# where CKSW_BASE__Status__c !='Canceled' and Opportunity__r.IsClosed = true and RecordType.name like '%{LOB}%' and CKSW_BASE__Appointment_Start__c < TODAY and Opportunity__r.Historical_Dataload__c != true and CKSW_BASE__Appointment_Start__c = LAST_N_MONTHS:12
# """,
# """
# select Name, Store_Code__c
# from CKSW_BASE__Zip_Code__c
# where Store_Code__c != null
# """,
# f"""
# select Store_Code__c, Sales_Manager__r.Branch__r.Name
# from Store_Manager__c
# where Sales_Manager__r.LOB__c like '%{LOB}%' and {branch_insert}
# """]
# 
# object_list = ['CKSW_BASE__Service__c','CKSW_BASE__Zip_Code__c','Store_Manager__c']
# 
# dfs = SF_Dataframe(query_list, object_list)
# 
# 
# ###########################################
# pickle.dump(dfs, open('DELETE ME.pickle','wb'))
# =============================================================================
###########################################
dfs = pickle.load(open('DELETE ME.pickle','rb'))
###########################################


fiscal = pd.read_pickle('C:/Users/pmc1104/OneDrive - The Home Depot/Documents/Projects/Data Storage/Fiscal Info.pickle')
fiscal = fiscal[['Date','Fiscal Week','Fiscal Period','Fiscal Quarter','Fiscal Half','Fiscal Year']]

appts = dfs[0]
zipcodes = dfs[1]
branches = dfs[2]



appts.columns = ['ID','Lat','Long','Zip','Won','Gross Sales','Store Lat','Store Long','Date']
zipcodes.columns = ['Zip','Store']
branches.columns = ['Store','Branch']

appts['Won'] = appts['Won'].apply(Clean_Boolean)
appts['Gross Sales'] = appts['Gross Sales'].apply(Clean_Sales)
appts['Date'] = appts['Date'].apply(Clean_Date)

df = zipcodes.merge(branches, on = 'Store', how = 'left')
df.dropna(inplace = True)
df = df.merge(appts, on = 'Zip', how = 'left')
df = df.merge(fiscal, on = 'Date', how = 'left')
df['Appts'] = 1
df.dropna(subset = ['Lat','Long'], inplace = True)
###### Need to include net sales still ######

[print(x) for x in df.columns]
# =============================================================================
# copy_df = dc(df)
# 
# X = copy_df[['Lat','Long']].drop_duplicates()
# X = np.array(X).reshape(len(X), 2)
# 
# output_dfs = {'Originals':[], 'Optimized':[], 'Headcount':[]}
# print('Running Algorithms...')
# 
# clf = K_Means(k = k_value)
# 
# clf.fit(X)
# 
# df['Label'] = df.apply(Prediction, args = (clf,'Lat','Long', ), axis = 1)
# df['Centroid Dist'] = df.apply(Prediction, args = (clf, True,'Lat','Long', ), axis = 1)
# 
# df['Store Label'] = df.apply(Prediction, args = (clf,'Store Lat','Store Long', ), axis = 1)
# df['Store Centroid Dist'] = df.apply(Prediction, args = (clf,'Store Lat','Store Long',False, ), axis = 1)
# 
# 
# print('Analyzing initial clusters...')
# temp = df[['Store Label','Zip','Appts']].groupby(['Store Label','Zip']).count()
# temp.reset_index(inplace = True)
# temp = temp[['Store Label','Zip']].groupby(['Store Label']).count()
# temp.reset_index(inplace = True)
# temp.columns = ['Store Label','Zip Count']
# df = df.merge(temp, on = 'Store Label', how = 'left')
# 
# 
# temp = df[['Store Label','Store','Appts']].groupby(['Store Label','Store']).count()
# temp.reset_index(inplace = True)
# temp = temp[['Store Label','Store']].groupby(['Store Label']).count()
# temp.reset_index(inplace = True)
# temp.columns = ['Store Label','Store Count']
# df = df.merge(temp, on = 'Store Label', how = 'left')
# 
# print('Defining totals')
# 
# 
# ####################################### DEFINING POTENTIAL #######################################
# df.dropna(inplace = True)
# potential_columns = ['Gross Sales','Appts']
# weights = [1, 1]
# means = []
# stds = []
# for column in potential_columns:
#     means.append(df[column].mean())
#     stds.append(df[column].std())
# 
# df['Potential'] = df.apply(Joint_Potential, args = (potential_columns, weights, means, stds), axis = 1)
# ##################################################################################################
# 
# totals = df[['Store Label','Appts','Potential']].groupby('Store Label').sum()
# totals.reset_index(inplace = True)
# 
# centroids = clf.centroids
# 
# df['K Value'] = k_value
# output_dfs['Originals'].append(df)
# 
# 
# print('Running optimization')
# max_iter = 300
# count = 0
# while True:
#     starting_variance = totals['Potential'].var()
#     lowest_sales = totals['Potential'].min()
#     
#     low_label = totals.loc[totals['Potential'].tolist().index(lowest_sales), 'Store Label']
#     
#     low = df.loc[df['Store Label']==low_label,:]
#     high = df.loc[df['Store Label']!=low_label,:]
#     
#     stores = high['Store'].unique()
#     
#     min_store = 0
#     min_distance = 9999999999
#     centroid_lat = centroids[low_label][0]
#     centroid_long = centroids[low_label][1] 
#     for store in stores:
#         i = df[df['Store']==store].index.tolist()[0]
#         lat = df.loc[i,'Store Lat']
#         long = df.loc[i,'Store Long']
#         
#         distance = vincenty((lat, long),(centroid_lat, centroid_long)).miles
#         if distance < min_distance:
#             min_store = store
#             min_distance = distance
#             
#     temp = dc(df)
#     temp.loc[temp['Store']==min_store,'Store Label'] = low_label
# 
#     totals = temp[['Store Label','Potential']].groupby('Store Label').sum()
#     totals.reset_index(inplace = True)
#     
#     new_variance = totals['Potential'].var()
#     
#     if starting_variance >= new_variance:
#         df = dc(temp)
#     
#     if starting_variance < new_variance or count >= max_iter:
#         break
#     
#     count += 1
#     
# 
# output_dfs['Optimized'].append(df)
# print()
# print()
# print()
# 
# 
# 
# 
# # =============================================================================
# # print('Starting Headcount Clustering')
# # sc_locations = {}
# # heads = ''
# # for label in df['Store Label'].unique():
# #     print('%s of %s' % (df['Store Label'].unique().tolist().index(label) + 1, len(df['Store Label'].unique())))
# #     temp = dc(df[df['Store Label']==label])
# #     x_SCs = temp[['Fiscal Quarter','Appts']].groupby('Fiscal Quarter').sum().reset_index()
# #     
# #     ###-------------------------------------------------------------------------------###
# #     ### REMOVE THIS LATER (Only here because of missing 2017 data in HDI ATL Salesforce)
# #     ###-------------------------------------------------------------------------------###
# #     x_SCs = x_SCs[x_SCs['Fiscal Quarter']!=1]
# #     ###-------------------------------------------------------------------------------###
# #     
# #     x_SCs['Appts'] = (x_SCs['Appts']/13) / target_productivity
# #     x_SCs = ceil(x_SCs['Appts'].mean())
# #     
# #     X = temp[['Lat','Long']].drop_duplicates()
# #     X = np.array(X).reshape(len(X), 2)
# #     
# #     clf = Center_of_Mass(x = x_SCs)
# #     print('Fitting Data')
# #     clf.fit(X)
# #     
# #     temp['SC Label'] = df.apply(Visual, args = (clf,'Lat','Long', ), axis = 1)
# #     temp['Headcount'] = x_SCs
# #     
# #     sc_locations[label] = clf.centroids
# #     
# #     if len(heads) == 0:
# #         heads = dc(temp)
# #     else:
# #         heads = heads.append(temp, ignore_index = True)
# #     print()
# #         
# # output_dfs['Headcount'].append(heads)
# # =============================================================================
#         
#         
#         
# writer = pd.ExcelWriter(f'{output_filename}.xlsx')
# print('Concatenating...')
# original = pd.concat(output_dfs['Originals'])
# optimized = pd.concat(output_dfs['Optimized'])
# #headcount = pd.concat(output_dfs['Headcount'])
# 
# print('Saving to excel...')
# original.to_excel(writer, sheet_name = 'Original', index = False)
# optimized.to_excel(writer, sheet_name = 'Optimized', index = False)
# #headcount.to_excel(writer, sheet_name = 'Headcount', index = False)
# writer.save()
# 
# 
# # =============================================================================
# # df = headcount[['Lat','Long','Store Label','SC Label']]
# # df['Type'] = 'Appointment'
# # 
# # i = df.index.tolist()[-1] + 1
# # for label in sc_locations:
# #     centroids = sc_locations[label]
# #     
# #     for centroid in centroids:
# #         lat = centroids[centroid][0]
# #         long = centroids[centroid][1]
# #         
# #         df.loc[i, 'Lat'] = lat
# #         df.loc[i, 'Long'] = long
# #         df.loc[i, 'Store Label'] = label
# #         df.loc[i, 'SC Label'] = centroid
# #         df.loc[i, 'Type'] = 'SC Centroid'
# #         
# #         i += 1
# #         
# # df.to_excel('Mapline Input.xlsx', index = False)
# # 
# # print("Job's Done")
# # =============================================================================
# =============================================================================
