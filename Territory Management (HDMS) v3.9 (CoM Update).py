import numpy as np
import pandas as pd
from geopy.distance import vincenty
from copy import deepcopy as dc
from datetime import datetime as dt
import os
from time import sleep
import salesforce_bulk as sf
import pickle
import configparser

naming_convention = ' Center of Mass Test'
current_tech_headcount = 32
target_productivity = 7
min_value = 1
max_value = 10
options = []

if not options:
    options = [x for x in range(min_value, max_value + 1)]
    

alphabet = ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z','AA','AB','AC','AD','AE','AF','AG','AH','AI','AJ','AK','AL','AM','AN','AO','AP','AQ','AR','AS','AT','AU','AV','AW','AX','AY','AZ']

# =============================================================================
# parent_location = '%Cleveland%'
# market_location_name = 'Cleveland'
# =============================================================================

parent_location = '%TX%'
market_location_name = 'Dallas'

### Future test markets: Minneapolis, New York, LA, Denver, Boston?


if target_productivity != None:
    market_location_name = market_location_name + f' prod-{target_productivity}'


dataset_location = "C:/Users/pmc1104/OneDrive - The Home Depot/Documents/Projects/HDMS/Redistricting/Pickled Classifiers/"
classifier_location = "C:/Users/pmc1104/OneDrive - The Home Depot/Documents/Projects/HDMS/Redistricting/Pickled Datasets/"
output_location = "C:/Users/pmc1104/OneDrive - The Home Depot/Documents/Projects/HDMS/Redistricting/Pickled Outputs/"
parameter_location = "C:/Users/pmc1104/OneDrive - The Home Depot/Documents/Projects/HDMS/Redistricting/Pickled Parameters/"

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
    return dt(d[0], d[1], d[2], t[0], t[1], t[2])


def Week(x):
    return x.isocalendar()[1]


def Calculate_Inclusion(df, homes, radius):
    lat = df['Lat']
    long = df['Long']
    for home in homes:
        base = homes[home]
        distance = vincenty((base[0], base[1]), (lat, long)).miles
        if distance <= radius:
            return 1
    
    return 0


def Distance(df, lat1, long1, lat2, long2):
    lat1 = df[lat1]
    lat2 = df[lat2]
    long1 = df[long1]
    long2 = df[long2]
    
    distance = vincenty((lat1, long1), (lat2, long2)).miles
    return distance


def Centroid_Distance(df, lat, long, starting_point):
    lat = df[lat]
    long = df[long]
    
    distance = vincenty(starting_point, (lat, long)).miles
    return distance


def Travel_Exceeded(df):
    radius = df['Optimal Radius']
    distance = df['Distance']
    
    if distance > radius:
        return 1
    else:
        return 0



class K_Means:
    def __init__(self, k=3, tol=0.001, max_iter=300):
        self.k = k
        self.tol = tol
        self.max_iter = max_iter
        self.optimized = False
        if self.k == 1:
            self.optimized = True
            self.centroids = {}
        

    def fit(self, data):
        if self.optimized:
            print('Starting Centroid 1 of 1')
            self.centroids[0] = np.mean(data, axis = 0)
            return None

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

            self.optimized = True
            
            count = 0
            for c in self.centroids:
                original_centroid = prev_centroids[c]
                current_centroid = self.centroids[c]
                
                tolerance_check = np.sum((current_centroid-original_centroid)/original_centroid*100.0)
                if tolerance_check > self.tol:
                    count += 1
                    if count == 1 or count % 10 == 0:
                        print(tolerance_check)
                    
                    self.optimized = False

            if self.optimized:
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
    
    
def Durations(df):
    start = df['Actual Start']
    end = df['Actual Finish']
    time = end - start
    time = time.seconds
    return time/60
            
            
            

class Center_of_Mass:
    def __init__(self, x=3):
        self.x = x
        self.assignments = {}
        self.optimized = False
        if self.x == 1:
            self.centroids = {}
            self.optimized = True

    def fit(self, data):
        if self.optimized:
            print('Starting Centroid 1 of 1')
            self.centroids[0] = np.mean(data, axis = 0)
            return None
        
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
                centroid = np.mean(included_points, axis = 0)
                
                data = np.delete(data, np.argwhere(data==best_location)[0][0], 0)
                
            print()
            self.centroids[i] = centroid
        return None
    
    

class Center_of_Mass_Algorithm:
    def __init__(self, x=3):
        self.x = x
        self.assignments = {}
        self.optimized = False
        if self.x == 1:
            self.centroids = {}
            self.optimized = True

    def fit(self, data, max_iter = 10, acceptable_shift = 2.5):
        
        if self.optimized:
            print('Starting Centroid 1 of 1')
            self.centroids[0] = np.mean(data, axis = 0)
            self.centroid_iterations = {1:self.centroids}
            return None
        
        np.random.shuffle(data)
        self.centroids = {}
        self.centroid_iterations = {}   ### Not necessary, but will let me output what the centroids looked like as they moved through the data
        self.appts_per_sc = len(data) // self.x
        included_points = {}
        viable_centroids = []
        
        for centroid in range(self.x):
            starting_point = data[np.random.randint(len(data), size = 1)[0]]
            
            viable_centroids.append(centroid)
            self.centroids[centroid] = starting_point
            included_points[centroid] = [starting_point]
        
        
        
        iteration = 0
        while iteration < max_iter:
            iteration += 1
            print(f'Iteration {iteration}')
            
            old_centroids = dc(self.centroids)
            self.centroid_iterations[iteration] = old_centroids
            
            for point in data:
                distances = []
                for centroid in viable_centroids:
                    if np.array_equal(self.centroids, point):
                        distances.append(0)
                        
                    else:
                        lat1 = self.centroids[centroid][0]
                        long1 = self.centroids[centroid][1]
                        lat2 = point[0]
                        long2 = point[1]
                        
                        distance = vincenty((lat1, long1), (lat2, long2)).miles
                        distances.append(distance)
                
                closest_centroid = viable_centroids[distances.index(min(distances))]
                included_points[closest_centroid].append(point)
                
                self.centroids[closest_centroid] = np.mean(np.array(included_points[closest_centroid]), axis = 0)
                
                if len(included_points[closest_centroid]) >= self.appts_per_sc:
                    viable_centroids.remove(closest_centroid)
                    if not viable_centroids:
                        break
            
            
            self.optimized = True
            for centroid in self.centroids:
                old_centroid = old_centroids[centroid]
                new_centroid = self.centroids[centroid]
                
                if not np.array_equal(old_centroid, new_centroid):
                    shift = vincenty(old_centroid, new_centroid).miles
                    
                    if shift > acceptable_shift:
                        self.optimized = False
                        break
            
            if self.optimized:
                return None
            
            viable_centroids = []
            for centroid in self.centroids:
                included_points[centroid] = []
                viable_centroids.append(centroid)
            
        return None
    
    
###############################################################################
# =============================================================================
# ##################   Data Extraction + Cleaning/Prep   ########################
# if naming_convention == None:
#     naming_convention = ''
#     
#     
# queries = [f"""
# select Name, RecordType.Name, CKSW_BASE__Geolocation__Latitude__s, CKSW_BASE__Geolocation__Longitude__s, CKSW_BASE__Service_Type__r.Name, CKSW_BASE__Resource__r.Name, CKSW_BASE__Resource__r.CKSW_BASE__User__r.User_Role_Name__c, CKSW_BASE__Start__c, CKSW_BASE__Appointment_Start__c, CKSW_BASE__Finish__c, CKSW_BASE__Appointment_Finish__c, CKSW_BASE__Resource__r.CKSW_BASE__Homebase__Latitude__s, CKSW_BASE__Resource__r.CKSW_BASE__Homebase__Longitude__s, CKSW_BASE__Zip__c, CKSW_BASE__Location__r.Name
# from CKSW_BASE__Service__c
# where RecordType.Name like '%HDMS%' and ((not CKSW_BASE__Resource__r.name like '%test%') or (not CKSW_BASE__Resource__r.name like '%homer%')) and CKSW_BASE__Appointment_Start__c <=today and CKSW_BASE__Appointment_Start__c = THIS_FISCAL_YEAR and
# CKSW_BASE__Location__r.CKSW_BASE__Parent_Location__r.Name like '{parent_location}'
# """,
# """
# select name, Hdms_Market_Id__c, CKSW_BASE__Parent_Location__r.Name
# from CKSW_BASE__Location__c
# where name like '%HDMS%'
# """]
# 
# 
# objects = ['CKSW_BASE__Service__c','CKSW_BASE__Location__c']
# 
# 
# fiscal = pd.read_pickle('C:/Users/pmc1104/OneDrive - The Home Depot/Documents/Projects/Data Storage/Fiscal Info.pickle')
# fiscal = fiscal[['Date','Fiscal Week','Fiscal Period','Fiscal Quarter','Fiscal Half','Fiscal Year']]
# 
# 
# dfs = SF_Dataframe(queries, objects)
# df = dfs[0]
# df.columns = ['ID','LOB','Lat','Long','Product','Tech','Role','Base Lat','Base Long','Actual Start','Start','Actual Finish','Finish','Zip','Location']
# 
# df.dropna(inplace = True)
# 
# df['Date'] = df['Start'].apply(Clean_Date)
# df['Start'] = df['Start'].apply(Clean_Date)
# df['Finish'] = df['Finish'].apply(Clean_Date)
# df['Actual Start'] = df['Actual Start'].apply(Clean_Time)
# df['Actual Finish'] = df['Actual Finish'].apply(Clean_Time)
# df['Duration'] = df.apply(Durations, axis = 1)
# 
# 
# df = df.merge(fiscal, on = 'Date', how = 'left')
# del fiscal
# 
# temp = dfs[1]
# temp.columns = ['Location','Market ID','Market Name']
# 
# df = df.merge(temp, on = 'Location', how = 'left')
# del temp
# 
# df.dropna(subset = ['Base Lat','Base Long','Lat','Long'], inplace = True)
# 
# df.to_pickle(f'Queried Data {market_location_name}{naming_convention}.pickle')
# ##############################   Prep complete   ##############################
# =============================================================================




# =============================================================================
# ############################   K-Means Training   #############################
# if naming_convention == None:
#     naming_convention = ''
# 
# df = pd.read_pickle(f'Queried Data {market_location_name}{naming_convention}.pickle')
# 
# 
# X = df[['Lat','Long']].drop_duplicates()
# X = np.array(X).reshape(len(X), 2)
# 
# 
# print()
# print('Running Algorithms')
# 
# for option in options:
#     print()
#     print(f'Working on K Value of {option}')
#     clf = K_Means(k = option)
#     clf.fit(X)
#     
#     pickle.dump(clf, open(f'{classifier_location}Classifier - {option}{market_location_name}{naming_convention}.pickle', 'wb'))
# 
# ############################   K-Means Trained   ##############################
# =============================================================================



# =============================================================================
# #################   K-Means Fitment & Zip Code Assignemnt   ###################
# if naming_convention == None:
#     naming_convention = ''
#     
# 
# df = pd.read_pickle(f'Queried Data {market_location_name}{naming_convention}.pickle')
# geo = pd.read_pickle('C:/Users/pmc1104/OneDrive - The Home Depot/Documents/Projects/HDMS/Zip Code Geolocations.pickle')[['Zip','Geo Zone','Lat','Long']]
# geo.columns = ['Zip','Location','Zip Lat','Zip Long']
# 
# print()
# print('Defining Zipcode Boundaries')
# for option in options:
#     print(f'Defining Zipcodes for {option}')
#     print()
#     clf = pickle.load(open(f'{classifier_location}Classifier - {option}{market_location_name}{naming_convention}.pickle', 'rb'))
#     df['Label'] = df.apply(Prediction, args = (clf,'Lat','Long', ), axis = 1)
#     temp = df[['Label','Zip','LOB']].groupby(['Label','Zip']).count().reset_index()
#     temp.sort_values('LOB', ascending = False, inplace = True)
#     temp.drop_duplicates(subset = ['Zip'], inplace = True, keep = 'first')
#     
#     temp = temp[['Label','Zip']]
#     temp.columns = ['Geozone','Zip']
#     
#     temp = df.merge(temp, on = 'Zip', how = 'left')
#     
#     temp_geo = dc(geo[(geo['Location'].isin(temp['Location'].unique().tolist())) & (~geo['Zip'].isin(temp['Zip'].unique().tolist()))])
#     temp_geo.columns = ['Zip','Location','Lat','Long']
#     
#     temp_geo['Geozone'] = temp_geo.apply(Prediction, args = (clf,'Lat','Long', ), axis = 1)
#     temp_geo['Label'] = dc(temp_geo['Geozone'])
#     temp_geo['LOB'] = 'HDMS'
#     temp_geo['Product'] = 'Fake Appointment'
#     temp_geo['ID'] = 'Fake Appointment'
#     temp_geo['Tech'] = 'Fake Tech'
#     temp_geo['Role'] = 'HDMS Measure Technician'
#     temp_geo['Base Lat'] = dc(temp_geo['Lat'])
#     temp_geo['Base Long'] = dc(temp_geo['Long'])
#     temp_geo['Actual Start'] = temp['Actual Start'].max()
#     temp_geo['Actual Finish'] = temp['Actual Finish'].max()
#     temp_geo['Start'] = temp['Start'].max()
#     temp_geo['Finish'] = temp['Finish'].max()
#     temp_geo['Date'] = temp['Date'].max()
#     temp_geo['Fiscal Week'] = temp['Fiscal Week'].max()
#     temp_geo['Fiscal Period'] = temp['Fiscal Period'].max()
#     temp_geo['Fiscal Quarter'] = temp['Fiscal Quarter'].max()
#     temp_geo['Fiscal Half'] = temp['Fiscal Half'].max()
#     temp_geo['Fiscal Year'] = temp['Fiscal Year'].max()
#     temp_geo['Market ID'] = temp.loc[temp.index.tolist()[0], 'Market ID']
#     temp_geo['Market Name'] = temp.loc[temp.index.tolist()[0], 'Market Name']
#     temp_geo['Duration'] = temp['Duration'].mean()
#     
#     temp = temp.append(temp_geo, ignore_index = True)
#     
#     temp.to_pickle(f'{dataset_location}Data - {option}{market_location_name}{naming_convention}.pickle')
# ###################   K-Means Fitted & Zip Codes Assigned   ###################
# =============================================================================




##############   Forecasting Headcount & Determining Assignment   #############
if naming_convention == None:
    naming_convention = ''
    
print()
print('Starting Headcount Clustering')
for option in options:
    print()
    print(f"Beginning K Value of {option}")
    df = pd.read_pickle(f'{dataset_location}Data - {option}{market_location_name}{naming_convention}.pickle')
    sc_locations = {}
    heads = ''
    df['Measures'] = 1
    
    
    df['Week'] = df['Date'].apply(Week)
    temp = df[['Measures','Week','Tech']].groupby(['Week','Tech']).sum().reset_index()
    temp['Measures'] = temp['Measures'] / 5
    tech_productivity = temp['Measures'].mean()
    if target_productivity != None:
        tech_productivity = target_productivity
        
    daily_measures = df[['Measures','Week']].groupby(['Week']).sum().reset_index()
    daily_measures['Measures'] = daily_measures['Measures'] / 5
    option_daily_measures = daily_measures['Measures'].mean()
    
    
    for label in df['Geozone'].unique():
        # Calculating headcount for each geozone
        print('%s of %s' % (df['Geozone'].unique().tolist().index(label) + 1, len(df['Geozone'].unique())))
        temp = dc(df[df['Geozone']==label])
        
        daily_measures = temp[['Measures','Week']].groupby(['Week']).sum().reset_index()
        daily_measures['Measures'] = daily_measures['Measures'] / 5
        daily_measures = daily_measures['Measures'].mean()
        
        if current_tech_headcount != None:
            percentage_value = daily_measures / option_daily_measures
            techs = int(round(percentage_value * current_tech_headcount))
        
        else:
            techs = int(round(daily_measures / tech_productivity))
            
        temp['Headcount'] = techs
        
        
        # Assigning Headcount
        if techs < 1:
            techs = 1
            
        X = temp[['Lat','Long']].drop_duplicates()
        X = np.array(X).reshape(len(X), 2)
        clf = Center_of_Mass_Algorithm(x = techs)
        print(f'Fitting Center of Mass Algorithm for {techs} techs')
        clf.fit(X, max_iter = 50, acceptable_shift = 2.5)
        
        sc_locations[label] = clf.centroids
        
        
        if len(heads) == 0:
            heads = dc(temp)
        else:
            heads = heads.append(temp, ignore_index = True)
        print()
    
    heads['Option'] = option
    heads['Prod Target'] = tech_productivity
        
    heads.to_pickle(f"{output_location}Output - {option}{market_location_name}{naming_convention}.pickle")
    pickle.dump(sc_locations, open(f'{output_location}Centroids - {option}{market_location_name}{naming_convention}.pickle', 'wb'))
##############   Headcount Forecasted & Assignments Determined   ##############
    
    
    
    
# =============================================================================
# #################   Determining Optimal Travel Parameters   ###################
# if naming_convention == None:
#     naming_convention = ''
#     
# print()
# print('Determining optimal travel parameters')
# radii = [10, 20, 25, 30, 50]
# 
# 
# for option in options:
#     results = []
#     print()
#     print(f'Beginning Optimal Travel Parameter Calculation for Option {option} {market_location_name}')
#     
#     base_df = pd.read_pickle(f"{output_location}Output - {option}{market_location_name}{naming_convention}.pickle")
#     
#     sc_locations = pickle.load(open(f'{output_location}Centroids - {option}{market_location_name}{naming_convention}.pickle', 'rb'))
#     for label in sc_locations:
#         homes = sc_locations[label]
#         df = base_df[base_df['Geozone'] == label].sort_values('Lat')
#             
#             
#         for radius in sorted(radii):
#             df['Included'] = df.apply(Calculate_Inclusion, axis = 1, args = (homes, radius, ))
#             
#             total = len(df)
#             included = df['Included'].sum()
#             if included/total >= .95:
#                 break
#             
#         df['Optimal Radius'] = radius
#         results.append(df)
#         
#     pd.concat(results, ignore_index = True).to_pickle(f"{parameter_location}Parameter {option}{market_location_name}{naming_convention}.pickle")
# #################   Optimal Travel Parameters Determined   ####################
# =============================================================================




# =============================================================================
# ######################   Generating Tableau Outputs   #########################
# if naming_convention == None:
#     naming_convention = ''
# 
# print()
# print('Compiling data for tableau outputs')
# df = pd.concat([pd.read_pickle(f"{parameter_location}Parameter {option}{market_location_name}{naming_convention}.pickle") for option in options], ignore_index = True)
# df['Geozone'] = df['Geozone'] + 1
# df['Distance'] = df.apply(Distance, axis = 1, args = ('Base Lat','Base Long','Lat','Long', ))
# df['Travel Exceeded'] = df.apply(Travel_Exceeded, axis = 1)
# 
# print('Prepping summary data')
# temp = df[['Option','Geozone','Travel Exceeded']].groupby(['Option','Geozone']).agg({'Travel Exceeded':['sum','count']}).reset_index()
# temp.columns = ['Option','Geozone','Exceeded','Total Measures']
# 
# summary = df[['Base Lat','Base Long','Lat','Long','Date','Option','Optimal Radius','Headcount','Geozone','Measures','Included','Prod Target']]
# summary = summary[['Date','Option','Optimal Radius','Headcount','Geozone','Measures','Included','Prod Target']].groupby(['Date','Option','Optimal Radius','Headcount','Geozone','Prod Target']).sum().reset_index()
# summary = summary[['Option','Optimal Radius','Headcount','Geozone','Measures','Included','Prod Target']].groupby(['Option','Optimal Radius','Headcount','Geozone','Prod Target']).mean().reset_index()
# summary = summary.merge(temp, on = ['Option','Geozone'], how = 'left')
# del temp
# 
# summary.sort_values(['Option','Geozone'], ascending = True, inplace = True)
# 
# print('Prepping mapping data')
# ### Create data table used to generate tableau maps
# tableau_mapping = dc(df[['LOB','Lat','Long','Product','Tech','Base Lat','Base Long','Start','Finish','Actual Start','Actual Finish','Zip','Location','Date','Market Name','Label','Geozone','Option','Included']])
# 
# 
# ### Generating Original Zipcodes for Tableau Maps
# tableau_zips = pd.read_excel('C:/Users/pmc1104/OneDrive - The Home Depot/Documents/Projects/HDMS/Geozone - Zips (10-5-18).xlsx')
# tableau_zips = tableau_zips[tableau_zips['NAME'].isin(tableau_mapping['Zip'].unique().tolist())]
# 
# print('Writing data to excel')
# writer = pd.ExcelWriter(f'Tableau Inputs - {market_location_name}{naming_convention}.xlsx')
# summary.to_excel(writer, 'Summary Data', index = False)
# tableau_mapping.to_excel(writer, 'Mapping Data', index = False)
# tableau_zips.to_excel(writer, 'Original Zips', index = False)
# writer.save()
# del summary, tableau_mapping, tableau_zips
# print('Tableau outputs have been generated')
# #######################   Tableau Outputs Generated   #########################
# =============================================================================




# =============================================================================
# ######################   Generating Mapline Outputs   #########################
# if naming_convention == None:
#     naming_convention = ''
#     
# df = pd.concat([pd.read_pickle(f"{parameter_location}Parameter {option}{market_location_name}{naming_convention}.pickle") for option in options], ignore_index = True)
# df = df[df['ID'] != 'Fake Appointment']
# df['Geozone'] = df['Geozone'] + 1
# df['Distance'] = df.apply(Distance, axis = 1, args = ('Base Lat','Base Long','Lat','Long', ))
# df['Travel Exceeded'] = df.apply(Travel_Exceeded, axis = 1)
# 
# 
# writer = pd.ExcelWriter(f'Mapline Inputs - {market_location_name}{naming_convention}.xlsx')
# 
# appointments = df[['ID','Lat','Long']].drop_duplicates()
# df = dc(df[['ID','Geozone','Option']])
# 
# for option in df['Option'].unique():
#     temp = dc(df[df['Option'] == option])
#     temp[f'{option} - Geozone'] = temp['Geozone']
#     temp.drop(['Geozone','Option'], inplace = True, axis = 1)
#     appointments = appointments.merge(temp, on = ['ID'], how = 'left')
# 
# 
# appointments.to_excel(writer, 'Appointment Data', index = False)
# del appointments
# print('Appointments')
# 
# 
# ### Create dataset of labeled measure tech locations
# tech_placement = pd.DataFrame(columns = [['Option','Geozone','Tech','Lat','Long']], index = range(10))
# i = 0
# for option in df['Option'].unique():
#     sc_locations = pickle.load(open(f'{output_location}Centroids - {option}{market_location_name}{naming_convention}.pickle', 'rb'))
#     
#     for geozone in sc_locations:
#         for tech in sc_locations[geozone]:
#             tech_placement.loc[i, 'Option'] = option
#             tech_placement.loc[i, 'Geozone'] = geozone + 1
#             tech_placement.loc[i, 'Tech'] = str(geozone + 1) + alphabet[tech]
#             tech_placement.loc[i, 'Lat'] = sc_locations[geozone][tech][0]
#             tech_placement.loc[i, 'Long'] = sc_locations[geozone][tech][1]
#             
#             i += 1
# 
# tech_placement.to_excel(writer, 'Tech Base Data', index = False)
# del tech_placement
# print('Tech Placement')
# writer.save()
# #######################   Mapline Output Generated   ##########################
# =============================================================================
