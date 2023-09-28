import pandas as pd
import glob

import sys
import yaml
import os
import time
import glob

from datetime import date, timedelta, datetime
mintsDefinitions         = yaml.load(open("mintsDefinitions.yaml"))
print(mintsDefinitions)
nodeIDs            = mintsDefinitions['nodeIDs']
dataFolder         = mintsDefinitions['dataFolder']
dataFolderParent   = mintsDefinitions['dataFolderParent']
dataFolderMqtt     = mintsDefinitions['dataFolderMqtt']
sensorIDs          = mintsDefinitions['sensorIDs']

print()
print("MINTS")
print()


resampleTime = mintsDefinitions['resampleTime']

startTime    = mintsDefinitions['startTime']
endTime      = mintsDefinitions['endTime']

start_time_df = datetime.strptime(mintsDefinitions['startTime'], '%Y_%m_%d_%H_%M_%S')
end_time_df   = datetime.strptime(mintsDefinitions['endTime']  , '%Y_%m_%d_%H_%M_%S')


# Create an empty DataFrame to store the merged data
merged_df = pd.DataFrame()

 
for currentNode in nodeIDs:
    
    nodeID      = currentNode['nodeID']
    nodeName    = currentNode['nodeName']
    print("========================NODES========================")
    print("Merging node data for "+ nodeName+ " with node ID " + nodeID)
    dfs = []

   # Use glob to get a list of all CSV files in the directory
    csv_directory = dataFolder +"/" + nodeID
    
    csv_files = glob.glob(f'{csv_directory}/*/*/*/*.csv')

    print(csv_files)
    if len(csv_files)>0:
        # Loop through the CSV files and merge them into the main DataFrame
        for csv_file in csv_files:
            # print(csv_file.split('_')[2])
            sensorID_csv = csv_file.split('_')[2]
            temp_df = pd.read_csv(csv_file)

            string_to_add = "_" + sensorID_csv 

            # Specify the column name to exclude
            exclude_column = 'dateTime'
            # Add the string to column names except for the excluded column
            temp_df.columns = [col + string_to_add if col != exclude_column else col for col in temp_df.columns]
            # print(temp_df)
            # print(temp_df.to_string)
            # temp_df = temp_df.resample('1S').sum()
            dfs.append(temp_df)
            

    # Concatenate all the DataFrames into one
        merged_df = pd.concat(dfs, ignore_index=True)
        merged_df = merged_df.dropna(how='all')
        merged_df = merged_df.dropna(subset=['dateTime'])
        desired_format = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{6}'
        merged_df = merged_df[merged_df['dateTime'].str.contains(desired_format, regex=True)]
        merged_df['dateTime'] = pd.to_datetime(merged_df['dateTime'])
        merged_df = merged_df[(merged_df['dateTime'] >=  start_time_df) & (merged_df['dateTime'] <= end_time_df)]
        merged_df.set_index('dateTime', inplace=True)
        print(merged_df)
        
        filePathePrePkl        =dataFolderParent + "/mergedPickles/" + nodeID +  "/" +nodeID + "_" +nodeName + "_" +startTime +"-" +endTime 
        filePathePreCSV        =dataFolderParent + "/mergedCSVs/" + nodeID +  "/" +nodeID + "_" +nodeName + "_" +startTime +"-" +endTime 


        directoryPathPkl = os.path.dirname( filePathePrePkl + '.pkl')
        if not os.path.exists(directoryPathPkl):
            os.makedirs(directoryPathPkl)



        directoryPathCSV  = os.path.dirname( filePathePreCSV + '.csv')
        if not os.path.exists(directoryPathCSV):
            os.makedirs(directoryPathCSV)

             
        merged_df.to_pickle(filePathePrePkl + '.pkl')
        merged_df.to_csv( filePathePreCSV   + '.csv')
   

    else:
        print("No data found for node "+ nodeID)