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
startDate    = mintsDefinitions['startDate']
endDate      = mintsDefinitions['endDate']

start_time_df = datetime.strptime(mintsDefinitions['startDate'], '%Y_%m_%d')
end_time_df   = datetime.strptime(mintsDefinitions['endDate'], '%Y_%m_%d')+ timedelta(days=1)


# Create an empty DataFrame to store the merged data
merged_df = pd.DataFrame()




for nodeID in nodeIDs:
    print("========================NODES========================")
    print("Merging node data for node "+ nodeID)
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
        # print(merged_df.columns)



        file_path      = dataFolderParent + "/mergedPickles/" + nodeID +  "/" +nodeID + "_" +startDate +"-" +endDate + '.pkl'
        directory_path = os.path.dirname(file_path)

        # Create the directory if it doesn't exist
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
             
        merged_df.to_pickle(file_path)

   

    else:
        print("No data found for node "+ nodeID)