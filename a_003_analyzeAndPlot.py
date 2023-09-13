import pandas as pd
import glob

import sys
import yaml
import os
import time
import glob
import pickle


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


startDate = mintsDefinitions['startDate']
endDate   = mintsDefinitions['endDate']

start_time_df = datetime.strptime(mintsDefinitions['startDate'], '%Y_%m_%d')
end_time_df   = datetime.strptime(mintsDefinitions['endDate'], '%Y_%m_%d')+ timedelta(days=1)


# Create an empty DataFrame to store the merged data
merged_df = pd.DataFrame()


columns_to_keep =['temperature_BME280', 'pressure_BME280', 'humidity_BME280','altitude_BME280',
                   'pc0_1_IPS7100', 'pc0_3_IPS7100', 'pc0_5_IPS7100',
                    'pc1_0_IPS7100', 'pc2_5_IPS7100', 'pc5_0_IPS7100', 'pc10_0_IPS7100',
                    'pm0_1_IPS7100', 'pm0_3_IPS7100', 'pm0_5_IPS7100', 'pm1_0_IPS7100',
                    'pm2_5_IPS7100', 'pm5_0_IPS7100', 'pm10_0_IPS7100',
                        'co2_SCD30V2', 'temperature_SCD30V2','humidity_SCD30V2',
                        'latitudeCoordinate_GPSGPGGA2', 'longitudeCoordinate_GPSGPGGA2',
                         'speedOverGround_GPSGPRMC2']




for nodeID in nodeIDs:
    print("========================NODES========================")
    print("Reading node data for node "+ nodeID)
    file_path      = dataFolderParent + "/mergedPickles/" + nodeID +  "/" +nodeID + "_" +startDate +"-" +endDate + '.pkl'
    directory_path = os.path.dirname(file_path)

    if os.path.exists(file_path):
        print(f"The file '{file_path}' exists.")
        with open(file_path, 'rb') as file:
            df_mints = pickle.load(file)

         
            columns_exist = all(column in df_mints.columns for column in columns_to_keep)

            # Print the result
            if columns_exist:
                print("All listed columns exist in the DataFrame.")
                df_mints  = df_mints [columns_to_keep]



                # Print the resampled DataFrame
                contains_string = df_mints.applymap(lambda x: isinstance(x, str))
                df_mints = df_mints[~contains_string.any(axis=1)]


                df_mints = df_mints.resample('5S').mean()
                df_mints  =  df_mints.dropna(how='any')

                # Filter out rows where any column contains a string value

                print(df_mints)

               
            else:
                print("Not all listed columns exist in the DataFrame.")

    else:
        print(f"The file '{file_path}' does not exist.")
        continue;




   

    # else:
    #     print("No data found for node "+ nodeID)