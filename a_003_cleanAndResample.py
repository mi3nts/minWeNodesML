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

resampleTime = mintsDefinitions['resampleTime']
# startDate = mintsDefinitions['startDate']
# endDate   = mintsDefinitions['endDate']
startTime = mintsDefinitions['startTime']
endTime    = mintsDefinitions['endTime']


start_time_df = datetime.strptime(mintsDefinitions['startTime'],'%Y_%m_%d_%H_%M_%S')
end_time_df   = datetime.strptime(mintsDefinitions['endTime']  ,'%Y_%m_%d_%H_%M_%S')


# Create an empty DataFrame to store the merged data
merged_df = pd.DataFrame()


columns_to_keep =[  'pc0_1_IPS7100', 'pc0_3_IPS7100', 'pc0_5_IPS7100',
                    'pc1_0_IPS7100', 'pc2_5_IPS7100', 'pc5_0_IPS7100', 'pc10_0_IPS7100',
                    'pm0_1_IPS7100', 'pm0_3_IPS7100', 'pm0_5_IPS7100', 'pm1_0_IPS7100',
                    'pm2_5_IPS7100', 'pm5_0_IPS7100', 'pm10_0_IPS7100',
                        'latitudeCoordinate_GPSGPGGA2', 'longitudeCoordinate_GPSGPGGA2',
                         'speedOverGround_GPSGPRMC2']


# columns_to_keep =['temperature_BME280', 'pressure_BME280', 'humidity_BME280','altitude_BME280',
#                    'pc0_1_IPS7100', 'pc0_3_IPS7100', 'pc0_5_IPS7100',
#                     'pc1_0_IPS7100', 'pc2_5_IPS7100', 'pc5_0_IPS7100', 'pc10_0_IPS7100',
#                     'pm0_1_IPS7100', 'pm0_3_IPS7100', 'pm0_5_IPS7100', 'pm1_0_IPS7100',
#                     'pm2_5_IPS7100', 'pm5_0_IPS7100', 'pm10_0_IPS7100',
#                         'co2_SCD30V2', 'temperature_SCD30V2','humidity_SCD30V2',
#                         'latitudeCoordinate_GPSGPGGA2', 'longitudeCoordinate_GPSGPGGA2',
#                          'speedOverGround_GPSGPRMC2']




for nodeID in nodeIDs:
    print("========================NODES========================")
    print("Reading node data for node "+ nodeID)
    file_path      = dataFolderParent + "/mergedPickles/" + nodeID +  "/" +nodeID + "_" +startTime +"-" +endTime + '.pkl'
    directory_path = os.path.dirname(file_path)

    if os.path.exists(file_path):
        print(f"The file '{file_path}' exists.")
        with open(file_path, 'rb') as file:
            df_mints = pickle.load(file)
            print(df_mints)
            # display(df.to_string())
            # with pd.option_context('display.max_rows', None,
            #            'display.max_columns', None,
            #            'display.precision', 3,
            #            ):
            #     print(df_mints)
            columns_exist = all(column in df_mints.columns for column in columns_to_keep)

            # Print the result
            if columns_exist:
                print("All listed columns exist in the DataFrame.")
                df_mints  = df_mints [columns_to_keep]
                df_mints = df_mints.apply(pd.to_numeric, errors='coerce')
                df_mints[columns_to_keep] = df_mints[columns_to_keep].apply(pd.to_numeric)
                df_mints = df_mints.resample(resampleTime).mean()
                df_mints  =  df_mints.dropna()
                if (df_mints.shape[0] >0):
                    file_path      = dataFolderParent + "/mergedPickles/" + nodeID +  "/" +nodeID + "_" +startTime +"-" +endTime + '_resampled_'+resampleTime+'.pkl'
                    directory_path = os.path.dirname(file_path)

                    # Create the directory if it doesn't exist
                    if not os.path.exists(directory_path):
                        os.makedirs(directory_path)
                        
                    df_mints.to_pickle(file_path)

                else:
                    print("No data left")

               
            else:
                print("Not all listed columns exist in the DataFrame.")

    else:
        print(f"The file '{file_path}' does not exist.")
        continue;
