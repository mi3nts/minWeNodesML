import pandas as pd
import glob

import sys
import yaml
import os
import time
import glob
import pickle

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import mplleaflet
from matplotlib.colors import Normalize
from matplotlib.cm import ScalarMappable

import folium
import webbrowser

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

resampleTime  = mintsDefinitions['resampleTime']
startTime     = mintsDefinitions['startTime']
endTime       = mintsDefinitions['endTime']

start_time_df = datetime.strptime(mintsDefinitions['startTime'], '%Y_%m_%d_%H_%M_%S')
end_time_df   = datetime.strptime(mintsDefinitions['endTime'], '%Y_%m_%d_%H_%M_%S')

varUsed   = mintsDefinitions['varUsed']


# Create an empty DataFrame to store the merged data
merged_df = pd.DataFrame()


for nodeID in nodeIDs:
    print("========================NODES========================")
    print("Reading node data for node "+ nodeID)
    file_path       = dataFolderParent + "/mergedPickles/" + nodeID +  "/" +nodeID + "_" +startTime +"-" +endTime + '_resampled_'+resampleTime+'.pkl'
    directory_path = os.path.dirname(file_path)

    if os.path.exists(file_path):
        print(f"The file '{file_path}' exists.")
        with open(file_path, 'rb') as file:
            df_mints = pickle.load(file)

        color_column = varUsed
        map_center = [32.967407, -96.703391]
        m = folium.Map(location=map_center, zoom_start=20)

        color_scale = folium.LinearColormap(
                        colors=['yellow', 'red'],
                        vmin=df_mints[color_column].min(),  # Minimum value from the specified column
                        vmax=df_mints[color_column].max()   # Maximum value from the specified column
                        )
        
        # Create a feature group for the markers
        marker_group = folium.FeatureGroup(name= mintsDefinitions['varLabel'])
        
        for index, row in df_mints.iterrows():
            latitude, longitude, varUsedNow = row['latitudeCoordinate_GPSGPGGA2'], row['longitudeCoordinate_GPSGPGGA2'], row[varUsed]
            color = color_scale(varUsedNow)
            
            folium.CircleMarker(
                location=(latitude, longitude),
                radius=5,
                color=color,
                fill=True,
                fill_color=color,
                fill_opacity=0.7,
                popup=f' PM 2.5: {varUsedNow}'
            ).add_to(marker_group)


        marker_group.add_to(m)
        
        # Add a color legend
        color_scale.add_to(m)

        folium.LayerControl().add_to(m)

        file_path = dataFolderParent + "/plots/surveplots/" + nodeID +  "/" +nodeID + "_" + varUsed + "_"+ startTime +"-" +endTime + '_surveyPlot_' +resampleTime+'.html'

        directory_path = os.path.dirname(file_path)

        # Create the directory if it doesn't exist
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)

        # Save the map to an HTML file
        # m.save(dataFolderParent + "/plots/surveplots/" + nodeID +  "/" +nodeID + "_" + varUsed + "_" +startTime +"-" +endTime + '_surveyPlot_'+resampleTime+'.html')
        m.save(file_path)