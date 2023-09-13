

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
 
startDate = datetime.strptime(mintsDefinitions['startDate'], '%Y_%m_%d')
endDate = datetime.strptime(mintsDefinitions['endDate'], '%Y_%m_%d')

delta      = timedelta(days=1)

def delete_empty_folders(root_folder):
    for folder_name, subfolders, filenames in os.walk(root_folder, topdown=False):
        for subfolder in subfolders:
            folder_path = os.path.join(folder_name, subfolder)
            if not os.listdir(folder_path):  # Check if the folder is empty
                try:
                    os.rmdir(folder_path)  # Delete the empty folder
                    print(f"Deleted empty folder: {folder_path}")
                except OSError as e:
                    print(f"Error deleting folder {folder_path}: {e}")

 
for nodeID in nodeIDs:
   
    print("========================NODES========================")
    print("Syncing node data for node "+ nodeID)
    currentDate = startDate
    includeStatements = " "
    
    while currentDate <= endDate:
        includeStatements = " "
        print("========================DATES========================")
        currentDateStr = currentDate.strftime("%Y_%m_%d")
        currentDate   += delta

        for sensorID in sensorIDs:
            print("========================SENSORS========================")
            print("Syncing data from node " + nodeID + ", sensor ID " + sensorID +  " for the date of " + currentDateStr)
            includeStatement = "--include='*"+  sensorID + "_" + currentDateStr +".csv' "
            includeStatements = includeStatements + includeStatement;
                
        sysStr = 'rsync -avzrtu -e "ssh -p 2222" ' +  includeStatements+ "--include='*/' --exclude='*' mints@mintsdata.utdallas.edu:/mfs/io/groups/lary/gitHubRepos/raw/" + nodeID + " " + dataFolder
        print(sysStr)
        os.system(sysStr)

        sysStr = 'rsync -avzrtu -e "ssh -p 2222" ' +  includeStatements+ "--include='*/' --exclude='*' mints@mintsdata.utdallas.edu:/mfs/io/groups/lary/mintsData/raw/" + nodeID + " " + dataFolder
        print(sysStr)
        os.system(sysStr)

        sysStr = 'rsync -avzrtu -e "ssh -p 2222" ' +  includeStatements+ "--include='*/' --exclude='*' mints@mintsdata.utdallas.edu:/home/mints/raw/" + nodeID + " " + dataFolder
        print(sysStr)
        os.system(sysStr)


delete_empty_folders(dataFolder)
delete_empty_folders(dataFolderMqtt)