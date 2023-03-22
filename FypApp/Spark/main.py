import pandas as pd
import requests
import json
import csv
import os
import glob
from datetime import date

# get today date
today_is = date.today().strftime("%Y-%m-%d")
#print(today_is) #output: 2023-03-14 #test code

# make a directory to save the downloaded csv files
save_dir = "/app/data"

# to create the directory if not existed
if not os.path.exists(save_dir):
    os.makedirs(save_dir)

country = ["australia", "singapore"]

for i in range(len(country)):
    # Set up the API endpoint URL
    url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/" + country[i] + "/" + "2022-06-01" + "/" + today_is + "?unitGroup=metric&include=days&key=8MJQ4966D6CWWU5GBV7KBHBDS&contentType=csv"
    #print(url) #test code

    try:
        df=pd.read_csv(url)

        # set up the path to save the csv files
        filePath = os.path.join(save_dir, "data_"+country[i]+".csv")

        # save the csv files
        df.to_csv(filePath, index=False)
        #print("data_"+country[i]+".csv") #test code

    except Exception as e:
        print(f"Error occurred while processing {country[i]}: {e}")

#concat
os.chdir("/app/data")

extension = "csv"
all_filenames = [i for i in
    glob.glob('*.{}'.format(extension))]

#combine all files in the llist
combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames])
#export to csv
combined_csv.to_csv("combined_csv.csv", index=False, encoding='utf-8-sig')
