# This code sends a get request to the Sportsradar API, which returns a JSON object.
# This specifically returns game summary data.
# This includes team lineups as well as team and player statistics for a given game.
# Sportradar Documentation: https://developer.sportradar.com/docs/read/baseball/MLB_v66#game-summary

# Prerequisites: 
# 1. Create an account with Sportradar, and generate a free trial API Key. Put API key in line: 71
# https://developer.sportradar.com/member/register
# 2. Decide on directory to store picked JSON files returned by the API in lines: 91, 99

import pandas as pd
import numpy as np
import requests
import json
import time
import pickle
from datetime import timedelta, date

def daterange(date1, date2):
    for n in range(int ((date2 - date1).days)+1):
        yield (date1 + timedelta(n))

def write_pickle(file, data):
    with open(file, 'wb') as picklefile:
        pickle.dump(data, picklefile)

# Create a list of dates for API request
#2015
dt_15_list = []
start_dt = date(2015, 4, 5) # first day of regular season
end_dt = date(2015, 10, 4) # last day of regular season
for dt in daterange(start_dt, end_dt):
    dt_15_list.append(dt.strftime("%Y/%m/%d"))
    
#2016    
dt_16_list = []
start_dt = date(2016, 4, 3)
end_dt = date(2016, 10, 2)
for dt in daterange(start_dt, end_dt):
    dt_16_list.append(dt.strftime("%Y/%m/%d"))

#2017
dt_17_list = []
start_dt = date(2017, 4, 2)
end_dt = date(2017, 10, 2)
for dt in daterange(start_dt, end_dt):
    dt_17_list.append(dt.strftime("%Y/%m/%d"))

#2018
dt_18_list = []
start_dt = date(2018, 3, 29)
end_dt = date(2018, 10, 1)
for dt in daterange(start_dt, end_dt):
    dt_18_list.append(dt.strftime("%Y/%m/%d"))

#2019
dt_19_list = []
start_dt = date(2019, 3, 20)
end_dt = date(2019, 9, 29)
for dt in daterange(start_dt, end_dt):
    dt_19_list.append(dt.strftime("%Y/%m/%d"))

all_dates_list = dt_15_list + dt_16_list + dt_17_list + dt_18_list + dt_19_list

### Grab JSONs and PICKLE ###
# Register for a key on sportradar.com, and define it here
key = '***********'

count = 0
bad_date_list = []

# send request to API for each day of every season
for date in all_dates_list:
    print(str(date), " ", str(count))
    url = "https://api.sportradar.us/mlb/trial/v6.6/en/games/{}/summary.json?api_key={}".format(date, key)
    print(url)
    response = requests.get(url)
    
    if response.status_code != 200:
        bad_date_list.append(date)
        print("bad date: ", str(date))
        
    else:
        r = response.json()
        print("Response Successful!")
        name = str(date).replace('/', '') + "_" + str(count)
        filename = '******************{}.pkl'.format(name) # add filepath to store json files
        write_pickle(filename, r)
        print("Pickle Successful")
    
    count += 1
    time.sleep(1)

print("Pickle Bad Date List")
write_pickle("******************/bad_date_list.pkl", bad_date_list) # save a list of "bad dates" for reference
print("DONE DONE DONE!")