# This code takes each pickled JSON file and parses them into Dataframes
# Note: This code simply "extracts" the player statistics we will work with for feature engineering in the next script

# Prerequisites:
# 1. Insert directory pathnames in lines: 26, 270, 271, 290, 291

import pandas as pd
import numpy as np
import glob
import pickle
import json
from pandas.io.json import json_normalize
from datetime import timedelta, date
import time

def read_pickle(file):
    with open(file, 'rb') as picklefile:
        data = pickle.load(picklefile)
    return(data)

def write_pickle(file, data):
    with open(file, 'wb') as picklefile:
        pickle.dump(data, picklefile)

# Get list of pickled JSON filenames to be parsed
pkl_file_list = glob.glob('*****************/*.pkl') # Replace with directory pathname where pickled JSON files from first code were stored

## This For loop goes through each JSON object and creates Dataframes:
## df_game_full has all information related to the game (DataFrame version of JSON). This is organized at a game level.
## df_players has all statistics related to the players. This is organized at a player-game level (there are duplicate players)

df_game_full = pd.DataFrame() # Will contain all general game
df_players = pd.DataFrame() # Will contain player statistics 

count = 0

for file in pkl_file_list:
    r = read_pickle(file)
    print(file)
    print(str(count))

    if 'games' in r['league']:
        df = json_normalize(r['league']['games'])

        # Append GAME data to game dataframe
        df_game = json_normalize(r['league']['games'])
        df_game_full = df_game_full.append(df_game)
        #print("Game Dataframe Created")

        # CREATE home_players dictionary
        home_list = []
        df_home_plyr_full = pd.DataFrame()
        #print("Creating HOME dictionary")

        df_home_plyr_dropna = df['game.home.players'].dropna().reset_index(drop = True)

        for row in range(0, len(df_home_plyr_dropna)):

            for plyr in range(0, len(df_home_plyr_dropna[row])):
                home_plyr = {}

                # add game ID
                home_plyr['game_id'] = df['game.id'][row]
                home_plyr['home_away'] = "home"

                # player general info
                home_plyr['preferred_name'] = df_home_plyr_dropna[row][plyr]['preferred_name']
                home_plyr['first_name'] = df_home_plyr_dropna[row][plyr]['first_name']
                home_plyr['last_name'] = df_home_plyr_dropna[row][plyr]['last_name']
                home_plyr['plyr_id'] = df_home_plyr_dropna[row][plyr]['id']
                home_plyr['plyr_position'] = df_home_plyr_dropna[row][plyr]['position']
                home_plyr['plyr_primary_position'] = df_home_plyr_dropna[row][plyr]['primary_position']

                #pitching
                try:
                    home_plyr['era'] = df_home_plyr_dropna[row][plyr]['statistics']['pitching']['overall']['era']
                except:
                    home_plyr['era'] = np.nan

                try:
                    home_plyr['whip'] = df_home_plyr_dropna[row][plyr]['statistics']['pitching']['overall']['whip']
                except:
                    home_plyr['whip'] = np.nan

                try:
                    home_plyr['k9'] = df_home_plyr_dropna[row][plyr]['statistics']['pitching']['overall']['k9']
                except:
                    home_plyr['k9'] = np.nan

                try:
                    home_plyr['pitch_count'] = df_home_plyr_dropna[row][plyr]['statistics']['pitching']['overall']['pitch_count']
                except:
                    home_plyr['pitch_count'] = np.nan
         
                try:
                    home_plyr['obp'] = df_home_plyr_dropna[row][plyr]['statistics']['hitting']['overall']['obp']
                except:
                    home_plyr['obp'] = np.nan

                try:
                    home_plyr['ops'] = df_home_plyr_dropna[row][plyr]['statistics']['hitting']['overall']['ops']
                except:
                    home_plyr['ops'] = np.nan

                try:
                    home_plyr['slg'] = df_home_plyr_dropna[row][plyr]['statistics']['hitting']['overall']['slg']
                except:
                    home_plyr['slg'] = np.nan

                try:
                    home_plyr['rbi'] = df_home_plyr_dropna[row][plyr]['statistics']['hitting']['overall']['rbi']
                except:
                    home_plyr['rbi'] = np.nan

                try:
                    home_plyr['avg'] = df_home_plyr_dropna[row][plyr]['statistics']['hitting']['overall']['avg']
                except:
                    home_plyr['avg'] = np.nan

                try:
                    home_plyr['av_risp'] = df_home_plyr_dropna[row][plyr]['statistics']['hitting']['overall']['ab_risp']
                except:
                    home_plyr['av_risp'] = np.nan

                try:
                    home_plyr['hit_risp'] = df_home_plyr_dropna[row][plyr]['statistics']['hitting']['overall']['hit_risp']
                except:
                    home_plyr['hit_risp'] = np.nan

                try:
                    home_plyr['runs'] = df_home_plyr_dropna[row][plyr]['statistics']['hitting']['overall']['runs']['total']
                except:
                    home_plyr['runs'] = np.nan

                # fielding
                try:
                    home_plyr['error'] = df_home_plyr_dropna[row][plyr]['statistics']['fielding']['overall']['error']
                except:
                    home_plyr['error'] = np.nan

                try:
                    home_plyr['fpct'] = df_home_plyr_dropna[row][plyr]['statistics']['fielding']['overall']['fpct']
                except:
                    home_plyr['fpct'] = np.nan

                try:
                    home_plyr['assists'] = df_home_plyr_dropna[row][plyr]['statistics']['fielding']['overall']['assists']['total']
                except:
                    home_plyr['assists'] = np.nan

                home_list.append(home_plyr)

        df_home_plyr = pd.DataFrame(home_list)   
        df_players = df_players.append(df_home_plyr)
        #print("DONE creating HOME dictionary")

        ## Create AWAY_player dictionary ##
        away_list = []
        #print("Creating AWAY dictionary")

        df_away_plyr_dropna = df['game.away.players'].dropna().reset_index(drop = True)

        for row in range(0, len(df_away_plyr_dropna)):

            for plyr in range(0, len(df_away_plyr_dropna[row])):
                away_plyr = {}

                # add game ID
                away_plyr['game_id'] = df['game.id'][row]
                away_plyr['home_away'] = "away"

                # player general info
                away_plyr['preferred_name'] = df_away_plyr_dropna[row][plyr]['preferred_name']
                away_plyr['first_name'] = df_away_plyr_dropna[row][plyr]['first_name']
                away_plyr['last_name'] = df_away_plyr_dropna[row][plyr]['last_name']
                away_plyr['plyr_id'] = df_away_plyr_dropna[row][plyr]['id']
                away_plyr['plyr_position'] = df_away_plyr_dropna[row][plyr]['position']
                away_plyr['plyr_primary_position'] = df_away_plyr_dropna[row][plyr]['primary_position']

                try:
                    away_plyr['era'] = df_away_plyr_dropna[row][plyr]['statistics']['pitching']['overall']['era']
                except:
                    away_plyr['era'] = np.nan

                try:
                    away_plyr['whip'] = df_away_plyr_dropna[row][plyr]['statistics']['pitching']['overall']['whip']
                except:
                    away_plyr['whip'] = np.nan

                try:
                    away_plyr['k9'] = df_away_plyr_dropna[row][plyr]['statistics']['pitching']['overall']['k9']
                except:
                    away_plyr['k9'] = np.nan

                try:
                    away_plyr['pitch_count'] = df_away_plyr_dropna[row][plyr]['statistics']['pitching']['overall']['pitch_count']
                except:
                    away_plyr['pitch_count'] = np.nan

                try:
                    away_plyr['obp'] = df_away_plyr_dropna[row][plyr]['statistics']['hitting']['overall']['obp']
                except:
                    away_plyr['obp'] = np.nan

                try:
                    away_plyr['ops'] = df_away_plyr_dropna[row][plyr]['statistics']['hitting']['overall']['ops']
                except:
                    away_plyr['ops'] = np.nan

                try:
                    away_plyr['slg'] = df_away_plyr_dropna[row][plyr]['statistics']['hitting']['overall']['slg']
                except:
                    away_plyr['slg'] = np.nan

                try:
                    away_plyr['rbi'] = df_away_plyr_dropna[row][plyr]['statistics']['hitting']['overall']['rbi']
                except:
                    away_plyr['rbi'] = np.nan

                try:
                    away_plyr['avg'] = df_away_plyr_dropna[row][plyr]['statistics']['hitting']['overall']['avg']
                except:
                    away_plyr['avg'] = np.nan

                try:
                    away_plyr['av_risp'] = df_away_plyr_dropna[row][plyr]['statistics']['hitting']['overall']['ab_risp']
                except:
                    away_plyr['av_risp'] = np.nan

                try:
                    away_plyr['hit_risp'] = df_away_plyr_dropna[row][plyr]['statistics']['hitting']['overall']['hit_risp']
                except:
                    away_plyr['hit_risp'] = np.nan

                try:
                    away_plyr['runs'] = df_away_plyr_dropna[row][plyr]['statistics']['hitting']['overall']['runs']['total']
                except:
                    away_plyr['runs'] = np.nan

                # fielding
                try:
                    away_plyr['error'] = df_away_plyr_dropna[row][plyr]['statistics']['fielding']['overall']['error']
                except:
                    away_plyr['error'] = np.nan

                try:
                    away_plyr['fpct'] = df_away_plyr_dropna[row][plyr]['statistics']['fielding']['overall']['fpct']
                except:
                    away_plyr['fpct'] = np.nan

                try:
                    away_plyr['assists'] = df_away_plyr_dropna[row][plyr]['statistics']['fielding']['overall']['assists']['total']
                except:
                    away_plyr['assists'] = np.nan

                away_list.append(away_plyr)

        df_away_plyr = pd.DataFrame(away_list)
        df_players = df_players.append(df_away_plyr)
        #print("DONE creating AWAY dictionary")

    else:
        continue

    count += 1

    # Save files incrementally, after ever 50 days worth of data is collected
    if count % 50 == 0:
        print("Creating File Number ", str(count))
        game_filename = "*****************/game_{}.pkl".format(count) # Replace with pathname to folder
        player_filename = "*****************/player_{}.pkl".format(count) # Replace with pathname to folder
        
        # Pickle each file
        with open(game_filename, 'wb') as picklefile1:
            pickle.dump(df_game_full, picklefile1)
        
        with open(player_filename, 'wb') as picklefile2:
            pickle.dump(df_players, picklefile2)
        
        print("DONE creating File Number ", str(count))
    
        #clear the dataframe
        df_game_full = df_game_full.iloc[0:0]
        df_players = df_players.iloc[0:0]
    else:
        continue


# Pickle final file
game_filename = "*******************/game_{}.pkl".format(count) # Replace with pathname to folder (same as above)
player_filename = "*******************/player_{}.pkl".format(count) # Replace with pathname to folder (same as above)
with open(game_filename, 'wb') as picklefile1:
    pickle.dump(df_game_full, picklefile1)

with open(player_filename, 'wb') as picklefile2:
    pickle.dump(df_players, picklefile2)

print("DONE creating File Number ", str(count))
print("DONE DONE DONE!")