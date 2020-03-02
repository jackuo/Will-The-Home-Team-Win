import pandas as pd
import numpy as np
import pickle

def read_pickle(file):
    with open(file, 'rb') as picklefile:
        data = pickle.load(picklefile)
    return(data)

def write_pickle(file, data):
    with open(file, 'wb') as picklefile:
        pickle.dump(data, picklefile)


# Generate list of filenames containing player data and game data
player_file_list = glob.glob('/Users/JacKuo14/Documents/Metis/course_work/project03_baseball/sportstradar_data2/*player*.pkl')
gamefile_list = glob.glob('/Users/JacKuo14/Documents/Metis/course_work/project03_baseball/sportstradar_data2/*game*.pkl')

# Create "Master" Dataframes with all game and player data
df_game = pd.DataFrame()
for file in gamefile_list:
    df_game = df_game.append(read_pickle(file))
    
df_play = pd.DataFrame()
for file in player_file_list:
    df_play = df_play.append(read_pickle(file))


