import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def read_song_jsons (columns_list, files_list, debug = False):
    '''the function reads json file(s) from the files_list and returns a data frame with columns = columns_list'''
    data_list = []
    j = 0
    for a_file in files_list:
        json_df = pd.read_json(files_list[j], lines=True, convert_axes=False, dtype=False)
        j+=1
        for i, row in json_df.iterrows():
            data = row[columns_list].values.tolist()
            if debug:
                print(type(data[0]))
            data_list.append(data)   
    return(pd.DataFrame(data_list, columns = columns_list))

def trim_str(string_to_trim, length):
    '''if string_to_trim is a string and the length of it is more than "length", the function truncates it to the given length'''
    if (isinstance(string_to_trim, str)):
        if len(string_to_trim) > length:
            return(string_to_trim[:length])
        else:
            return(string_to_trim)
    else:
        return(string_to_trim)

def fill_table(dataframe, cursor, q, col_max_length, debug = False):
    '''the function inserts data from the "dataframe" to the "cursor" position of a table. "col_max_length" is a list of max length for\
    string data (for non string arguments it's ignored). "q" -- SQL query for performing insert'''
    print(dataframe.columns)
    for i, row in dataframe.iterrows():
        if debug:
            print((row))
        row_list = row.tolist()    
        for index, el in enumerate(row_list):
            #print('index=',index, ' len1=',  len(row_list), ' len2=',  len(col_max_length))
            row_list[index] = trim_str(el, col_max_length[index])
        new_row = pd.Series(row_list)
        cursor.execute(q, new_row) 

def convert_to_datetime(ms):
    '''the auxiary function for converting int mileseconds to pd.timestamp'''
    df = pd.DataFrame([ms], columns = ['ms'])
    df['timestamp'] = pd.to_datetime(df['ms'] , unit='ms')
    return (df['timestamp'].iloc[0])

def process_song_file(cur, filepath):
    '''the function takes current cursor and the files path where it reads all JSON files and fills the song and artist tables'''
    # open song file
    song_col_list = ['song_id', 'title', 'duration',  'year','artist_id']
    artist_col_list = ['artist_id', 'artist_name', 'artist_latitude','artist_longitude', 'artist_location']
    df = read_song_jsons (artist_col_list + ['song_id', 'title', 'duration',  'year'], [filepath])

    # insert song record
    max_length = [22, 128, None,  None, 22]
    fill_table(df[song_col_list], cur, song_table_insert, max_length)
    
    # insert artist record
    max_length = [22, 128, None,  None, 128]
    fill_table(df[artist_col_list], cur, artist_table_insert, max_length)

def process_log_file(cur, filepath):
    '''the function takes current cursor and the files path where it reads all JSON files and fills the time, user and songolay tables'''
    columns_log = ['artist', 'auth', 'firstName', 'gender', 'itemInSession', 'lastName', 'length', 'level', 'location', 'method', 'page', 'registration'\
           ,'sessionId', 'song', 'status', 'ts', 'userAgent', 'userId']
    # open log file
    df = read_song_jsons(columns_log, [filepath])

    # filter by NextSong action
    df = df[df['page'] == 'NextSong'].copy()
    time_df = df

    # convert timestamp column to datetime
    time_df['timestamp'] = pd.to_datetime(time_df['ts'] , unit='ms') 
    time_df['new_col'] = time_df['timestamp'].apply(lambda row: [row.hour, row.day, row.weekofyear, row.month, row.year, row.dayofweek])
    time_df[['hour', 'day', 'weekofyear', 'month', 'year', 'dayofweek']]= pd.DataFrame(time_df['new_col'].tolist(), index = time_df.index)
    columns_time =  ['sessionId', 'itemInSession', 'timestamp', 'hour', 'day', 'weekofyear', 'month', 'year', 'dayofweek']
    max_length = [None, None, None,  None, None, None, None, None, None]
    fill_table( time_df[columns_time], cur, time_table_insert, max_length)
    
    # Fill user table
    user_columns = ['userId', 'lastName', 'gender', 'firstName', 'level']
    user_df = df[user_columns].drop_duplicates(subset='userId')
    max_length = [11, 64, 1,  64, 4]
    fill_table(user_df, cur, user_table_insert, max_length)
    
    # insert songplay records
    for index, row in df.iterrows():
        # get songid and artistid from the song table
        cur.execute(song_select, (row.song, float(str(row.length))))
        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
        # insert songplay record
        my_dict = row.to_dict()
        songplay_data = (trim_str(my_dict.get('userId'), 11), trim_str(songid,22), trim_str(artistid,22), my_dict.get('sessionId')\
                     , trim_str(my_dict.get('location'), 128), trim_str(my_dict.get('userAgent'),192), trim_str(my_dict.get('level'),4), convert_to_datetime(my_dict.get('ts')))
        cur.execute(songplay_table_insert, songplay_data)
        


def process_data(cur, conn, filepath, func):
    '''the function takes the current cursor, connection, the function name, and the files path where it reads all JSON files and runs the function'''
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()