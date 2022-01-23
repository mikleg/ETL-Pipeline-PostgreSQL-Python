# DROP TABLES

songplay_table_drop =\
"""DROP 
  TABLE IF EXISTS songplays;"""
user_table_drop =\
"""DROP 
  TABLE IF EXISTS users;"""
song_table_drop =\
"""DROP 
  TABLE IF EXISTS songs;"""
artist_table_drop =\
"""DROP 
  TABLE IF EXISTS artists;"""
time_table_drop =\
"""DROP 
  TABLE IF EXISTS time;"""

# CREATE TABLES
#songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
songplay_table_create =\
"""CREATE TABLE IF NOT EXISTS songplays (
  songplay_id SERIAL PRIMARY KEY, 
  user_id VARCHAR (11), 
  song_id VARCHAR(22), 
  artist_id VARCHAR(22), 
  session_id INT, 
  location VARCHAR(128), 
  user_agent VARCHAR(192), 
  level VARCHAR(4), 
  start_time TIMESTAMP
);"""
  
#user_id, first_name, last_name, gender, level
user_table_create =\
"""CREATE TABLE IF NOT EXISTS users(
  user_id VARCHAR (11), 
  last_name VARCHAR(64), 
  gender VARCHAR(1), 
  first_name VARCHAR(64), 
  level VARCHAR(4), 
  PRIMARY KEY(user_id)
);"""  
#song_id, title, artist_id, year, duration
song_table_create = """CREATE TABLE IF NOT EXISTS songs(
  song_id VARCHAR(22), 
  song_title VARCHAR(128), 
  duration DOUBLE PRECISION, 
  year INT, 
  artist_id VARCHAR(22), 
  PRIMARY KEY(song_id)
);""" 
#artist_id, name, location, latitude, longitude
artist_table_create = \
"""CREATE TABLE IF NOT EXISTS artists(
  artist_id VARCHAR(22), 
  name VARCHAR(128), 
  latitude DOUBLE PRECISION, 
  longitude DOUBLE PRECISION, 
  location VARCHAR(128), 
  PRIMARY KEY(artist_id)
);"""
#start_time, hour, day, week, month, year, weekday
time_table_create = """CREATE TABLE IF NOT EXISTS time(
  session_id INT, 
  item_in_session INT, 
  start_time TIMESTAMP, 
  hour INT, 
  day INT, 
  week INT, 
  month INT, 
  year INT, 
  weekday INT, 
  PRIMARY KEY(item_in_session, session_id)
);"""

# INSERT RECORDS

songplay_table_insert = \
"""INSERT INTO songplays 
VALUES 
  (
    DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s
  );
"""

user_table_insert = \
"""INSERT INTO users (user_id, last_name, gender, first_name, level)
    VALUES (%s, %s, %s, %s, %s)   
    ON CONFLICT (user_id)
    DO UPDATE SET level = EXCLUDED.level"""



#user_table_insert = \
#"""INSERT INTO users 
#VALUES 
#  (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;"""

song_table_insert = \
"""INSERT INTO songs 
VALUES 
  (%s, %s, %s, %s, %s);"""


artist_table_insert = \
"""INSERT INTO artists 
VALUES 
  (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;"""


time_table_insert = \
"""INSERT INTO time 
VALUES 
  (%s, %s, %s, %s, %s, %s, %s, %s, %s);"""

# FIND SONGS

song_select = \
"""SELECT 
  song_id, 
  artist_id 
FROM 
  songs 
WHERE 
  song_title = %s 
  AND duration = %s;"""

 
# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create\
                        , song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop,\
                      song_table_drop, artist_table_drop, time_table_drop]
