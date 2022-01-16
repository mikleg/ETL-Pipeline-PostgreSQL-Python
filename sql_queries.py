# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time_play"

# CREATE TABLES
#songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
songplay_table_create = "CREATE TABLE IF NOT EXISTS songplay(user_id INT, song_id INT, artist_id INT, session_id INT\
, location VARCHAR(128), item_in_session INT, user_agent VARCHAR(128), PRIMARY KEY(item_in_session, session_id));"        
#user_id, first_name, last_name, gender, level
user_table_create = "CREATE TABLE IF NOT EXISTS users(user_id INT, user_last_name VARCHAR(50), user_gender VARCHAR(1), user_first_name VARCHAR(50)\
, level VARCHAR(4), PRIMARY KEY(user_id));"  
#song_id, title, artist_id, year, duration
song_table_create = "CREATE TABLE IF NOT EXISTS songs(song_id varchar(22), song_title VARCHAR(150), artist_id INT, year INT, duration REAL, PRIMARY KEY(song_id));" 
#artist_id, name, location, latitude, longitude
artist_table_create = "CREATE TABLE IF NOT EXISTS artists(artist_id INT, artist_name VARCHAR(150), artist_latitude DOUBLE PRECISION\
, artist_longitude DOUBLE PRECISION, PRIMARY KEY(artist_id));"
#start_time, hour, day, week, month, year, weekday
time_table_create = "CREATE TABLE IF NOT EXISTS time_play(start_time TIMESTAMP, session_id INT,  item_in_session INT\
, hour INT,  day INT, week INT, month INT, year INT, weekday INT, PRIMARY KEY(item_in_session, session_id));"

# INSERT RECORDS

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")


time_table_insert = ("""
""")

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
#create_table_queries = [songplay_table_create, user_table_create]
#drop_table_queries = [songplay_table_drop]