## Introduction

The company wants to analyze the data they collect about songs and user activity on their new music streaming app. The analytics are interested in understanding what songs users listen to in order to find patterns and predict behavior. In the current stage of the app, they can't use such information because it is scattered between logs and metadata files in JSON format. 
They want that the data engineer should create a Postgres database to make the above task easier. Practically this means that the data engineer will create the database schema and ETL pipeline for such analysis. 
Because in the current stage the app is written on Python, the data engineer should use in the project Python exclusively and all used SQL queries should be in a separate file.

## Project Description

The project defined fact and dimension tables for a star schema for analytics and wrote an ETL pipeline that transfers data from files in two local directories to these tables in Postgres using Python & SQL

>The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

>> song_data/A/B/C/TRABCEI128F424C983.json
>> song_data/A/A/B/TRAABJL12903CDCF1A.json

>And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

>>{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}


>The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations.

>The log files in the dataset partitioned by year and month. For example, here are filepaths to two files in this dataset.

>>log_data/2018/11/2018-11-12-events.json
>>log_data/2018/11/2018-11-13-events.json

>And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.

![example](/docs/ex1.png)

## Files
The project includes six files:

1. test.ipynb -- the notebook to test the current content of all tables.
2. create_tables.py -- creation and dropping of all tables.  Before every run of ETL files, it should be started.
3. etl.py -- the main ETL file. It reads JSON files and fills all data into the database.
4. etl.ipynb -- the draft of the whole ETL pipeline, it performs the same as etl.py but interactively (thanks to Jupyter) and with very few differences.
5. sql_queries.py -- holds all SQL queries.
6. README.md -- the brief description of the project.

## Database Schema
The project has created a denormalized database with a star schema optimized for fast aggregation of requests for the analysis of song playback. It includes one song play fact table and four dimension tables for songs, users, time, and artists.
![DB schema](/docs/db_schema.png)


## ETL Pipeline

The ETL pipeline extracts data from JSON files from two folders in the data directory:
song_data 
log_data 
using code in the etl.py

After that, it transforms and loads the data into the five tables described above in the Database Schema using code in the etl.py and SQL queries in sql_queries.py.

Before starting etl.py, all tables should exist. In the project, this purpose has the create_tables.py file.

## Steps to Run the Pipeline:

1. Make sure that the data folder exists with the corresponding log_data and song_data folders with matching JSON files inside the working directory.  
    
2. Using terminal, start create_tables.py by command "python create_tables.py"

3. Using terminal, start etl.py by command "python etl.py"

4. Using Jupyter Notebook start the notebook "test.ipynb" and check the results

## Sample Queries

SELECT level, count(*)  
FROM songplays  
GROUP BY level;

![q1](/docs/q1.png)



SELECT users.level, count(*), gender  
FROM songplays  
INNER JOIN users  
ON songplays.user_id = users.user_id  
GROUP BY users.level, gender;

![q2](/docs/q2.png)