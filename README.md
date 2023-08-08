# Udacity-NDDE-Project 2

This project is part of Udacity Data Engineer Nanodegree. 

In this project, I applied what I have learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, I loaded data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

## How to Run the Python Scripts

First run create_tables.py to reset the tables, then run etl.py to fill the database.

create_tables.py: drops and creates your tables.
etl.py: reads and processes files from song_data and log_data and loads them into your tables.
sql_queries.py: contains all your sql queries.

## Database Schema Design and ETL Pipeline

A standard star model was implemented. 

The song dataset and log dataset provided in JSON format were converted to a database using Amazon Redshift.

The song dataset contains the following entry:

['num_songs', 'artist_id', 'artist_latitude', 'artist_longitude',
       'artist_location', 'artist_name', 'song_id', 'title', 'duration',
       'year']

The log dataset contains the following entry:

['artist', 'auth', 'firstName', 'gender', 'itemInSession', 'lastName',
       'length', 'level', 'location', 'method', 'page', 'registration',
       'sessionId', 'song', 'status', 'ts', 'userAgent', 'userId']

The final database consists of the following tables:

### Fact Table
1. songplays - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
### Dimension Tables
2. users - users in the app
user_id, first_name, last_name, gender, level
3. songs - songs in music database
song_id, title, artist_id, year, duration
4. artists - artists in music database
artist_id, name, location, latitude, longitude
5. time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday