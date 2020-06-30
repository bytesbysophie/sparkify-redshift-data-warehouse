import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS song_event_log (
    artist varchar, 
    auth varchar, 
    firstName varchar, 
    gender varchar, 
    itemInSession int, 
    lastName varchar, 
    length DECIMAL, 
    level varchar, 
    location varchar, 
    method varchar, 
    page varchar, 
    registration varchar, 
    sessionId int, 
    song varchar, 
    status int, 
    ts timestamp, 
    userAgent varchar, 
    userId varchar);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS songs_info (
    song_id varchar,
    title varchar,
    artist_id varchar,
    artist_name varchar,
    artist_location varchar,
    artist_latitude numeric,
    artist_longitude numeric,
    year int,
    num_songs int,
    duration float8);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id SERIAL PRIMARY KEY, 
    start_time timestamp, 
    user_id varchar REFERENCES users (user_id), 
    level varchar, 
    song_id varchar REFERENCES songs (song_id), 
    artist_id varchar REFERENCES artists (artist_id), 
    session_id int, 
    location varchar, 
    user_agent varchar
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id varchar PRIMARY KEY, 
    first_name varchar, 
    last_name varchar,
    gender varchar,
    level varchar
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY, 
    title varchar, 
    artist_id varchar,
    year int NOT NULL,
    duration float8 NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY,
    name varchar, 
    location varchar, 
    latitude numeric, 
    longitude numeric
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int
);
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
VALUES (%s, %s, %s, %s, %s) 
ON CONFLICT DO NOTHING
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week,  month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
