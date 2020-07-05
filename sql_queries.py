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
    songplay_id INT IDENTITY(0, 1) PRIMARY KEY SORTKEY, 
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
    COPY staging_events FROM 's3://udacity-dend/log_data'
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    FORMAT AS JSON 's3://udacity-dend/log_json_path.json';
""").format(config.get('IAM_ROLE', 'ARN'))

staging_songs_copy = ("""
    COPY staging_songs FROM 's3://udacity-dend/song_data'
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    JSON 'auto';
""").format(config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
    ev.ts AS start_time
    ,ev.userId AS user_id
    ,ev.level AS level
    ,so.song_id AS song_id
    ,so.artist_id AS artist_id
    ,ev.sessionId AS session_id
    ,ev.location AS location
    ,ev.userAgent AS user_agent
FROM staging_events ev
JOIN staging_songs so ON (ev.song = so.title AND ev.artist = so.artist_name)
WHERE ev.page='NextSong'

""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT 
    userId
    ,firstName
    ,lastName
    ,gender
    ,level
FROM staging_events ev
WHERE ev.page='NextSong'
AND userId is NOT null
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT 
    song_id
    ,title
    ,artist_id
    ,year
    ,duration 
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
    artist_id
    ,artist_name
    ,artist_location
    ,artist_latitude
    ,artist_longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT 
    ts AS start_time
    EXTRACT(HOUR FROM start_time),
    EXTRACT(DAY FROM start_time),
    EXTRACT(WEEK FROM start_time),
    EXTRACT(MONTH FROM start_time),
    EXTRACT(YEAR FROM start_time),
    EXTRACT(DOW FROM start_time)
FROM staging_events 
WHERE staging_events.page='NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
