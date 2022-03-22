import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

#[CLUSTER]
HOST=config.get('CLUSTER', 'HOST')
DB_NAME=config.get('CLUSTER', 'DB_NAME')
DB_USER=config.get('CLUSTER', 'DB_USER')
DB_PASSWORD=config.get('CLUSTER', 'DB_PASSWORD')
DB_PORT=config.get('CLUSTER', 'DB_PORT')

#[IAM_ROLE]
ARN=config.get('IAM_ROLE', 'ARN')

#[S3]
LOG_DATA=config.get('S3', 'LOG_DATA')
LOG_JSONPATH=config.get('S3', 'LOG_JSONPATH')
SONG_DATA=config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events CASCADE"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs CASCADE"
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events
(
    artist VARCHAR,
    auth VARCHAR,
    first_name VARCHAR,
    gender CHAR(1),
    itemInSession INT,
    last_name VARCHAR,
    length DECIMAL(3,2),
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration VARCHAR,
    sessionId INT,
    song VARCHAR,
    status INT,
    ts BIGINT SORTKEY,
    useragent VARCHAR,
    user_id INT
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
    num_songs INT,
    artist_id VARCHAR NOT NULL,
    artist_latitude VARCHAR,
    artist_longitude VARCHAR,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration DECIMAL(3,2) NOT NULL,
    year INT
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay
(
    songplay_id INT PRIMARY KEY,
    start_time datetime NOT NULL REFERENCES time(start_time) DISTKEY SORTKEY,
    user_id INT NOT NULL REFERENCES users(user_id),
    level VARCHAR NOT NULL,
    song_id VARCHAR NOT NULL REFERENCES songs(song_id),
    artist_id VARCHAR NOT NULL REFERENCES artists(artist_id),
    session_id INT NOT NULL,
    location VARCHAR,
    user_agent VARCHAR
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
    user_id INT PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender CHAR(1),
    level VARCHAR NOT NULL
)
diststyle all
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
    song_id VARCHAR NOT NULL PRIMARY KEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL REFERENCES artists(artist_id),
    year VARCHAR,
    duration DECIMAL(3,2) NOT NULL
)
diststyle all
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
    artist_id VARCHAR NOT NULL PRIMARY KEY,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude VARCHAR,
    longitude VARCHAR
)
diststyle all
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
    start_time datetime NOT NULL PRIMARY KEY,
    hour INT NOT NULL, 
    day INT NOT NULL, 
    week INT NOT NULL, 
    month INT NOT NULL, 
    year INT NOT NULL, 
    weekday INT NOT NULL
)
distkey(year)
sortkey(month, day)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
credentials 'aws_iam_role':{}
region 'us-east-1'
""").format(LOG_DATA, ARN)

staging_songs_copy = ("""
COPY staging_songs FROM {}
credentials 'aws_iam_role':{},
region 'us-east-1'
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO song_play 
(songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
INSERT OR IGNORE INTO users 
(user_id, first_name, last_name, gender, level)
SELECT
    user_id,
    first_name,
    last_name,
    gender,
    level
FROM 
    staging_events
VALUES (%s, %s, %s, %s, %s)
""")

song_table_insert = ("""
INSERT INTO songs
(song_id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s)
""")

artist_table_insert = ("""
INSERT INTO artists
(artist_id, name, location, lattitude, longitude)
VALUES
(%s, %s, %s, %s, %s)
""")

time_table_insert = ("""
INSERT INTO time
(start_time, hour, day, week, month, year, weekday)
VALUES
(%s, %s, %s, %s, %s, %s, %s)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
