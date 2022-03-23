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
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

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
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration DOUBLE PRECISION,
    sessionId VARCHAR,
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
    artist_id VARCHAR,
    artist_latitude VARCHAR,
    artist_longitude VARCHAR,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year INT SORTKEY
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay
(
    songplay_id INT IDENTITY(1,1) PRIMARY KEY,
    start_time datetime NOT NULL REFERENCES time(start_time) DISTKEY SORTKEY,
    user_id INT NOT NULL REFERENCES users(user_id),
    level VARCHAR NOT NULL,
    song_id VARCHAR REFERENCES songs(song_id),
    artist_id VARCHAR REFERENCES artists(artist_id),
    session_id VARCHAR NOT NULL,
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
    duration FLOAT NOT NULL
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
iam_role {}
COMPUPDATE OFF region 'us-west-2'
json {}
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs FROM {}
iam_role {}
COMPUPDATE OFF region 'us-west-2'
json 'auto'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay
(start_time, user_id, level,song_id, artist_id, session_id, location, user_agent)
SELECT
    timestamp 'epoch' + se.ts/1000 * interval '1 second',
    se.user_id,
    se.level,
    s.song_id,
    s.artist_id,
    se.location,
    se.sessionId,
    se.useragent
FROM
    staging_events se
    LEFT JOIN songs s ON s.title = se.song and s.duration = se.length
WHERE
    se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users
(user_id, first_name, last_name, gender, level)
SELECT
    distinct user_id,
    first_name,
    last_name,
    gender,
    level
FROM 
    staging_events
WHERE
    user_id IS NOT NULL
ORDER BY
    ts DESC;
""")

song_table_insert = ("""
INSERT INTO songs
SELECT
    distinct song_id,
    title,
    artist_id,
    year,
    duration
FROM
    staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists
SELECT
    distinct artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM
    staging_songs
WHERE 
    artist_id IS NOT NULL;   
""")

time_table_insert = ("""
INSERT INTO time
SELECT
    distinct timestamp 'epoch' + ts/1000 * interval '1 second' AS start,
    EXTRACT(hour FROM start),
    EXTRACT(day FROM start),
    EXTRACT(week FROM start),
    EXTRACT(month FROM start),
    EXTRACT(year FROM start),
    EXTRACT(weekday FROM start)
FROM
    staging_events
WHERE ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [artist_table_insert,time_table_insert, user_table_insert, song_table_insert, songplay_table_insert]
