import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        artist_name TEXT,
        auth TEXT,
        first_name TEXT,
        gender TEXT,
        itemsession INT,
        last_name TEXT,
        length NUMERIC,
        level TEXT,
        location TEXT,
        method TEXT,
        page TEXT,
        registration BIGINT,
        session_id INT,
        song TEXT,
        status INT,
        ts BIGINT,
        user_agent TEXT,
        user_id INT
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs           INT,
        artist_id           TEXT,
        artist_latitude     TEXT,
        artist_longitude    TEXT,
        artist_location     TEXT,
        artist_name         TEXT,
        song_id             TEXT,
        title               TEXT,
        duration            NUMERIC,
        year                INT
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL SORTKEY,
        user_id BIGINT NOT NULL,
        level TEXT,
        song_id TEXT, 
        artist_id TEXT, 
        session_id INT, 
        location TEXT, 
        user_agent TEXT
    )
        diststyle key 
        distkey(user_id);

""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id INT PRIMARY KEY, 
        first_name TEXT, 
        last_name TEXT, 
        gender TEXT, 
        level TEXT
    )
        diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id TEXT PRIMARY KEY, 
        title TEXT, 
        artist_id TEXT NOT NULL, 
        year INT, 
        duration NUMERIC
    )
        diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
        artist_id TEXT PRIMARY KEY, 
        name TEXT, 
        location TEXT, 
        latitude TEXT,  
        longitude TEXT
    )
        diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
        start_time TIMESTAMP PRIMARY KEY SORTKEY, 
        hour INT, 
        day INT, 
        week INT, 
        month INT, 
        year INT, 
        weekday INT
    )
        diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS JSON {}
    REGION 'us-west-2' 
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS JSON 'auto'
    REGION 'us-west-2'
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(
        start_time,
        user_id,
        level,
        song_id, 
        artist_id, 
        session_id, 
        location, 
        user_agent
    ) 
    SELECT 
        DISTINCT timestamp 'epoch' + se.ts/1000 * interval '1 second' AS start_time,
        se.user_id AS user_id,
        se.level AS level,
        ss.song_id AS song_id,
        ss.artist_id AS artist_id,
        se.session_id AS session_id,
        se.location AS location,
        se.user_agent AS user_agent
    FROM staging_events AS se
    JOIN staging_songs AS ss
    ON se.song = ss.title
    WHERE se.page = 'NextSong'
    ;

""")

user_table_insert = ("""
    INSERT INTO users(
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level
    )
    SELECT 
        DISTINCT user_id,
        first_name,
        last_name,
        gender,
        level
    FROM staging_events
    WHERE page = 'NextSong'
    ;
""")

song_table_insert = ("""
    INSERT INTO songs(
        song_id, 
        title, 
        artist_id, 
        year, 
        duration
    )
    SELECT
        DISTINCT song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    ;
""")

artist_table_insert = ("""
    INSERT INTO artists(
        artist_id, 
        name, 
        location, 
        latitude, 
        longitude
    )
    SELECT 
        DISTINCT artist_id, 
        artist_name, 
        artist_location, 
        artist_latitude, 
        artist_longitude
    FROM staging_songs
    ;
""")

time_table_insert = ("""
    INSERT INTO time(
        start_time, 
        hour, 
        day, 
        week, 
        month, 
        year, 
        weekday
    )
    SELECT 
        DISTINCT timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time,
        EXTRACT(hour FROM start_time) AS hour,
        EXTRACT(day FROM start_time) AS day,
        EXTRACT(week FROM start_time) AS week,
        EXTRACT(month FROM start_time) AS month,
        EXTRACT(year FROM start_time) AS year,
        EXTRACT(weekday  FROM start_time) AS weekday    
    FROM staging_events
    WHERE page = 'NextSong'
    ;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
