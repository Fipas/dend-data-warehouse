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
        artist text,
        auth text,
        first_name text,
        gender char,
        item_in_session integer,
        last_name text,
        length float,
        level text,
        location text,
        method text,
        page text,
        registration text,
        session_id integer,
        song text,
        status int,
        ts timestamp,
        user_agent text,
        user_id integer
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs integer,
        artist_id text NOT NULL,
        artist_latitude float,
        artist_longitude float,
        artist_location text,
        artist_name text,
        song_id text NOT NULL,
        title text,
        duration float,
        year integer
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id integer IDENTITY(0,1) NOT NULL,
        start_time timestamp,
        user_id integer,
        level text,
        song_id text,
        artist_id text,
        session_id int,
        location text,
        user_agent text,
        PRIMARY KEY(songplay_id),
        FOREIGN KEY(start_time) REFERENCES time(start_time),
        FOREIGN KEY(user_id) REFERENCES users(user_id),
        FOREIGN KEY(song_id) REFERENCES songs(song_id),
        FOREIGN KEY(artist_id) REFERENCES artists(artist_id)
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id integer PRIMARY KEY NOT NULL,
        first_name text,
        last_name text,
        gender char,
        level text
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id text PRIMARY KEY NOT NULL,
        title text,
        artist_id text,
        year integer,
        duration float
    )
""")

artist_table_create = ("""
     CREATE TABLE IF NOT EXISTS artists(
        artist_id text PRIMARY KEY NOT NULL,
        name text,
        location text,
        latitude float,
        longitude float
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time timestamp PRIMARY KEY NOT NULL,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events 
    FROM {}
    TIMEFORMAT 'epochmillisecs'
    IAM_ROLE {}
    JSON {}
""").format(config.get("S3", "LOG_DATA"), config.get("IAM_ROLE", "ARN"), config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    IAM_ROLE {}
    JSON 'auto';
""").format(config.get("S3", "SONG_DATA"), config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT se.ts, se.user_id, se.level, ss.song_id, ss.artist_id, se.session_id, se.location, se.user_agent
    FROM staging_events AS se
    JOIN staging_songs AS ss
    ON se.song = ss.title AND se.artist = ss.artist_name AND se.length = ss.duration
    
    
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT user_id, first_name, last_name, gender, level
    FROM staging_events
    WHERE user_id IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name as name, artist_location as location, artist_latitude as latitude, artist_longitude as longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT ts, EXTRACT(hour FROM ts) as hour, EXTRACT(day FROM ts) as day, EXTRACT(week FROM ts) as week,
            EXTRACT(month FROM ts) as month, EXTRACT(year FROM ts) as year, EXTRACT(weekday FROM ts) as weekday
    FROM staging_events
    WHERE ts IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
