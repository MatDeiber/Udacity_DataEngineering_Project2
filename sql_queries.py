import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')


ARN = config.get('IAM_ROLE','ARN')
# DROP TABLES
# The following queries are used to drop the tables if they exist

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
# The following queries are used to create the tables if they do not exist

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
    artist        VARCHAR    NULL,
    auth          VARCHAR    NULL,
    firstName     VARCHAR    NULL,
    gender        VARCHAR    NULL,
    itemInSession INTEGER    NULL,
    lastName      VARCHAR    NULL,
    length        DECIMAL    NULL,
    level         VARCHAR    NULL,
    location      VARCHAR    NULL,
    method        VARCHAR    NULL,
    page          VARCHAR    NULL,
    registration  VARCHAR    NULL,
    sessionId     INTEGER    NULL,
    song          VARCHAR    NULL,
    status        INTEGER    NULL,
    ts            BIGINT    NULL,
    userAgent     VARCHAR    NULL,
    userId        INTEGER    NULL
        );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
    song_id             VARCHAR    NULL,
    num_songs           INTEGER    NULL,
    title               VARCHAR    NULL,
    artist_name         VARCHAR    NULL,
    artist_latitude        REAL    NULL,
    year               SMALLINT    NULL,
    duration            DECIMAL    NULL,
    artist_id           VARCHAR    NULL,
    artist_longitude       REAL    NULL,
    artist_location     VARCHAR    NULL
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
    songplay_id   INTEGER IDENTITY(0,1)  PRIMARY KEY NOT NULL SORTKEY,
    start_time     BIGINT    NOT NULL,
    user_id       INTEGER    NOT NULL DISTKEY,
    level         VARCHAR    NULL,
    song_id       VARCHAR    NOT NULL,
    artist_id     VARCHAR    NOT NULL,
    session_id    INTEGER    NOT NULL,
    location      VARCHAR    NULL,
    user_agent    VARCHAR    NULL
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
    user_id       INTEGER    PRIMARY KEY NOT NULL SORTKEY,
    first_name    VARCHAR    NULL,
    last_name     VARCHAR    NULL,
    gender        VARCHAR    NULL,
    level         VARCHAR    NULL
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
    song_id   VARCHAR    PRIMARY KEY NOT NULL    SORTKEY,
    title     VARCHAR    NOT NULL,
    artist_id VARCHAR    NOT NULL,
    year      INTEGER    NULL,
    duration  DECIMAL    NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR    PRIMARY KEY NOT NULL    SORTKEY,
    name      VARCHAR    NOT NULL,
    location  VARCHAR    NULL,
    latitude  DECIMAL    NULL,
    longitude DECIMAL    NULL
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
    start_time    TIMESTAMP    PRIMARY KEY NOT NULL    SORTKEY,
    hour            INTEGER    NULL,
    day             INTEGER    NULL,
    week            INTEGER    NULL,
    month           INTEGER    NULL,
    year            INTEGER    NULL,
    weekday         INTEGER    NULL
    );
""")

# STAGING TABLES
# The following queries are used to copy the data from S3 to the staging tables
staging_events_copy = ("""
    COPY staging_events FROM {} CREDENTIALS 'aws_iam_role={}' FORMAT AS JSON  {} compupdate off region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {} CREDENTIALS 'aws_iam_role={}' FORMAT AS JSON 'auto' compupdate off region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES
# The following queries are used to insert the data from the staging tables to the final tables
songplay_table_insert = ("""
    INSERT INTO songplays (songplay_id,
                           start_time,
                           user_id,
                           level,
                           song_id,
                           artist_id,
                           session_id,
                           location,
                           user_agent)
        SELECT  se.itemInSession  AS songplay_id,
                se.ts             AS start_time,
                se.userId         AS user_id,
                se.level          AS level,
                ss.song_id        AS song_id,
                ss.artist_id      AS artist_id,
                se.sessionId      AS session_id,
                se.location       AS location,
                se.userAgent      AS user_agent
        FROM staging_events AS se
        JOIN staging_songs AS ss
            ON (se.artist = ss.artist_name)
            AND (se.song = ss.title)
            AND (se.length = ss.duration)
        WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id,
                      first_name,
                      last_name,
                      gender,
                      level)
    SELECT  DISTINCT e.userId    AS user_id,
            e.firstName          AS first_name,
            e.lastName           AS last_name,
            e.gender             AS gender,
            e.level              AS level
    FROM staging_events AS e
    WHERE e.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs(song_id,
                      title,
                      artist_id,
                      year,
                      duration)
    SELECT  DISTINCT s.song_id    AS song_id,
            s.title               AS title,
            s.artist_id           AS artist_id,
            s.year                AS year,
            s.duration            AS duration
    FROM staging_songs AS s;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id,
                         name,
                         location,
                         latitude,
                         longitude)
    SELECT  DISTINCT s.artist_id       AS artist_id,
            s.artist_name              AS name,
            s.artist_location          AS location,
            s.artist_latitude          AS latitude,
            s.artist_longitude         AS longitude
    FROM staging_songs AS s;
""")

time_table_insert = ("""
    INSERT INTO time (start_time,
                      hour,
                      day,
                      week,
                      month,
                      year,
                      weekday)
        SELECT  TIMESTAMP 'epoch' + e.ts/1000 *INTERVAL '1 second' as start_time,
                EXTRACT(hour FROM start_time)    AS hour,
                EXTRACT(day FROM start_time)     AS day,
                EXTRACT(week FROM start_time)    AS week,
                EXTRACT(month FROM start_time)   AS month,
                EXTRACT(year FROM start_time)    AS year,
                EXTRACT(dow FROM start_time)    AS weekday
        FROM    staging_events  AS e
        WHERE e.page = 'NextSong';    
""")

# QUERY LISTS
# The following queries are used to create, drop, copy and insert the data from the staging tables to the final tables
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
