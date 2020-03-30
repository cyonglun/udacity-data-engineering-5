class SqlQueries:

    staging_events_table_create = ("""
        CREATE TABLE IF NOT EXISTS staging_events(
            artist varchar(256),
            auth varchar(256),
            firstname varchar(256),
            gender varchar(256),
            iteminsession INTEGER,
            lastname varchar(256),
            length numeric(18,0),
            "level" varchar(256),
            location varchar(256),
            "method" varchar(256),
            page varchar(256),
            registration numeric(18,0),
            sessionid INTEGER,
            song varchar(256),
            status INTEGER,
            ts int8,
            useragent varchar(256),
            userid INTEGER
        );
    """)

    staging_songs_table_create = ("""
        CREATE TABLE IF NOT EXISTS staging_songs(
            num_songs INTEGER,
            artist_id varchar(256),
            artist_name varchar(256),
            artist_latitude numeric(18,0),
            artist_longitude numeric(18,0),
            artist_location varchar(256),
            song_id varchar(256),
            title varchar(256),
            duration numeric(18,0),
            "year" INTEGER
        );
    """)

    songplay_table_create = ("""
        CREATE TABLE IF NOT EXISTS songplays (
            playid varchar(32) NOT NULL,
            start_time timestamp NOT NULL,
            userid INTEGER NOT NULL,
            "level" varchar(256),
            songid varchar(256),
            artistid varchar(256),
            sessionid INTEGER,
            location varchar(256),
            user_agent varchar(256),
            CONSTRAINT songplays_pkey PRIMARY KEY (playid)
        );
    """)
    
    songplay_table_insert = ("""
        INSERT INTO songplays (
            playid,
            start_time,
            userid, 
            "level",
            songid,
            artistid,
            sessionid,
            location,
            user_agent
        ) 
        SELECT
            md5(events.sessionid || events.start_time) songplay_id,
            events.start_time, 
            events.userid, 
            events.level, 
            songs.song_id, 
            songs.artist_id, 
            events.sessionid, 
            events.location, 
            events.useragent
            FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
        FROM staging_events
        WHERE page='NextSong') events
        LEFT JOIN staging_songs songs
        ON events.song = songs.title
            AND events.artist = songs.artist_name
            AND events.length = songs.duration
    """)

    user_table_create = ("""
        CREATE TABLE IF NOT EXISTS users (
            userid INTEGER NOT NULL,
            first_name varchar(256),
            last_name varchar(256),
            gender varchar(256),
            "level" varchar(256),
            CONSTRAINT users_pkey PRIMARY KEY (userid)
        );
    """)
    
    user_table_insert = ("""
        INSERT INTO users (
            userid,
            first_name,
            last_name,
            gender,
            "level"
        )
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_create = ("""
        CREATE TABLE IF NOT EXISTS songs (
            songid varchar(256) NOT NULL,
            title varchar(256),
            artistid varchar(256),
            "year" INTEGER,
            duration numeric(18,0),
            CONSTRAINT songs_pkey PRIMARY KEY (songid)
        );
    """)
    
    song_table_insert = ("""
        INSERT INTO songs (
            songid,
            title,
            artistid,
            "year",
            duration
        )
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_create = ("""
        CREATE TABLE IF NOT EXISTS artists (
            artistid varchar(256) NOT NULL,
            name varchar(256),
            location varchar(256),
            lattitude numeric(18,0),
            longitude numeric(18,0)
        );
    """)
    
    artist_table_insert = ("""
        INSERT INTO artists (
            artistid,
            name, 
            location, 
            lattitude, 
            longitude
        )
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_create = ("""
        CREATE TABLE IF NOT EXISTS time(
            start_time TIMESTAMP NOT NULL, 
            hour INTEGER, 
            day INTEGER, 
            week INTEGER, 
            month varchar(256),
            year INTEGER, 
            weekday varchar(256),
            CONSTRAINT time_pkey PRIMARY KEY (start_time)
        );
    """)
    
    time_table_insert = ("""
        INSERT INTO "time" (
            start_time,
            hour,
            day,
            week,
            month,
            year,
            weekday
        )
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)