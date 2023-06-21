class SqlSchema:
    """Class that contains the SQL code for the Redshift schema creation."""
    
    create = ("""
    
        DROP TABLE IF EXISTS public.staging_events;
        CREATE TABLE public.staging_events (
            artist varchar(256),
            auth varchar(256),
            firstname varchar(256),
            gender varchar(256),
            iteminsession int4,
            lastname varchar(256),
            length numeric(18,0),
            "level" varchar(256),
            location varchar(256),
            "method" varchar(256),
            page varchar(256),
            registration numeric(18,0),
            sessionid int4,
            song varchar(256),
            status int4,
            ts int8,
            useragent varchar(256),
            userid int4
        );
        
        DROP TABLE IF EXISTS public.staging_songs;
        CREATE TABLE public.staging_songs (
            num_songs int4,
            artist_id varchar(256),
            artist_name varchar(256),
            artist_latitude numeric(18,0),
            artist_longitude numeric(18,0),
            artist_location varchar(256),
            song_id varchar(256),
            title varchar(256),
            duration numeric(18,0),
            "year" int4
        );
        
        DROP TABLE IF EXISTS public.songplays;
        CREATE TABLE public.songplays (
            songplay_id int IDENTITY(0,1) PRIMARY KEY,
            start_time timestamp NOT NULL,
            user_id int4 NOT NULL,
            "level" varchar(256),
            song_id varchar(256),
            artist_id varchar(256),
            session_id int4,
            location varchar(256),
            user_agent varchar(256)
        );
        
        DROP TABLE IF EXISTS public.users;
        CREATE TABLE public.users (
            user_id int4 NOT NULL,
            first_name varchar(256),
            last_name varchar(256),
            gender varchar(256),
            "level" varchar(256),
            CONSTRAINT PK_users PRIMARY KEY (user_id)
        );
        
        DROP TABLE IF EXISTS public.songs;
        CREATE TABLE public.songs (
            song_id varchar(256) NOT NULL,
            title varchar(256),
            artist_id varchar(256),
            "year" int4,
            duration numeric(18,0),
            CONSTRAINT PK_songs PRIMARY KEY (song_id)
        );

        DROP TABLE IF EXISTS public.artists;
        CREATE TABLE public.artists (
            artist_id varchar(256) NOT NULL,
            name varchar(256),
            location varchar(256),
            latitude numeric(18,0),
            longitude numeric(18,0),
            CONSTRAINT PK_artists PRIMARY KEY (artist_id)
        );

        DROP TABLE IF EXISTS public."time";
        CREATE TABLE public."time" (
            start_time timestamp NOT NULL,
            "hour" int4,
            "day" int4,
            week int4,
            "month" varchar(256),
            "year" int4,
            weekday varchar(256),
            CONSTRAINT PK_time PRIMARY KEY (start_time)
        );  
        
    """)      
