# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplays_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS \"user\""
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
artist_table_create = ("""
                    CREATE TABLE public.artist (
                        artistid varchar(256) NOT NULL sortkey,
                        name varchar(256),
                        location varchar(256),
                        lattitude numeric(18,0),
                        longitude numeric(18,0)
                    ) diststyle all;
""")
songplays_table_create = ("""
                        CREATE TABLE public.songplays (
                            playid varchar(32) NOT NULL,
                            start_time timestamp NOT NULL sortkey distkey,
                            userid int4 NOT NULL,
                            "level" varchar(256),
                            songid varchar(256),
                            artistid varchar(256),
                            sessionid int4,
                            location varchar(256),
                            user_agent varchar(256),
                            CONSTRAINT songplays_pkey PRIMARY KEY (playid)
                        );
""")


song_table_create = ("""
                    CREATE TABLE public.song (
                        songid varchar(256) NOT NULL sortkey,
                        title varchar(256),
                        artistid varchar(256),
                        "year" int4,
                        duration numeric(18,0),
                        CONSTRAINT song_pkey PRIMARY KEY (songid)
                    ) diststyle all;
""")

staging_events_table_create = ("""
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
""")

staging_songs_table_create = ("""
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
""")


user_table_create = ("""
                    CREATE TABLE public.user (
                        userid int4 NOT NULL sortkey,
                        first_name varchar(256),
                        last_name varchar(256),
                        gender varchar(256),
                        "level" varchar(256),
                        CONSTRAINT user_pkey PRIMARY KEY (userid)
                    ) diststyle all;
""")
time_table_create = ("""
                    create table public.time( 
                          start_time timestamp NOT NULL sortkey distkey,
                          hour int NOT NULL,
                          day int NOT NULL,
                          week int NOT NULL,
                          month int NOT NULL,
                          year int NOT NULL,
                          weekday int NOT NULL
                          );
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplays_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplays_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
