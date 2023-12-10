# -*- coding: utf-8 -*-
import os
import sys 
# Add the parent directory to the Python path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)
from common.utils import (logger_setup, delete_old_log_files, create_segments_table,
                          DatabaseConnection, exit_script, map_log_level,
                          read_testing_date, config_reader, RedisConnection)
import datetime
import time


# Reading the configuration file
config = config_reader(config_dir=parent_dir, config_file='config.ini')

# DATABASE Connection parameters
host = config.get('DATABASE', 'HOST')
port = config.get('DATABASE', 'PORT')
database = config.get('DATABASE', 'DATABASE')
user = config.get('DATABASE', 'USER')
password = config.get('DATABASE', 'PASSWORD')

# LOGGING config.
MAX_LOGS_DAYS = config.getint('LOGGING', 'MAX_LOGS_DAYS')
console_logging_level = map_log_level(config.get('LOGGING', 'CONSOLE_LEVEL'))
file_logging_level = map_log_level(config.get('LOGGING', 'FILE_LEVEL'))

# frequency for updating table in seconds.,
update_tbl_freq = config.getint('BUNCHING', 'UPDATE_FREQUENCY_SEC')
# max time threshold in minutes
max_time_threshold = config.getfloat('BUNCHING', 'MAX_TIME_THRESHOLD_MINS')

# parameters for skipping trip IDs that are deviated.
ADD_DEVIATION_FILTER = config.getboolean('BUNCHING', 'ADD_DEVIATION_FILTER')
# distance in meters from deviation,
DEVIATION_DISTANCE_LIMIT = config.getfloat('DEVIATION', 'DISTANCE_THRESHOLD')

# flag for overwriting bunching_monitor and bunching_sample table.
OVERWRITE_TABLES = config.getboolean('BUNCHING', 'OVERWRITE_TABLES')
# delete previous day's tables and saving data in historical table.
DELETE_PREVIOUS_TABLES = config.getboolean('BUNCHING', 'DELETE_PREVIOUS_TABLES')
DATA_EXPIRY_DAYS = config.getint('BUNCHING', 'SAMPLES_DATA_EXPIRY_DAYS')
# script closing time,
SCRIPT_EXIT_TIME = config.get('BUNCHING', 'SCRIPT_EXIT_TIME')
SCRIPT_EXIT_TIME = datetime.datetime.strptime(SCRIPT_EXIT_TIME, '%H:%M').time()

# script testing paramters.
TEST_DATE = read_testing_date(config=config)

# column used for differentiating locations.
# i.e., geom or distancefromstop
location_unique_column = 'geom'

# required tables to create trip_performance_table.
required_tables_dict_ymd = {"trip_perform_monitor_table": "public.trip_perform_monitor_yyyymmdd",
                        "gtfs_stops_time_table": "gtfs_data.gtfs_stop_times_yyyymmdd",
                        "gtfs_stops_table": "gtfs_data.gtfs_stops_yyyymmdd",
                        "siri_sm_res_monitor_28": "public.siri_sm_res_monitor_jerusalem",
                        "segments_table": "public.segments_yyyymmdd",
                        "trip_bunching_table": "public.bunching_samples_yyyymmdd",
                        "trip_bunching_monitor_table": "public.trip_bunching_monitor_yyyymmdd",
                        "trip_all_events_table": "public.trip_all_events",
                        "all_trip_bunching_table": "public.all_bunching_samples"}

create_trigger_statements_temp = """CREATE OR REPLACE FUNCTION insert_bunching_events()
                                    RETURNS TRIGGER AS $$
                                    BEGIN
                                        INSERT INTO public.trip_all_events(event_type, event_index, event_name, sample_time, 
                                                route_short_name, route_long_name, route_id, bunching_meta, geom)
                                        SELECT
                                            5, ds.bunching_index, 'Bunching', ds.median_time,
                                            ds.route_short_name, ds.route_long_name,ds.lineref,
                                            ds.bunching_meta, ds.bunching_station_geom
                                        FROM (
                                            SELECT bunching_index,median_time,lineref,
                                                    route_short_name, route_long_name,
                                                    jsonb_build_object('siri_trip_id', jsonb_agg(siri_trip_id),
                                                    'vehicle_number', jsonb_agg(vehicleref),
                                                    'origin_aimed_departure_time',jsonb_agg(originaimeddeparturetime))
                                                    as bunching_meta, bunching_station_geom
                                            FROM bunching_events
                                            -- where bunching_events.is_latest = True
                                            group by bunching_index,median_time,lineref,
                                            route_short_name, route_long_name, bunching_station_geom
                                        ) ds;

                                        RETURN NULL;
                                    END;
                                    $$ LANGUAGE plpgsql;

                                    DROP TRIGGER IF EXISTS insert_bunching_events_trigger ON {bunching_tble};
                                    CREATE  TRIGGER  insert_bunching_events_trigger
                                    AFTER INSERT ON {bunching_tble}
                                    REFERENCING NEW TABLE AS bunching_events
                                    FOR EACH STATEMENT
                                    EXECUTE FUNCTION insert_bunching_events();"""

drop_table_query_temp = """DROP TABLE IF EXISTS {table} CASCADE;"""

trip_bunching_monitor_table_qry_tmp = """
                                        CREATE TABLE  IF NOT EXISTS {trip_buching_mntr_tbl} (
                                        id serial PRIMARY KEY,
                                        status boolean DEFAULT false,
                                        event_index integer DEFAULT 0,
                                        bunching_meta JSON, 
                                        last_updated timestamp without time zone DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'Israel'),
                                        trip_master_id bigint UNIQUE REFERENCES {trip_peform_tbl}(ID)
                                        );
                                        CREATE INDEX IF NOT EXISTS idx_bunching_trip_master_id_{date_str}   ON  {trip_buching_mntr_tbl} (trip_master_id);
                                        ----insert IDs from performance table,
                                        insert into {trip_buching_mntr_tbl}(trip_master_id)
                                        select id from {trip_peform_tbl}
                                        ON CONFLICT (trip_master_id) DO NOTHING;;
                                        """

siri_select_latest_idx1_tmp = """   SELECT * from
                                    (SELECT DISTINCT ON ({loc_unique_col}) 1 as uq_idx,
                                    1 as idx,{siri_columns}
                                    FROM {siri_table}
                                    WHERE
                                    datedvehiclejourneyref<>'0'
                                    AND datedvehiclejourneyref = trip_monitor.siri_trip_id
                                    AND lineref = trip_monitor.route_id
                                    AND originaimeddeparturetime= trip_monitor.departure_time
                                    AND recordedattime>=CURRENT_TIMESTAMP AT TIME ZONE 'Israel' - INTERVAL '{minutes} minutes'
                                    ) SAMPLES1
                                    ORDER BY SAMPLES1.recordedattime DESC
                                    LIMIT {sample_limit}"""

siri_select_latest_idx2_tmp = """   SELECT * from
                                    (SELECT  DISTINCT ON ({loc_unique_col}) 2 as uq_idx,
                                    2 as idx,{siri_columns}
                                    FROM {siri_table}
                                    WHERE
                                    datedvehiclejourneyref='0'
                                    AND lineref = trip_monitor.route_id
                                    AND originaimeddeparturetime= trip_monitor.departure_time
                                    AND recordedattime>=CURRENT_TIMESTAMP AT TIME ZONE 'Israel' - INTERVAL '{minutes} minutes') SAMPLES2
                                    ORDER BY SAMPLES2.recordedattime DESC
                                    LIMIT {sample_limit}"""

siri_select_latest_query_tmp = """SELECT {siri_columns}
                                  FROM
                                  (
                                    ({siri_select_latest_idx1_qry})
                                    UNION
                                    ({siri_select_latest_idx2_qry})
                                    ) joined_data
                                    order by idx
                                    LIMIT {sample_limit}"""

# Jerusalem, Israel, the UTM Zone 36N, SRID 32636
trip_monitor_select_query_tmp = """SELECT trip_monitor.id as trip_perform_id,
                                    trip_monitor.the_geom as route_geom_84,trip_monitor.geom_len,
                                    trip_monitor.st_seq,trip_monitor.end_seq,
                                    trip_monitor.route_short_name, trip_monitor.route_long_name, trip_monitor.siri_trip_id, 
                                    {siri_table_columns}
                                    FROM 
                                    (select perf_mon.id,perf_mon.st_seq,perf_mon.end_seq,
                                    perf_mon.the_geom,ST_Length(geom_36N) as geom_len,
                                    perf_mon.route_id,perf_mon.siri_trip_id,
                                    perf_mon.origin_aimed_departure_time AS departure_time, perf_mon.vehicle_number,perf_mon.route_short_name,
                                    perf_mon.route_long_name
                                    from {trip_perform_tbl} perf_mon
                                    inner join (
                                        select route_id
                                        from {trip_perform_tbl}
                                        group by route_id
                                        having count(*)>1
                                        ) mult_siri_trip
                                     on perf_mon.route_id=mult_siri_trip.route_id
                                     --- active trip filter.
                                    and perf_mon.activated = True and perf_mon.de_activated = False
                                    ) trip_monitor
                                    inner JOIN LATERAL (
                                    {siri_select_query}
                                    ) siri_selection on true
                                    """

speed_samples_query_tmp = """   select
                                    bunching_samples.unique_id,
                                    bunching_samples.siri_trip_id,
                                    bunching_samples.route_geom_84,
                                    bunching_samples.geom_len,
                                    bunching_samples.trip_perform_id,
                                    bunching_samples.datedvehiclejourneyref,
                                    bunching_samples."order",
                                    bunching_samples.lineref,
                                    mapped_samples.stop_sequence,
                                    mapped_samples.next_seq,
                                    mapped_samples.st_dist_m,
                                    mapped_samples.end_dist_m,
                                    bunching_samples.recordedattime,
                                    LAG(bunching_samples.recordedattime, 1) OVER (PARTITION BY bunching_samples.trip_perform_id ORDER BY bunching_samples.recordedattime) as prev_recordedattime,
                                    
                                    mapped_samples.st_dist_m+ST_LineLocatePoint(mapped_samples.geom_36n ,ST_Transform(bunching_samples.geom, 32636)) * mapped_samples.shape_len
                                    as dis_travelled,
                                    
                                    LAG(mapped_samples.st_dist_m, 1) OVER (PARTITION BY bunching_samples.trip_perform_id ORDER BY bunching_samples.recordedattime)
                                    +
                                    ST_LineLocatePoint(LAG(mapped_samples.geom_36n, 1) OVER (PARTITION BY bunching_samples.trip_perform_id ORDER BY bunching_samples.recordedattime),
                                    ST_Transform(LAG(bunching_samples.geom, 1) OVER (PARTITION BY bunching_samples.trip_perform_id ORDER BY bunching_samples.recordedattime), 32636))
                                    * LAG(mapped_samples.shape_len, 1) OVER (PARTITION BY bunching_samples.trip_perform_id ORDER BY bunching_samples.recordedattime)
                                    as prev_dist_travelled,
                                    mapped_samples.deviation_size,
                                    LAG(mapped_samples.deviation_size, 1) 
                                    OVER (PARTITION BY bunching_samples.trip_perform_id ORDER BY bunching_samples.recordedattime)  as previous_deviation_size
                
                                    from bunching_samples
                                    ----- Mapping each siri sample on its respective segment,
                                    inner JOIN LATERAL
                                    (
                                    select stop_sequence, next_seq, st_dist_m,  end_dist_m, st_stop_code, end_stop_code, shape_len, geom_36n,
                                    ROUND(CAST(ST_Distance(ST_Transform(bunching_samples.geom,32636),geom_36n) as numeric),2) AS deviation_size
                                    from {segments_table}
                                    where
                                    stop_sequence>=bunching_samples.order-2 and
                                    stop_sequence<=bunching_samples.order+2
                                    and route_id=bunching_samples.lineref
                                    order by  geom_36n <-> ST_Transform(bunching_samples.geom,32636)
                                    limit 1
                                    ) mapped_samples on true                          
                            """

with_speed_samples_query_tmp = """  select s_s.stop_sequence, s_s.siri_trip_id,s_s.trip_perform_id,
                                            s_s.route_geom_84,s_s.geom_len,
                                            s_s."order", s_s.unique_id, s_s.prev_recordedattime, s_s.recordedattime,
                                            s_s.datedvehiclejourneyref,s_s.lineref,
                                            (s_s.dis_travelled-s_s.prev_dist_travelled)/ (CASE WHEN
                                            EXTRACT(EPOCH FROM (s_s.recordedattime-s_s.prev_recordedattime))=0 THEN 1
                                            ELSE EXTRACT(EPOCH FROM (s_s.recordedattime-s_s.prev_recordedattime)) END) as speed,
                                            
                                            -- for speed testing distance and time diff, (REMOVE LATER!!!)
                                            -- dis_travelled-prev_dist_travelled as dist_diff,
                                            -- EXTRACT(EPOCH FROM (recordedattime-prev_recordedattime )) as ts,
                                            -- recordedattime-prev_recordedattime  as test,
                                            
                                            --- median time difference and distance to snap using speed and timed diff.
                                            med_t.median_t,
                                            EXTRACT(EPOCH FROM (med_t.median_t-s_s.recordedattime)) as median_t_diff,
                                            
                                            ((s_s.dis_travelled-s_s.prev_dist_travelled)/ (CASE WHEN EXTRACT(EPOCH FROM (s_s.recordedattime-s_s.prev_recordedattime))=0 THEN 1
                                            ELSE EXTRACT(EPOCH FROM (s_s.recordedattime-s_s.prev_recordedattime)) END))
                                            *EXTRACT(EPOCH FROM (med_t.median_t-s_s.recordedattime)) as  dist_to_snap,
                                            
                                            
                                            s_s.dis_travelled,
                                            s_s.dis_travelled+
                                            (((s_s.dis_travelled-s_s.prev_dist_travelled)/ (CASE WHEN
                                            EXTRACT(EPOCH FROM (s_s.recordedattime-s_s.prev_recordedattime))=0 THEN 1
                                            ELSE EXTRACT(EPOCH FROM (s_s.recordedattime-s_s.prev_recordedattime)) END))*
                                            EXTRACT(EPOCH FROM (med_t.median_t-s_s.recordedattime)))
                                            as total_snapped_dist,
                                            s_s.deviation_size
                                    from speed_samples s_s
                                    inner join median_time_qry med_t
                                    on med_t.lineref=s_s.lineref 
                                    where s_s.prev_recordedattime is not null  
                                    {deviation_filter}"""

snapped_samples_query_temp = """select 
                                    sp_samples.unique_id,
                                    ST_LineInterpolatePoint(sp_samples.route_geom_84,(sp_samples.total_snapped_dist/sp_samples.geom_len)) as snapped_geom,
                                    sp_samples.siri_trip_id,
                                    sp_samples.trip_perform_id,
                                    sp_samples.lineref,
                                    sp_samples.speed as bunching_speed ,
                                    sp_samples.median_t as median_time,
                                    sp_samples.dis_travelled as current_distance ,
                                    sp_samples.dist_to_snap as snap_distance ,
                                    sp_samples.total_snapped_dist as distance_after_snap ,
                                    snap_mapped.end_dist_m-sp_samples.total_snapped_dist as distance_to_next_station,
                                    CASE
                                    WHEN sp_samples.speed = 0 THEN NULL -- or another appropriate value
                                    ELSE (snap_mapped.end_dist_m - sp_samples.total_snapped_dist) / sp_samples.speed
                                    END AS time_to_station,
                                    sp_samples.stop_sequence as current_stop_sequence,
                                    snap_mapped.stop_sequence as next_stop_sequence,
                                    snap_mapped.st_stop_code as prev_station, 
                                    snap_mapped.end_stop_code as bunching_station,
                                    snap_mapped.bunching_station_geom,
                                    sp_samples.deviation_size 
                                    from  with_speed_samples sp_samples
                                    inner JOIN LATERAL
                                    (
                                    select stop_sequence, next_seq, st_dist_m,  end_dist_m, 
                                    st_stop_code, end_stop_code, shape_len, geom_36n,
                                    snapped_geom as bunching_station_geom
                                    from {segments_table}
                                    where
                                    route_id=sp_samples.lineref and
                                    sp_samples.total_snapped_dist BETWEEN st_dist_m and end_dist_m
                                    -- skip first and last segment.
                                    AND is_extreme=False
                                    ) snap_mapped on true"""

latest_event_index_query_temp = """ select trip_bun_mtr.trip_master_id as trip_perform_id,
                                    CASE WHEN bunching_trips.bunching AND trip_bun_mtr.status=False 
                                    THEN trip_bun_mtr.event_index + 1 ELSE trip_bun_mtr.event_index END		
                                    as latest_event_index,
                                    CASE WHEN bunching_trips.bunching AND trip_bun_mtr.status=False 
                                    THEN True ELSE False END as is_latest_event
                                    from
                                    {trip_buching_mntr_tbl}   trip_bun_mtr
                                    inner join bunching_trips on  bunching_trips.trip_perform_id = trip_bun_mtr.trip_master_id"""

bunching_monitor_update_query_temp = """ update  {trip_buching_mntr_tbl}    trip_bun_mtr
                                        SET status = bunching_trips.bunching,
                                        event_index = CASE WHEN bunching_trips.bunching  and status=False THEN event_index + 1 ELSE event_index END,
                                        bunching_meta = bunching_trips.bunching_trip_perform_ids,
                                        last_updated = CURRENT_TIMESTAMP AT TIME ZONE 'Israel'
                                        FROM bunching_trips
                                        WHERE bunching_trips.trip_perform_id = trip_bun_mtr.trip_master_id"""

trip_bunching_main_query_tmp = """ with bunching_samples as (
                                            {trip_monitor_qry}
                                        ),
 
                                    ----- samples for calculating speed
                                    speed_samples as (
                                     {speed_samples_query}
                                    ),
                                    
                                     -------- selecting latest samples per siri_trip_id
                                    distinct_samples as
                                    (select s_s.siri_trip_id,s_s.recordedattime,s_s.lineref, s_s.unique_id
                                    from speed_samples s_s
                                    where s_s.prev_recordedattime is not null  
                                    {deviation_filter}),

                                     -------- finding median time using latest samples per siri_trip_id with the same lineref
                                    median_time_qry as
                                    (select lineref,
                                    percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM recordedattime)) as median_ts,
                                    to_timestamp(percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM recordedattime)))  AT TIME ZONE 'UTC'  AS median_t
                                    from distinct_samples
                                    group by lineref),
                                    
                                    with_speed_samples as (
                                    {with_speed_samples_query}
                                    ),
                                    
                                    snapped_samples as (
                                    {snapped_samples_query}
                                    ),
                                    
                                    max_bunching_index as (
                                    select max(bunching_index) as max_idx
                                    from {bunching_samples_tbl}),

                                    bunching_trips as 
                                    (
                                    select  snapped_samples.*,bunched_linerefs.trip_count,
                                     CASE WHEN bunched_linerefs.trip_count is null THEN FALSE
                                     ELSE TRUE END AS bunching,
                                    bunched_linerefs.bunching_index,
                                    bunched_linerefs.bunching_trip_perform_ids
                                    from
                                     snapped_samples
                                    left join (
                                    select COALESCE((SELECT max_idx FROM max_bunching_index),0)+ 
                                    ROW_NUMBER() OVER () AS bunching_index,
                                    lineref,bunching_station,
                                    jsonb_build_object('trip_perform_ids', jsonb_agg(trip_perform_id)) as bunching_trip_perform_ids,
                                    count(*) as trip_count from snapped_samples
                                    group by lineref,bunching_station
                                    having count(*)>1) bunched_linerefs
                                    on bunched_linerefs.lineref=snapped_samples.lineref and 
                                    bunched_linerefs.bunching_station=snapped_samples.bunching_station
                                    ----- remove this filter.
                                    -- and bunched_linerefs.lineref=6601
                                    ),
                                                
                                    latest_event_indexes as
                                        ({latest_event_index_query}),

                                    update_query as 
                                    ({bunching_monitor_update_query}) 
                                    
                                     {bunching_samples_insert_qry}                              
                                """

bunching_samples_insert_qry_tmp = """ INSERT INTO {bunching_samples_tbl}({siri_bunch_columns}, "batch_id",
                                        "route_short_name", "route_long_name", "siri_trip_id","is_latest",
                                        "median_time","current_distance","snap_distance","distance_after_snap",
                                        "bunching_speed","bunching_station","time_to_station","distance_to_next_station",
                                        "trip_count","current_stop_sequence","next_stop_sequence","bunching_index","deviation_size", "snapped_geom",
                                        "bunching_station_geom")	
                                        SELECT {siri_bunch_aliased_columns} ,latest_event_indexes.latest_event_index,
                                        bunching_samples.route_short_name, bunching_samples.route_long_name, bunching_samples.siri_trip_id,
                                        latest_event_indexes.is_latest_event,
                                        bunching_trips.median_time,bunching_trips.current_distance,bunching_trips.snap_distance,bunching_trips.distance_after_snap,
                                        bunching_speed,bunching_trips.bunching_station,bunching_trips.time_to_station,bunching_trips.distance_to_next_station,
                                        bunching_trips.trip_count,bunching_trips.current_stop_sequence,bunching_trips.next_stop_sequence,
                                        bunching_trips.bunching_index, bunching_trips.deviation_size,
                                        bunching_trips.snapped_geom, bunching_trips.bunching_station_geom
                                        from bunching_samples 
                                        inner join bunching_trips on bunching_trips.trip_perform_id=bunching_samples.trip_perform_id 
                                        and bunching_trips.bunching=True and bunching_trips.unique_id=bunching_samples.unique_id  
                                        and bunching_trips.siri_trip_id=bunching_samples.siri_trip_id
                                        inner join latest_event_indexes on latest_event_indexes.trip_perform_id=bunching_trips.trip_perform_id
                                        -- Removing Constraint for avoiding bunching with single Trips.
                                        --  ON CONFLICT (unique_id, siri_trip_id, bunching_station, median_time) DO NOTHING
                                        ; """

extra_columns_query = """   "unique_id" integer,
                            "geom" geometry,
                             trip_perform_id bigint,
                             median_time timestamp without time zone,
                             current_distance NUMERIC(15, 2),
                             snap_distance NUMERIC(15, 2),
                             distance_after_snap NUMERIC(15, 2),
                             bunching_speed NUMERIC(15, 2),
                             bunching_station text,
                             bunching_station_geom geometry,
                             time_to_station  NUMERIC(15, 2),
                             distance_to_next_station NUMERIC(15, 2),
                             trip_count integer,
                             current_stop_sequence integer,
                             next_stop_sequence integer,
                             created_at timestamp without time zone DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'Israel'),
                             batch_id integer,
                             id serial PRIMARY KEY,
                             route_short_name text,
                             route_long_name text,
                             siri_trip_id integer,
                             bunching_index integer,
                             is_latest boolean,
                             deviation_size  NUMERIC(15, 2),
                             snapped_geom geometry"""

create_index_query_temp = """ CREATE INDEX IF NOT EXISTS idx_bunching_perf_id_{date_str}   ON  {buching_tbl} (trip_perform_id);
                              CREATE INDEX IF NOT EXISTS idx_bunching_batch_id_{date_str}  ON  {buching_tbl} (batch_id);
                              CREATE INDEX IF NOT EXISTS idx_bunching_index_{date_str}    ON  {buching_tbl} (bunching_index);
                              """

create_all_bunching_index_query_temp = """CREATE INDEX IF NOT EXISTS idx_all_bunching_perf_id ON  {all_buching_tbl} (trip_perform_id);
                                          CREATE INDEX IF NOT EXISTS idx_all_bunching_batch_id  ON  {all_buching_tbl} (batch_id);
                                          CREATE INDEX IF NOT EXISTS idx_all_bunching_index   ON  {all_buching_tbl} (bunching_index);
                                          CREATE INDEX IF NOT EXISTS idx_all_bunching_date   ON  {all_buching_tbl} (date);
                                          """

add_unique_index_query_temp = """ ALTER TABLE {buching_tbl}
                                  ADD CONSTRAINT bunch_unique_id_siri_id_{date_str} UNIQUE (unique_id, siri_trip_id, bunching_station, median_time);"""

# number of samples.
# don't change.
sample_limit = 2

if __name__ == "__main__":
    try:
        log_file_init = "log_trip_bunching_"
        logger = logger_setup(console_log_level=console_logging_level, file_log_level=file_logging_level,
                              file_init=log_file_init, date_format='%Y-%m-%d')
        # Delete previous log files older than max_days
        delete_old_log_files(MAX_LOGS_DAYS, logger, file_init=log_file_init, date_format='%Y-%m-%d')
        # redis connection,
        SCRIPT_REDIS_KEY = "trip_bunching"
        redis_connection = RedisConnection(logger=logger)
        # check database connection.
        database_connection = DatabaseConnection(host, port, database, user, password, logger)
        database_connection.test_connection()
        date_format = "%Y%m%d"
        current_date = datetime.datetime.now()

        if TEST_DATE:
            current_date = TEST_DATE


        dayinweek = current_date.isoweekday()
        # converting day number to make sunday as first day and saturday as last day,
        day_number = dayinweek + 1 if dayinweek < 7 else 1
        today_name = current_date.strftime("%A").lower()

        current_date_str = current_date.strftime(date_format)
        # updating table names
        required_tables_dict = {key: table.replace('yyyymmdd', current_date_str) for key, table in
                                required_tables_dict_ymd.items()}
        table_names = [table.replace('yyyymmdd', current_date_str) for table in required_tables_dict.values()]
        segments_table = required_tables_dict["segments_table"]
        trip_bunching_monitor_table = required_tables_dict["trip_bunching_monitor_table"]
        trip_bunching_table = required_tables_dict["trip_bunching_table"]

        # creating segments table.
        create_segments_table(trip_perform_tbl=required_tables_dict["trip_perform_monitor_table"],
                              gfts_times_tbl=required_tables_dict["gtfs_stops_time_table"],
                              gtfs_stops_tbl=required_tables_dict["gtfs_stops_table"],
                              segments_table=segments_table,
                              current_date_str=current_date_str,
                              database_connection=database_connection, logger=logger)

        logger.info(f"Creating {trip_bunching_table}, table.")
        create_table_script = database_connection.generate_create_table_script(
            existing_table=required_tables_dict["siri_sm_res_monitor_28"], table_to_create=trip_bunching_table,
            skip_columns=["unique_id","created_at"], append_query=extra_columns_query, drop=OVERWRITE_TABLES)
        database_connection.execute_query(create_table_script, commit=True)

        # logger.info(f"Adding unique_contraint to  {trip_bunching_table}, table.")
        # add_unique_index_query = add_unique_index_query_temp.format(buching_tbl=trip_bunching_table,
        #                                                             date_str=current_date_str)
        # database_connection.execute_query(add_unique_index_query, commit=True, raise_exception=False)

        logger.info(f"Creating indexes on :: {trip_bunching_table}'")
        create_index_query = create_index_query_temp.format(date_str=current_date_str,
                                                            buching_tbl=trip_bunching_table)
        database_connection.execute_query(create_index_query, commit=True)


        all_trip_bunching_table = required_tables_dict["all_trip_bunching_table"]
        logger.info(f"Creating {all_trip_bunching_table}, table.")
        date_column = ", date DATE"
        create_all_trip_bunching_table_script = database_connection.generate_create_table_script(
            existing_table=required_tables_dict["siri_sm_res_monitor_28"], table_to_create=all_trip_bunching_table,
            skip_columns=["unique_id", "created_at"], append_query=extra_columns_query+date_column , drop=False)
        database_connection.execute_query(create_all_trip_bunching_table_script, commit=True)

        logger.info(f"Creating indexes on :: {all_trip_bunching_table}'")
        create_all_bunching_index_query = create_all_bunching_index_query_temp.format(all_buching_tbl=all_trip_bunching_table)
        database_connection.execute_query(create_all_bunching_index_query, commit=True)

        # drop existing tables
        if OVERWRITE_TABLES:
            drop_bunching_table_query = drop_table_query_temp.format(table=trip_bunching_monitor_table)
            database_connection.execute_query(drop_bunching_table_query, commit=True)

        if DELETE_PREVIOUS_TABLES:
            logger.info(f"Dropping previous day's tables.")
            all_trip_bunching_table = required_tables_dict["all_trip_bunching_table"]
            previous_date = (current_date - datetime.timedelta(days=1)).date().strftime('%Y-%m-%d')
            tables_config = {"sample_table": required_tables_dict_ymd['trip_bunching_table'],
                             "all_samples_table": all_trip_bunching_table,
                             "extra_columns": [{"date": previous_date}], "columns2ignore": ["id"]}
            database_connection.delete_previous_days_tables(
                table_names=[required_tables_dict_ymd['trip_bunching_monitor_table'],
                             required_tables_dict_ymd['trip_bunching_table'],
                             required_tables_dict_ymd['segments_table']],
                current_date=current_date, tables_config=tables_config,
                data_expiry_days=DATA_EXPIRY_DAYS)

        logger.info(f"Creating trip_bunching_monitor_table table :: '{trip_bunching_monitor_table}'")
        create_trip_bunching_monitor_table_qry = trip_bunching_monitor_table_qry_tmp.format(
            trip_buching_mntr_tbl=trip_bunching_monitor_table,
            trip_peform_tbl=required_tables_dict["trip_perform_monitor_table"],
            date_str=current_date_str)
        database_connection.execute_query(create_trip_bunching_monitor_table_qry, commit=True)
        logger.info(f"Creating Triggers on table :: '{trip_bunching_monitor_table}'")
        create_trigger_statements = create_trigger_statements_temp.format(bunching_tble=trip_bunching_table)
        database_connection.execute_query(create_trigger_statements, commit=True)

        if database_connection.check_table_existance(table_names):
            logger.info("All required tables exist in database.")
            trip_perform_tbl = required_tables_dict["trip_perform_monitor_table"]
            siri_sample_table = required_tables_dict["siri_sm_res_monitor_28"].split(".")

            siri_columns = database_connection.get_table_columns(schema_name=siri_sample_table[0],
                                                                 table_name=siri_sample_table[1])
            siri_columns_str = ','.join(['"{0}"'.format(col[0]) for col in siri_columns])
            siri_table_columns_str = ','.join(['siri_selection."{0}"'.format(col[0]) for col in siri_columns])

            siri_select_latest_idx1_qry = siri_select_latest_idx1_tmp.format(siri_columns=siri_columns_str,
                                                                             siri_table=required_tables_dict[
                                                                                 "siri_sm_res_monitor_28"],
                                                                             sample_limit=int(sample_limit),
                                                                             loc_unique_col=location_unique_column,
                                                                             minutes=max_time_threshold)
            siri_select_latest_idx2_qry = siri_select_latest_idx2_tmp.format(siri_columns=siri_columns_str,
                                                                             siri_table=required_tables_dict[
                                                                                 "siri_sm_res_monitor_28"],
                                                                             sample_limit=int(sample_limit),
                                                                             loc_unique_col=location_unique_column,
                                                                             minutes=max_time_threshold)

            siri_select_latest_query = siri_select_latest_query_tmp.format(siri_columns=siri_columns_str,
                                                                           siri_select_latest_idx1_qry=siri_select_latest_idx1_qry,
                                                                           siri_select_latest_idx2_qry=siri_select_latest_idx2_qry,
                                                                           sample_limit=int(sample_limit))

            trip_monitor_select_query = trip_monitor_select_query_tmp.format(trip_perform_tbl=trip_perform_tbl,
                                                                             siri_table_columns=siri_table_columns_str,
                                                                             siri_select_query=siri_select_latest_query)

            logger.debug(f"SIRI Latest Records::\n '{trip_monitor_select_query}'")

            trip_bunching_schema, trip_bunching_table_table_name = trip_bunching_table.split(".")
            siri_deviation_columns = database_connection.get_table_columns(schema_name=trip_bunching_schema,
                                                                           table_name=trip_bunching_table_table_name)
            column_exceptions = ["created_at", "batch_id", "id", "route_short_name", "route_long_name", "siri_trip_id",
                                 "is_latest", "median_time", "current_distance", "snap_distance", "distance_after_snap",
                                 "bunching_speed", "bunching_station", "time_to_station", "distance_to_next_station",
                                 "trip_count", "current_stop_sequence", "next_stop_sequence", "bunching_index",
                                 "deviation_size", "snapped_geom", "bunching_station_geom"]
            siri_bunching_columns_str = ','.join(
                ['"{0}"'.format(col[0]) for col in siri_deviation_columns if col[0] not in column_exceptions])

            siri_deviation_columns_aliased_str = ','.join(
                ['bunching_samples."{0}"'.format(col[0]) for col in siri_deviation_columns if
                 col[0] not in column_exceptions])

            bunching_samples_insert_qry = bunching_samples_insert_qry_tmp.format(
                bunching_samples_tbl=trip_bunching_table,
                siri_bunch_columns=siri_bunching_columns_str,
                siri_bunch_aliased_columns=siri_deviation_columns_aliased_str)

            speed_samples_query = speed_samples_query_tmp.format(segments_table=segments_table)
            snapped_samples_query = snapped_samples_query_temp.format(segments_table=segments_table)
            latest_event_index_query = latest_event_index_query_temp.format(
                trip_buching_mntr_tbl=trip_bunching_monitor_table)
            bunching_monitor_update_query = bunching_monitor_update_query_temp.format(
                trip_buching_mntr_tbl=trip_bunching_monitor_table)

            if ADD_DEVIATION_FILTER:
                logger.info(f"Adding Deviation filter, DEVIATION_DISTANCE_LIMIT='{DEVIATION_DISTANCE_LIMIT}' meters.")
                deviation_filter_param = f"and s_s.deviation_size<={DEVIATION_DISTANCE_LIMIT} and s_s.previous_deviation_size<={DEVIATION_DISTANCE_LIMIT}"
            else:
                deviation_filter_param = ""

            with_speed_samples_query = with_speed_samples_query_tmp.format(deviation_filter=deviation_filter_param)
            trip_bunching_main_query = trip_bunching_main_query_tmp.format(trip_monitor_qry=trip_monitor_select_query,
                                                                           speed_samples_query=speed_samples_query,
                                                                           with_speed_samples_query=with_speed_samples_query,
                                                                           snapped_samples_query=snapped_samples_query,
                                                                           latest_event_index_query=latest_event_index_query,
                                                                           bunching_monitor_update_query=bunching_monitor_update_query,
                                                                           bunching_samples_insert_qry=bunching_samples_insert_qry,
                                                                           bunching_samples_tbl=trip_bunching_table,
                                                                           deviation_filter=deviation_filter_param)

            logger.info(f"Trip Bunching Main Query::\n '{trip_bunching_main_query}'")
            logger.info(f"***************************************************************")
            logger.info(f"Adding data into {trip_bunching_table}, day:{today_name}, day_no:{day_number}")
            logger.info(f"***************************************************************")
            while True:
                st_time = time.time()
                deviated_count = database_connection.execute_query(trip_bunching_main_query,
                                                                   commit=True,
                                                                   rows_affected=True,
                                                                   raise_exception=False)
                end_time = time.time()
                ex_time = end_time - st_time
                logger.info(
                    f"Updated {trip_bunching_table}({update_tbl_freq}sec), inserted_rows:{deviated_count}, query_execution_time:{round(ex_time, 2)} sec.")

                # setting redis key with timestamp.
                redis_connection.set_key(key=SCRIPT_REDIS_KEY, value=int(time.time()))

                sleep_time = update_tbl_freq - ex_time if (update_tbl_freq - ex_time) > 1 else 2
                sleep_time = int(sleep_time)
                logger.debug(f"sleeping for {sleep_time}sec...")
                time.sleep(sleep_time)
                # stopping script next day at a particular time i.e, 3:30AM.
                exit_script(logger=logger, current_date=current_date, hour=SCRIPT_EXIT_TIME.hour,
                            minutes=SCRIPT_EXIT_TIME.minute, db_connection=database_connection)
        else:
            logger.error("Required tables are missing in database.")

    except Exception as e:
        # Log the error or perform any necessary actions
        logger.exception(f"Error occurred: {str(e)}")
        sys.exit(1)

sys.exit(0)
