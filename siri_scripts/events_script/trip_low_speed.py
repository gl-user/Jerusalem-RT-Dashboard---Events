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

#low-speed config.
# frequency for updating table in seconds.,
update_tbl_freq = config.getint('LOW-SPEED', 'UPDATE_FREQUENCY_SEC')
# flag for overwriting low_speed_monitor and low_speed_sample table.
OVERWRITE_TABLES = config.getboolean('LOW-SPEED', 'OVERWRITE_TABLES')
# number of samples to consider for low-speed.
sample_limit = config.getint('LOW-SPEED', 'SAMPLE_LIMIT')
# speed limit for samples to be considered as low_speed samples.
low_speed_limit = config.getfloat('LOW-SPEED', 'LOW_SPEED_THRESHOLD_KM_H')

# parameters for skipping trip IDs that are deviated.
ADD_DEVIATION_FILTER = config.getboolean('LOW-SPEED', 'ADD_DEVIATION_FILTER')
# distance in meters.
DEVIATION_DISTANCE_LIMIT = config.getfloat('DEVIATION', 'DISTANCE_THRESHOLD')

# delete previous day's tables and saving data in historical table.
DELETE_PREVIOUS_TABLES = config.getboolean('LOW-SPEED', 'DELETE_PREVIOUS_TABLES')
DATA_EXPIRY_DAYS = config.getint('LOW-SPEED', 'SAMPLES_DATA_EXPIRY_DAYS')
# script closing time,
SCRIPT_EXIT_TIME = config.get('LOW-SPEED', 'SCRIPT_EXIT_TIME')
SCRIPT_EXIT_TIME = datetime.datetime.strptime(SCRIPT_EXIT_TIME, '%H:%M').time()

# script testing paramters.
TEST_DATE = read_testing_date(config=config)


# max time threshold in minutes
ADD_TIME_FILTER = False
max_time_threshold = 30
# if speed is not feasible, due to incorrect GPS points or segments, using aerial speed instead.
# speed limit in km/h
max_speed_limit = 200

# column used for differentiating locations. i.e., geom or distancefromstop
location_unique_column = 'geom'

# for matching only  by vehicle # don't use.
ENABLE_VEHICLE_QUERY = False

# required tables to create trip_performance_table.
required_tables_dict_ymd = {"trip_perform_monitor_table": "public.trip_perform_monitor_yyyymmdd",
                        "gtfs_stops_time_table": "gtfs_data.gtfs_stop_times_yyyymmdd",
                        "gtfs_stops_table": "gtfs_data.gtfs_stops_yyyymmdd",
                        "siri_sm_res_monitor_28": "public.siri_sm_res_monitor_jerusalem",
                        "trip_low_speed_sample_table": "public.low_speed_samples_yyyymmdd",
                        "trip_low_speed_monitor_table": "public.trip_low_speed_monitor_yyyymmdd",
                        "trip_all_events_table":"public.trip_all_events",
                        "segments_table": "public.segments_yyyymmdd",
                        "all_trip_low_speed_table": "public.all_low_speed_samples"}

extra_columns_query = """ "unique_id" integer,
                        "geom" geometry,
                         trip_perform_id bigint,
                         low_speed_recorded NUMERIC(15, 2),
                         aerial_speed NUMERIC(15, 2),
                         along_line_speed NUMERIC(15, 2),
                         created_at timestamp without time zone DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'Israel'),
                         batch_id integer,
                         id serial PRIMARY KEY,
                         route_short_name text,
                         route_long_name text,
                         siri_trip_id integer,
                         is_latest boolean,
                         current_stop_sequence integer,
                         deviation_size  NUMERIC(15, 2)"""

siri_select_latest_idx1_tmp = """   SELECT * from
                                    (SELECT DISTINCT ON ({loc_unique_col}) 1 as uq_idx,
                                    1 as idx,{siri_columns}
                                    FROM {siri_table}
                                    WHERE
                                    datedvehiclejourneyref<>'0'
                                    AND datedvehiclejourneyref = trip_monitor.siri_trip_id
                                    AND lineref = trip_monitor.route_id
                                    AND originaimeddeparturetime= trip_monitor.departure_time
                                    {time_filter}
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
                                    {time_filter}
                                    ) SAMPLES2
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

siri_select_latest_vehicle_query_temp = """ SELECT DISTINCT ON ({loc_unique_col}) 1 as uq_idx,
                                                {siri_columns}
                                                FROM {siri_table}
                                                WHERE
                                                vehicleref=trip_monitor.vehicle_number
                                                AND lineref = trip_monitor.route_id
                                                AND originaimeddeparturetime= trip_monitor.departure_time
                                                ORDER BY {loc_unique_col}, recordedattime DESC
                                                LIMIT {sample_limit}"""

# Jerusalem, Israel, the UTM Zone 36N, SRID 32636
trip_monitor_select_query_tmp = """SELECT trip_monitor.id as trip_perform_id,trip_monitor.st_seq,trip_monitor.end_seq,
                                    trip_monitor.route_short_name, trip_monitor.route_long_name, trip_monitor.siri_trip_id, 
                                    {siri_table_columns}
                                    FROM 
                                    (select id,st_seq,end_seq,geom_36N,route_id,siri_trip_id,origin_aimed_departure_time AS departure_time,
                                    vehicle_number,route_short_name,route_long_name
                                    from {trip_perform_tbl} where activated = True and de_activated = False) trip_monitor
                                    inner JOIN LATERAL (
                                    {siri_select_query}
                                    ) siri_selection on true
                                """

speed_samples_query_tmp = """   select
                                    siri_samples.unique_id,
                                    siri_samples.siri_trip_id,
                                    siri_samples.trip_perform_id,
                                    siri_samples.datedvehiclejourneyref,
                                    siri_samples."order",
                                    siri_samples.lineref,
                                    mapped_samples.stop_sequence,
                                    mapped_samples.next_seq,
                                    mapped_samples.st_dist_m,
                                    mapped_samples.end_dist_m,
                                    siri_samples.geom,
                                    siri_samples.recordedattime,
                                    LAG(siri_samples.recordedattime, 1) OVER (PARTITION BY siri_samples.trip_perform_id ORDER BY siri_samples.recordedattime) as prev_recordedattime,

                                    mapped_samples.st_dist_m+ST_LineLocatePoint(mapped_samples.geom_36n ,ST_Transform(siri_samples.geom, 32636)) * mapped_samples.shape_len
                                    as dis_travelled,

                                    LAG(mapped_samples.st_dist_m, 1) OVER (PARTITION BY siri_samples.trip_perform_id ORDER BY siri_samples.recordedattime)
                                    +
                                    ST_LineLocatePoint(LAG(mapped_samples.geom_36n, 1) OVER (PARTITION BY siri_samples.trip_perform_id ORDER BY siri_samples.recordedattime),
                                    ST_Transform(LAG(siri_samples.geom, 1) OVER (PARTITION BY siri_samples.trip_perform_id ORDER BY siri_samples.recordedattime), 32636))
                                    * LAG(mapped_samples.shape_len, 1) OVER (PARTITION BY siri_samples.trip_perform_id ORDER BY siri_samples.recordedattime)
                                    as prev_dist_travelled,
                                    
                                    mapped_samples.deviation_size,
                                    LAG(mapped_samples.deviation_size, 1) OVER (PARTITION BY siri_samples.trip_perform_id ORDER BY siri_samples.recordedattime)  as previous_deviation_size,
                                    
                                    LAG(siri_samples.geom, 1) OVER (PARTITION BY siri_samples.trip_perform_id ORDER BY siri_samples.recordedattime) as prev_geom

                                    from siri_samples
                                    ----- Mapping each siri sample on its respective segment,
                                    inner JOIN LATERAL
                                    (
                                    select stop_sequence, next_seq, st_dist_m,  end_dist_m, st_stop_code, end_stop_code, shape_len, geom_36n,
                                    ROUND(CAST(ST_Distance(ST_Transform(siri_samples.geom,32636),geom_36n) as numeric),2) AS deviation_size
                                    from {segments_table}
                                    where
                                    stop_sequence>=siri_samples.order-2 and
                                    stop_sequence<=siri_samples.order+2
                                    and route_id=siri_samples.lineref
                                    order by  geom_36n <-> ST_Transform(siri_samples.geom,32636)
                                    limit 1
                                    ) mapped_samples on true                          
                            """

trip_low_speed_main_query_tmp = """ with siri_samples as  (
                                    {trip_monitor_qry}
                                ),
                                
                                speed_samples as (
                                {speed_samples_query}
                                ),
                                
                                with_speed_samples as (
                                    select s_s.trip_perform_id, s_s.unique_id,
                                    s_s.deviation_size, s_s.stop_sequence current_stop_sequence,
                                    
                                    ((s_s.dis_travelled-s_s.prev_dist_travelled)*3.6)/ (CASE WHEN
                                    EXTRACT(EPOCH FROM (s_s.recordedattime-s_s.prev_recordedattime))=0 THEN 1
                                    ELSE EXTRACT(EPOCH FROM (s_s.recordedattime-s_s.prev_recordedattime)) END) as speed,
                                    
                                    (ST_Distance(ST_Transform(prev_geom, 32636),ST_Transform(geom, 32636))*3.6) /
                                    (CASE WHEN
                                    EXTRACT(EPOCH FROM (s_s.recordedattime-s_s.prev_recordedattime))=0 THEN 1
                                    ELSE EXTRACT(EPOCH FROM (s_s.recordedattime-s_s.prev_recordedattime)) END) as aerial_speed
                                    
                                    -- for speed testing distance and time diff, (REMOVE LATER!!!)
                                    -- dis_travelled-prev_dist_travelled as dist_diff,
                                    -- EXTRACT(EPOCH FROM (recordedattime-prev_recordedattime )) as ts,
                                    -- recordedattime-prev_recordedattime  as test,
                                    from speed_samples s_s
                                    where s_s.prev_recordedattime is not null  
                                    {deviation_filter}
                                    ), 
                                    
                                trip_speed_status as (
                                    select trip_perform_id, unique_id,
                                    deviation_size, current_stop_sequence,
                                    CASE
                                    WHEN speed <= 0.0 OR speed > {max_speed_limit} THEN aerial_speed
                                    ELSE speed
                                    END AS final_speed,
                                    speed,
                                    aerial_speed
                                    from
                                    with_speed_samples),
                                      
                                    low_speed_trips  as (
                                    select trip_perform_id,
                                    CASE
                                    WHEN COUNT(*) FILTER (WHERE final_speed<{low_speed_limit}) =  {pair_limit} 
                                    THEN TRUE
                                    ELSE FALSE END AS low_speed
                                    from trip_speed_status
                                    group by trip_perform_id
                                    ),
                                    
                                    latest_event_index_query as
                                        (
                                        select low_speed_trips.trip_perform_id,
                                        CASE WHEN low_speed_trips.low_speed and trip_low_speed_mtr.status=False 
                                        THEN trip_low_speed_mtr.event_index + 1 
                                        ELSE trip_low_speed_mtr.event_index END
                                        as latest_event_index,
                                        CASE WHEN low_speed_trips.low_speed and trip_low_speed_mtr.status=False 
                                        THEN True ELSE False END as is_latest_event
                                        from
                                        {low_speed_mtr_tbl} trip_low_speed_mtr
                                        inner join  low_speed_trips on  
                                        trip_low_speed_mtr.trip_master_id = low_speed_trips.trip_perform_id
                                        ),
                                        
                                        update_query as
                                        (
                                        update  {low_speed_mtr_tbl} trip_low_speed_mtr
                                        SET status = low_speed_trips.low_speed,
                                        event_index = CASE WHEN low_speed_trips.low_speed and status=False THEN event_index + 1 ELSE event_index END,
                                        last_updated = CURRENT_TIMESTAMP AT TIME ZONE 'Israel'
                                        FROM low_speed_trips
                                        WHERE trip_low_speed_mtr.trip_master_id = low_speed_trips.trip_perform_id
                                        )         
                                 {low_speed_samples_insert_qry}    
                                """

table_columns_query_temp = """SELECT column_name  FROM information_schema.columns WHERE 
                             table_name = '{table_name}' and table_schema='{schema}';"""

low_speed_samples_insert_qry_tmp = """ INSERT INTO {low_speed_samples_tbl}({siri_low_sp_columns}, 
                                        "batch_id", "route_short_name", "route_long_name", "siri_trip_id",
                                        "low_speed_recorded","along_line_speed","aerial_speed","is_latest",
                                        "deviation_size", "current_stop_sequence")
                                        SELECT {siri_low_sp_aliased_columns} ,lt_idx.latest_event_index,
                                        trip_samp.route_short_name, trip_samp.route_long_name, trip_samp.siri_trip_id,
                                        tp_stats.final_speed,tp_stats.speed,tp_stats.aerial_speed,  lt_idx.is_latest_event,
                                        tp_stats.deviation_size, tp_stats.current_stop_sequence
                                        from 
                                        siri_samples trip_samp
                                        inner join low_speed_trips low_sp_tp
                                        on trip_samp.trip_perform_id=low_sp_tp.trip_perform_id and  low_sp_tp.low_speed=True
                                        inner join trip_speed_status tp_stats
                                        on tp_stats.trip_perform_id=low_sp_tp.trip_perform_id
                                        and tp_stats.unique_id=trip_samp.unique_id
                                        inner join latest_event_index_query lt_idx
                                        on lt_idx.trip_perform_id=trip_samp.trip_perform_id
                                        ON CONFLICT (unique_id, siri_trip_id) DO NOTHING; """


create_index_query_temp = """ CREATE INDEX IF NOT EXISTS idx_low_speed_perf_id_{date_str}     ON  {low_speed_tbl} (trip_perform_id);
                              CREATE INDEX IF NOT EXISTS idx_low_speed_batch_id_{date_str}  ON  {low_speed_tbl} (batch_id);
                              """
create_all_lsp_index_query_temp = """ CREATE INDEX IF NOT EXISTS idx_all_lsp_perf_id   ON  {all_low_speed_tbl} (trip_perform_id);
                                     CREATE INDEX IF NOT EXISTS idx_all_lsp_batch_id   ON  {all_low_speed_tbl} (batch_id);
                                     CREATE INDEX IF NOT EXISTS idx_all_lsp_date   ON  {all_low_speed_tbl} (date);
                                  """

add_unique_index_query_temp = """ALTER TABLE {low_speed_tbl}
                                 ADD CONSTRAINT lsp_unique_id_siri_id_{date_str} UNIQUE (unique_id, siri_trip_id);"""

create_trigger_statements_temp = """CREATE OR REPLACE FUNCTION insert_low_speed_events()
                                    RETURNS TRIGGER AS $$
                                    BEGIN
                                        INSERT INTO public.trip_all_events(event_type, event_index, event_name, sample_time, 
                                        trip_perform_id,siri_trip_id, vehicle_number, route_short_name, route_long_name,
                                         origin_aimed_departure_time,route_id, geom)
                                        SELECT
                                            4, ds.batch_id, 'Low-speed',  ds.recordedattime, ds.trip_perform_id,
                                            ds.siri_trip_id, ds.vehicleref, ds.route_short_name, ds.route_long_name,
                                                ds.originaimeddeparturetime, ds.lineref, ds.geom
                                        FROM (
                                            SELECT DISTINCT ON (trip_perform_id)
                                                trip_perform_id, batch_id, recordedattime,lineref,
                                                siri_trip_id, vehicleref,
                                                route_short_name, route_long_name,
                                                originaimeddeparturetime, geom
                                            FROM low_speed_events
                                            where low_speed_events.is_latest = True
                                            order by trip_perform_id,recordedattime
                                        ) ds;

                                        RETURN NULL;
                                    END;
                                    $$ LANGUAGE plpgsql;
                                    
                                    DROP TRIGGER IF EXISTS insert_low_speed_events_trigger ON {low_speed_tbl};
                                    CREATE  TRIGGER  insert_low_speed_events_trigger
                                    AFTER INSERT ON {low_speed_tbl}
                                    REFERENCING NEW TABLE AS low_speed_events
                                    FOR EACH STATEMENT
                                    EXECUTE FUNCTION insert_low_speed_events();"""

drop_table_query_temp = """DROP TABLE IF EXISTS {table} CASCADE;"""

trip_low_speed_monitor_table_qry_tmp = """
                                        CREATE TABLE  IF NOT EXISTS  {trip_low_speed_mntr_tbl} (
                                        id serial PRIMARY KEY,
                                        status boolean DEFAULT false,
                                        event_index integer DEFAULT 0,
                                        last_updated timestamp without time zone DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'Israel'),
                                        trip_master_id bigint UNIQUE REFERENCES {trip_peform_tbl}(ID)
                                        );
                                        CREATE INDEX IF NOT EXISTS idx_low_speed_trip_master_id_{date_str}   ON  {trip_low_speed_mntr_tbl} (trip_master_id);
                                        ----insert IDs from performance table,
                                        insert into {trip_low_speed_mntr_tbl}(trip_master_id)
                                        select id from {trip_peform_tbl}
                                        ON CONFLICT (trip_master_id) DO NOTHING;    
                                        """

if __name__ == "__main__":
    try:
        log_file_init = "log_trip_low_speed_"
        logger = logger_setup(console_log_level=console_logging_level, file_log_level=file_logging_level,
                              file_init=log_file_init, date_format='%Y-%m-%d')
        # Delete previous log files older than max_days
        delete_old_log_files(MAX_LOGS_DAYS, logger, file_init=log_file_init, date_format='%Y-%m-%d')
        # redis connection,
        SCRIPT_REDIS_KEY = "trip_low_speed"
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
        trip_low_speed_table = required_tables_dict["trip_low_speed_sample_table"]

        logger.info(f"Creating {trip_low_speed_table}, table.")
        create_table_script = database_connection.generate_create_table_script(
            existing_table=required_tables_dict["siri_sm_res_monitor_28"], table_to_create=trip_low_speed_table,
            skip_columns=["unique_id","created_at"], append_query=extra_columns_query, drop=OVERWRITE_TABLES)
        database_connection.execute_query(create_table_script, commit=True)

        logger.info(f"Adding unique_contraint to  {trip_low_speed_table}, table.")
        add_unique_index_query = add_unique_index_query_temp.format(low_speed_tbl=trip_low_speed_table,
                                                                    date_str=current_date_str)
        database_connection.execute_query(add_unique_index_query, commit=True, raise_exception=False)

        logger.info(f"Creating indexes on :: {trip_low_speed_table}'")
        create_index_query = create_index_query_temp.format(date_str=current_date_str, low_speed_tbl=trip_low_speed_table)
        database_connection.execute_query(create_index_query, commit=True)

        all_trip_low_speed_table = required_tables_dict["all_trip_low_speed_table"]
        logger.info(f"Creating {all_trip_low_speed_table}, table.")
        date_column = ", date DATE"
        create_all_trip_low_speed_table_script = database_connection.generate_create_table_script(
            existing_table=required_tables_dict["siri_sm_res_monitor_28"], table_to_create=all_trip_low_speed_table,
            skip_columns=["unique_id","created_at"], append_query=extra_columns_query+date_column , drop=False)
        database_connection.execute_query(create_all_trip_low_speed_table_script, commit=True)

        logger.info(f"Creating indexes on :: {all_trip_low_speed_table}'")
        create_all_lsp_index_query = create_all_lsp_index_query_temp.format(all_low_speed_tbl=all_trip_low_speed_table)
        database_connection.execute_query(create_all_lsp_index_query, commit=True)


        # creating trip_low_speed_monitor table.
        trip_low_speed_monitor_table = required_tables_dict["trip_low_speed_monitor_table"]
        logger.info(f"Creating trip_low_speed_monitor table :: '{trip_low_speed_monitor_table}'")
        if OVERWRITE_TABLES:
            drop_low_speed_table_query = drop_table_query_temp.format(table=trip_low_speed_monitor_table)
            database_connection.execute_query(drop_low_speed_table_query, commit=True)

        if DELETE_PREVIOUS_TABLES:
            logger.info(f"Dropping previous day's tables.")
            previous_date = (current_date - datetime.timedelta(days=1)).date().strftime('%Y-%m-%d')
            tables_config = {"sample_table": required_tables_dict_ymd['trip_low_speed_sample_table'],
                             "all_samples_table": all_trip_low_speed_table,
                             "extra_columns": [{"date": previous_date}], "columns2ignore": ["id"]}

            database_connection.delete_previous_days_tables(table_names=[required_tables_dict_ymd['trip_low_speed_monitor_table'],
                                                                         required_tables_dict_ymd['trip_low_speed_sample_table'],
                                                                         required_tables_dict_ymd['segments_table']],
                                                            current_date=current_date, tables_config=tables_config,
                                                            data_expiry_days=DATA_EXPIRY_DAYS)

        create_trip_low_sp_monitor_table_qry = trip_low_speed_monitor_table_qry_tmp.format(
            trip_low_speed_mntr_tbl=trip_low_speed_monitor_table,
            trip_peform_tbl=required_tables_dict["trip_perform_monitor_table"],
            date_str=current_date_str)
        database_connection.execute_query(create_trip_low_sp_monitor_table_qry, commit=True)

        logger.info(f"Creating Triggers on table :: '{trip_low_speed_table}'")
        create_trigger_statements = create_trigger_statements_temp.format(low_speed_tbl=trip_low_speed_table)
        database_connection.execute_query(create_trigger_statements, commit=True)

        segments_table = required_tables_dict["segments_table"]

        # creating segments table.
        create_segments_table(trip_perform_tbl=required_tables_dict["trip_perform_monitor_table"],
                              gfts_times_tbl=required_tables_dict["gtfs_stops_time_table"],
                              gtfs_stops_tbl=required_tables_dict["gtfs_stops_table"],
                              segments_table=segments_table, current_date_str=current_date_str,
                              database_connection=database_connection, logger=logger)

        if database_connection.check_table_existance(table_names):
            logger.info("All required tables exist in database.")
            trip_perform_tbl = required_tables_dict["trip_perform_monitor_table"]
            siri_sample_table = required_tables_dict["siri_sm_res_monitor_28"].split(".")
            # table_columns_query = table_columns_query_temp.format(schema=siri_sample_table[0], table_name=siri_sample_table[1])
            # logger.debug(f"SIRI Columns Query::\n '{table_columns_query}'")
            siri_columns = database_connection.get_table_columns(schema_name=siri_sample_table[0],
                                                                 table_name=siri_sample_table[1])
            siri_columns_str = ','.join(['"{0}"'.format(col[0]) for col in siri_columns])
            siri_table_columns_str = ','.join(['siri_selection."{0}"'.format(col[0]) for col in siri_columns])

            if ENABLE_VEHICLE_QUERY:
                siri_select_latest_query = siri_select_latest_vehicle_query_temp.format(siri_columns=siri_columns_str,
                                                                                        siri_table=required_tables_dict[
                                                                                            "siri_sm_res_monitor_28"],
                                                                                        sample_limit=int(sample_limit),
                                                                                        loc_unique_col=location_unique_column)
            else:
                if ADD_TIME_FILTER:
                    logger.info(f"Adding Time filter, max_time_threshold='{max_time_threshold}' minutes.")
                    siri_time_filter_param = f"AND recordedattime>=CURRENT_TIMESTAMP AT TIME ZONE 'Israel' - INTERVAL '{max_time_threshold} minutes'"
                else:
                    siri_time_filter_param = ""

                siri_select_latest_idx1_qry = siri_select_latest_idx1_tmp.format(siri_columns=siri_columns_str,
                                                                                 siri_table=required_tables_dict[
                                                                                     "siri_sm_res_monitor_28"],
                                                                                 sample_limit=int(sample_limit),
                                                                                 loc_unique_col=location_unique_column,
                                                                                 time_filter=siri_time_filter_param)
                siri_select_latest_idx2_qry = siri_select_latest_idx2_tmp.format(siri_columns=siri_columns_str,
                                                                                 siri_table=required_tables_dict[
                                                                                     "siri_sm_res_monitor_28"],
                                                                                 sample_limit=int(sample_limit),
                                                                                 loc_unique_col=location_unique_column,
                                                                                 time_filter=siri_time_filter_param)

                siri_select_latest_query = siri_select_latest_query_tmp.format(siri_columns=siri_columns_str,
                                                                               siri_select_latest_idx1_qry=siri_select_latest_idx1_qry,
                                                                               siri_select_latest_idx2_qry=siri_select_latest_idx2_qry,
                                                                               sample_limit=int(sample_limit))


            trip_monitor_select_query = trip_monitor_select_query_tmp.format(trip_perform_tbl=trip_perform_tbl,
                                                                             siri_table_columns=siri_table_columns_str,
                                                                             siri_select_query=siri_select_latest_query)
            logger.debug(f"SIRI Latest Samples::\n '{trip_monitor_select_query}'")

            trip_low_speed_schema, trip_low_speed_table_name = trip_low_speed_table.split(".")
            siri_low_speed_columns = database_connection.get_table_columns(schema_name=trip_low_speed_schema,
                                                                           table_name=trip_low_speed_table_name)
            column_exceptions = ["created_at", "batch_id", "id", "route_short_name", "route_long_name", "siri_trip_id",
                                 "low_speed_recorded","aerial_speed","along_line_speed","is_latest", "current_stop_sequence","deviation_size"]

            siri_low_speed_columns_str = ','.join(
                ['"{0}"'.format(col[0]) for col in siri_low_speed_columns if col[0] not in column_exceptions])

            siri_low_speed_aliased_str = ','.join(
                ['trip_samp."{0}"'.format(col[0]) for col in siri_low_speed_columns if
                 col[0] not in column_exceptions])

            low_speed_samples_insert_qry = low_speed_samples_insert_qry_tmp.format(
                low_speed_samples_tbl=trip_low_speed_table,
                siri_low_sp_columns=siri_low_speed_columns_str,
                siri_low_sp_aliased_columns=siri_low_speed_aliased_str)

            if ADD_DEVIATION_FILTER:
                logger.info(f"Adding Deviation filter, DEVIATION_DISTANCE_LIMIT='{DEVIATION_DISTANCE_LIMIT}' meters.")
                deviation_filter_param = f"and s_s.deviation_size<={DEVIATION_DISTANCE_LIMIT} and s_s.previous_deviation_size<={DEVIATION_DISTANCE_LIMIT}"
            else:
                deviation_filter_param = ""

            speed_samples_query = speed_samples_query_tmp.format(segments_table=segments_table)
            trip_low_speed_main_query = trip_low_speed_main_query_tmp.format(trip_monitor_qry=trip_monitor_select_query,
                                                                             speed_samples_query=speed_samples_query,
                                                                             low_speed_mtr_tbl=trip_low_speed_monitor_table,
                                                                             low_speed_samples_insert_qry=low_speed_samples_insert_qry,
                                                                             max_speed_limit=max_speed_limit,
                                                                             low_speed_limit=low_speed_limit,
                                                                             pair_limit=int(sample_limit-1),
                                                                             deviation_filter=deviation_filter_param)


            logger.info(f"Trip Low-speed Main Query::\n '{trip_low_speed_main_query}'")
            logger.info(f"***************************************************************")
            logger.info(f"Adding data into {trip_low_speed_table}, day:{today_name}, day_no:{day_number}")
            logger.info(f"***************************************************************")
            while True:
                st_time = time.time()
                low_speed_count = database_connection.execute_query(trip_low_speed_main_query,
                                                                    commit=True,
                                                                    rows_affected=True,
                                                                    raise_exception=False)
                end_time = time.time()
                ex_time = end_time - st_time
                logger.info(f"Updated {trip_low_speed_table}({update_tbl_freq}sec), inserted_rows:{low_speed_count}, query_execution_time:{round(ex_time, 2)} sec.")

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
