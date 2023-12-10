# -*- coding: utf-8 -*-
import os
import sys 
# Add the parent directory to the Python path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)
from common.utils import (logger_setup, delete_old_log_files,
                          DatabaseConnection, exit_script, map_log_level,
                          read_testing_date, config_reader, RedisConnection)
import datetime
import textwrap
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


# frequence for updating table,
update_tbl_freq = config.getint('TRIP-PERFORMANCE', 'UPDATE_FREQUENCY_SEC')
# buffer minutes.
buffer_minutes = config.getfloat('TRIP-PERFORMANCE', 'BUFFER_MINUTES')
late_time_limit = config.getfloat('TRIP-PERFORMANCE', 'LATE_TIME_LIMIT_MINUTES')
elapsed_time_limit = config.getfloat('TRIP-PERFORMANCE', 'ELAPSED_TIME_LIMIT_MINUTES')
# deactivate buffer minutes, time added in last stop departure time
# along with elapsed_time to dactivate a trip.
deactivate_buffer_minutes = config.getfloat('TRIP-PERFORMANCE', 'DEACTIVATE_BUFFER_MINUTES')


OVERWRITE_TABLE = config.getboolean('TRIP-PERFORMANCE', 'OVERWRITE_TABLES')
# delete previous day's tables and saving data in historical table.
DELETE_PREVIOUS_TABLES = config.getboolean('TRIP-PERFORMANCE', 'DELETE_PREVIOUS_TABLES')
DATA_EXPIRY_DAYS = config.getint('TRIP-PERFORMANCE', 'SAMPLES_DATA_EXPIRY_DAYS')
# script closing time,
SCRIPT_EXIT_TIME = config.get('TRIP-PERFORMANCE', 'SCRIPT_EXIT_TIME')
SCRIPT_EXIT_TIME = datetime.datetime.strptime(SCRIPT_EXIT_TIME, '%H:%M').time()

# script testing paramters.
TEST_DATE = read_testing_date(config=config)

REBUILD_SIRI_INDEXES = False
# required tables to create trip_performance_table.
required_tables_dict_ymd = {"trip_to_date_table": "public.tripidtodate_yyyymmdd",
                        "gtfs_calender_table": "gtfs_data.gtfs_calendar_yyyymmdd",
                        "gtfs_trips_table": "gtfs_data.filtered_trips_gtfs_jeru_yyyymmdd",
                        "gtfs_routes_table": "gtfs_data.gtfs_routes_yyyymmdd",
                        "gtfs_stop_times_table": "gtfs_data.gtfs_stop_times_yyyymmdd",
                        "gtfs_stop_table": "gtfs_data.gtfs_stops_yyyymmdd",
                        "gtfs_shape_table": "gtfs_data.gtfs_shape_geoms_yyyymmdd",
                        "siri_sm_res_monitor_28": "public.siri_sm_res_monitor_jerusalem",
                        "events_table": "public.trip_all_events"}

performance_table_temp = "public.trip_perform_monitor_yyyymmdd"
all_trip_perform_monitor = "public.all_trip_perform_monitor"

# create performance table query templates,
trip_to_date_tbl_query_tmp = """select tripid,linedetailrecordid as route_id, departuretime  
                                from {trip_to_date_tbl} where dayinweek={day_no} and 
                                '{date_str}' BETWEEN fromdate AND todate"""

gtfs_calendar_tbl_query_tmp = """select distinct service_id from {gtfs_calender_tbl}
                                 where '{date_str}' BETWEEN start_date AND end_date and {day_name}=1"""

trip_ids_stops_query_tmp = """  select stp_times_min.trip_id,stp_times_min.stop_id as st_stop_id,
                            st_end_pts.st_pt as st_seq,stops_st.stop_code as st_stop_code,
                            stops_st.the_geom as st_stop_geom,
                            stp_times_max.stop_id as end_stop_id,st_end_pts.end_pt as end_seq,
                            stops_end.stop_code as end_stop_code,normalize_timestamp(stp_times_max.departure_time) as end_dep_time
                            from {gtfs_stop_times_tbl} 
                            stp_times_min inner join 
                            (
                            SELECT trip_id, MIN(stop_sequence) AS st_pt, MAX(stop_sequence) AS end_pt
                            FROM {gtfs_stop_times_tbl}
                            GROUP BY trip_id
                            ) st_end_pts
                            on stp_times_min.stop_sequence=st_end_pts.st_pt
                            and stp_times_min.trip_id=st_end_pts.trip_id
                            inner join {gtfs_stop_times_tbl} 
                            stp_times_max
                            on stp_times_max.stop_sequence=st_end_pts.end_pt
                            and stp_times_max.trip_id=st_end_pts.trip_id
                            inner join {gtfs_stops_tbl} stops_st 
                            on stops_st.stop_id=stp_times_min.stop_id 
                            inner join {gtfs_stops_tbl} stops_end 
                            on stops_end.stop_id=stp_times_max.stop_id
                            """

create_tbl_query_temp = textwrap.dedent(""" WITH trip_filtering_query AS (
                                                select trip_id_date.tripid as siri_trip_id,
                                                trips.trip_id as gtfs_trip_id,
                                                trip_id_date.route_id as route_id,
                                                routes.route_short_name,
                                                routes.route_long_name,
                                                trip_id_date.departuretime as origin_aimed_departure_time,
                                                trips.shape_id as shape_id
                                                from ({trip_to_date_tbl_query}) trip_id_date
                                                inner join {gtfs_trips_tbl} trips
                                                on trips.route_id= cast(trip_id_date.route_id as varchar)
                                                inner join {gtfs_routes_tbl} routes
                                                on routes.route_id=trips.route_id
                                                inner join 
                                                ({gtfs_calendar_tbl_query})
                                                services on 
                                                services.service_id=trips.service_id
                                                inner join {gtfs_stop_times_tbl} stp_times on 
                                                trips.trip_id = stp_times.trip_id and 
                                                stp_times.departure_time=trip_id_date.departuretime::text and stp_times.stop_sequence = 1), 
                                                trip_ids_stops as 
                                                ({trip_ids_stops_query})
                                                select ROW_NUMBER() OVER () AS id,
                                                trip_filtering_query.siri_trip_id,trip_filtering_query.gtfs_trip_id,
                                                trip_filtering_query.route_id, 
                                                trip_filtering_query.route_short_name,
                                                trip_filtering_query.route_long_name,
                                                trip_filtering_query.shape_id,
                                                shape_tbl.the_geom ,ST_Transform(shape_tbl.the_geom,32636) as geom_36N,
                                               '{date_str}'::date+trip_filtering_query.origin_aimed_departure_time as origin_aimed_departure_time,
                                                CAST(NULL AS VARCHAR) AS vehicle_number,
                                                CAST(NULL AS int) AS trip_performance_status,
                                                CAST(NULL AS TIMESTAMP) AS  trip_performance_timestamp,
                                                CAST(NULL AS decimal(10, 2)) AS execution_time,
                                                CAST(NULL AS decimal(10, 2)) AS elapsed_time,
                                                CAST(False AS bool) AS activated,
                                                CAST(NULL AS TIMESTAMP)  AS activated_on,
                                                CAST(False AS bool) AS de_activated,
                                                CAST(NULL AS TIMESTAMP)  AS de_activated_on,
                                                trip_ids_stops.st_seq, trip_ids_stops.st_stop_id,trip_ids_stops.st_stop_code,
                                                trip_ids_stops.st_stop_geom,
                                                trip_ids_stops.end_seq, trip_ids_stops.end_stop_id,trip_ids_stops.end_stop_code,
                                                '{date_str}'::date+trip_ids_stops.end_dep_time as end_dep_time
                                               -- CAST(NULL AS bool) AS on_course_status,
                                               -- CAST(0 AS int) AS trip_deviation_status,
                                               -- CAST(NULL AS TIMESTAMP)  AS on_course_updated
                                                into {performance_tbl}
                                                from trip_filtering_query 
                                                inner join trip_ids_stops
                                                on trip_ids_stops.trip_id=trip_filtering_query.gtfs_trip_id
                                                LEFT JOIN  {gtfs_shape_table} shape_tbl 
                                                ON shape_tbl.shape_id = trip_filtering_query.shape_id; """)

drop_table_query = """ DROP TABLE IF EXISTS {table_name} CASCADE; """
count_records_query = """ SELECT count(*) from {table_name}; """

create_unique_contraint_query_temp = """ALTER TABLE {performance_tbl}
                                        ADD CONSTRAINT unique_id_contraint_{date_str} UNIQUE (id);"""

# queries for creating index
create_index_query_tmp = """ CREATE INDEX idx_trip_perform_monitor_id_{date_str} ON {performance_table} (id);
                             CREATE INDEX idx_trip_perform_monitor_siri_trip_id_{date_str}  ON {performance_table} (siri_trip_id);
                             CREATE INDEX idx_trip_perform_monitor_route_id_{date_str}  ON {performance_table} (route_id);
                             CREATE INDEX idx_trip_perform_monitor_geom_36N_{date_str} ON  {performance_table} USING GIST (geom_36N);
                             CREATE INDEX idx_trip_perform_monitor_activated_{date_str}  ON {performance_table} (activated);
                             CREATE INDEX idx_trip_perform_monitor_de_activated_{date_str}  ON {performance_table} (de_activated);
                             CREATE INDEX idx_trip_perform_monitor_vehicle_number_{date_str}  ON {performance_table} (vehicle_number);
                             CREATE INDEX idx_trip_perform_monitor_dpt_time_{date_str}  ON {performance_table} (origin_aimed_departure_time);"""

create_all_trip_monitor_index_query_temp = """ CREATE INDEX IF NOT EXISTS idx_all_trip_perform_monitor_id ON {all_trip_perf_table} (trip_perform_id);
                                             CREATE INDEX IF NOT EXISTS idx_all_trip_perform_monitor_siri_trip_id ON {all_trip_perf_table} (siri_trip_id);
                                             CREATE INDEX IF NOT EXISTS idx_all_trip_perform_monitor_route_id ON {all_trip_perf_table} (route_id);
                                             -- CREATE INDEX IF NOT EXISTS idx_all_trip_perform_monitor_geom_36N  ON  {all_trip_perf_table} USING GIST (geom_36N);
                                             -- CREATE INDEX IF NOT EXISTS idx_all_trip_perform_monitor_activated ON {all_trip_perf_table} (activated);
                                             -- CREATE INDEX IF NOT EXISTS idx_all_trip_perform_monitor_de_activated ON {all_trip_perf_table} (de_activated);
                                             CREATE INDEX IF NOT EXISTS idx_all_trip_perform_monitor_vehicle_number ON {all_trip_perf_table} (vehicle_number);
                                             CREATE INDEX IF NOT EXISTS idx_all_trip_perform_monitor_dpt_time  ON {all_trip_perf_table} (origin_aimed_departure_time);"""

# rebuild siri_tbl indexes.
rebuild_index_query_temp = """ REINDEX TABLE {siri_table}; """

# CREATE INDEX idx_datedvehiclejourneyref ON public.siri_sm_res_monitor_jerusalem  (datedvehiclejourneyref);
# CREATE INDEX idx_lineref ON public.siri_sm_res_monitor_jerusalem  (lineref);
# CREATE INDEX siri_idx_order ON public.siri_sm_res_monitor_jerusalem  ("order");
# CREATE INDEX idx_originaimeddeparturetime ON public.siri_sm_res_monitor_jerusalem  (originaimeddeparturetime);
# CREATE INDEX idx_responsetimestamp ON public.siri_sm_res_monitor_jerusalem  (responsetimestamp);
# CREATE INDEX idx_recordedattime ON public.siri_sm_res_monitor_jerusalem  (recordedattime);
# CREATE INDEX idx_vehicleref ON public.siri_sm_res_monitor_jerusalem (vehicleref);

# queries for updating performance table.
trip_performance_filter_query_temp = """ SELECT id,siri_trip_id, route_id, origin_aimed_departure_time AS departure_time,
                                        route_short_name,route_long_name,st_stop_geom
                                        FROM {performance_table}
                                        WHERE origin_aimed_departure_time <= CURRENT_TIMESTAMP AT TIME ZONE 'Israel' + INTERVAL '{buffer_minutes} minutes'
                                           AND activated = False"""

siri_query_temp_idx1 = """SELECT lineref AS route_id, datedvehiclejourneyref AS tripid, vehicleref,
                     originaimeddeparturetime AS departure_time, responsetimestamp, 1 as idx,
                     geom
                    FROM {siri_table}
                    WHERE
                    originaimeddeparturetime <= CURRENT_TIMESTAMP AT TIME ZONE 'Israel' + INTERVAL  '{buffer_minutes} minutes'
                    AND datedvehiclejourneyref<>'0'
                    AND datedvehiclejourneyref = trip_monitor.siri_trip_id
                    AND lineref = trip_monitor.route_id
                    AND originaimeddeparturetime= trip_monitor.departure_time
                    ORDER BY responsetimestamp ASC
                    LIMIT 1"""

siri_query_temp_idx2 = """SELECT lineref AS route_id, datedvehiclejourneyref AS tripid, vehicleref,
                        originaimeddeparturetime AS departure_time, responsetimestamp, 2 as idx,
                        geom
                        FROM {siri_table}
                        WHERE
                        originaimeddeparturetime <= CURRENT_TIMESTAMP AT TIME ZONE 'Israel' + INTERVAL  '{buffer_minutes} minutes'
                        AND datedvehiclejourneyref='0'
                        AND lineref = trip_monitor.route_id
                        AND originaimeddeparturetime= trip_monitor.departure_time
                        ORDER BY responsetimestamp ASC
                        LIMIT 1"""

siri_selection_query_temp = """SELECT  route_id, tripid, vehicleref, departure_time, responsetimestamp, geom
                                from (
                                ({siri_query_idx1})
                                UNION
                                ({siri_query_idx2})
                                ) joined_data
                                order by idx 
                                LIMIT 1	"""

trip_performance_select_query_temp = """ select id, responsetimestamp,time_diff,
                                        time_elapsed, vehicle_number, route_id,
                                        route_short_name,
                                        route_long_name,
                                        departure_time,
                                        st_stop_geom,
                                        siri_trip_id,
                                        case
                                        when time_diff<={late_time_limit} then 1
                                        when time_diff>{late_time_limit}  and time_diff<={elapsed_time_limit}  then 2
                                        when time_diff IS NOT null then 3
                                        else null  END AS performance_status,
                                        geom
                                        from 
                                        (
                                        SELECT trip_monitor.id,trip_monitor.siri_trip_id,trip_monitor.route_id,
                                        trip_monitor.route_short_name,
                                        trip_monitor.route_long_name,
                                        trip_monitor.departure_time, trip_monitor.st_stop_geom,
                                        siri_selection.responsetimestamp,
                                        ROUND(EXTRACT(EPOCH FROM (siri_selection.responsetimestamp - trip_monitor.departure_time))::numeric/60, 2) AS time_diff,
                                        ROUND(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP AT TIME ZONE 'Israel'- trip_monitor.departure_time))::numeric/60, 2) AS time_elapsed,
                                        siri_selection.vehicleref as vehicle_number, siri_selection.geom
                                        FROM ({trip_performance_filter_query}) trip_monitor
                                        left JOIN LATERAL (
                                        {siri_selection_query}
                                        ) siri_selection on true) trip_perf"""

trip_performance_update_query_temp = """ with performance_status_tbl as 
                                        ({trip_performance_select_query}),
                                        update_deactivate_trips as 
                                        ({deactivate_trips_update_query}),
                                        insert_qry as 
                                        ({trip_performance_event_insert_query}) 
                                        update  {performance_table}  trip_perfor_tbl
                                        set trip_performance_status=performance_status_tbl.performance_status,
                                        trip_performance_timestamp=performance_status_tbl.responsetimestamp,
                                        elapsed_time=performance_status_tbl.time_elapsed,
                                        vehicle_number=performance_status_tbl.vehicle_number,
                                        execution_time=performance_status_tbl.time_diff,
                                        activated_on=CURRENT_TIMESTAMP AT TIME ZONE 'Israel',
                                        activated= True
                                        from   performance_status_tbl
                                        where performance_status_tbl.id=trip_perfor_tbl.id
                                        and performance_status_tbl.performance_status is not null;"""

shape_not_found_query_temp = """select distinct siri_trip_id from {performance_table} where the_geom  is  null;"""

trip_performance_event_insert_query_temp = """ insert into {events_table}(event_type,event_index,event_name,sample_time,trip_perform_id,
                                                siri_trip_id,vehicle_number,
                                                route_short_name,route_long_name,origin_aimed_departure_time, route_id, geom)
                                                select case when perf_st_tbl.performance_status=2 then 1 else 2 end as event_type,
                                                perf_st_tbl.id,
                                                case when perf_st_tbl.performance_status=2 then 'Late' else 'Very-late' end as event_n,
                                                perf_st_tbl.responsetimestamp,
                                                perf_st_tbl.id,perf_st_tbl.siri_trip_id,perf_st_tbl.vehicle_number,
                                                perf_st_tbl.route_short_name, perf_st_tbl.route_long_name, perf_st_tbl.departure_time,
                                                perf_st_tbl.route_id,
                                                perf_st_tbl.geom
                                                from performance_status_tbl perf_st_tbl 
                                                LEFT JOIN (
                                                select distinct siri_trip_id 
                                                from {events_table}
                                                where event_type in (1,2)
                                                ) events_trips on events_trips.siri_trip_id=perf_st_tbl.siri_trip_id
                                                where  events_trips.siri_trip_id is null
                                                and perf_st_tbl.performance_status in (2,3)"""

trip_performance_deactivate_update_query_temp = """with  deactivate_trips
                                                    as (SELECT id,
                                                    case when execution_time<0 THEN end_dep_time +  INTERVAL '{deactivate_buffer} minutes'
                                                    ELSE end_dep_time + INTERVAL '{deactivate_buffer} minutes' + (execution_time * INTERVAL '1 minute')
                                                    END as deactivation_time
                                                    FROM {performance_table}
                                                    WHERE  activated = True and de_activated=False and execution_time is not null)
                                                    update   {performance_table}  trip_perfor_tbl
                                                    set 
                                                    de_activated_on=CURRENT_TIMESTAMP AT TIME ZONE 'Israel',
                                                    de_activated= True
                                                    from   deactivate_trips
                                                    where deactivate_trips.id=trip_perfor_tbl.id
                                                    and deactivate_trips.deactivation_time<=CURRENT_TIMESTAMP AT TIME ZONE 'Israel'"""

normalize_timestamp_function_query = """CREATE OR REPLACE FUNCTION normalize_timestamp(input_timestamp TEXT)
                                        RETURNS TIME AS $$
                                        DECLARE
                                            parsed_hour INT;
                                            parsed_minute INT;
                                            normalized_hour INT;
                                            normalized_minute INT;
                                        BEGIN
                                            -- Extract hours and minutes from the input timestamp
                                            parsed_hour := CAST(SPLIT_PART(input_timestamp, ':', 1) AS INT);
                                            parsed_minute := CAST(SPLIT_PART(input_timestamp, ':', 2) AS INT);
                                        
                                            -- Normalize hours and minutes to be within valid range
                                            normalized_hour := LEAST(parsed_hour, 23);
                                            normalized_minute := LEAST(parsed_minute, 59);
                                        
                                            -- Return the normalized time as TIME type
                                            RETURN MAKE_TIME(normalized_hour, normalized_minute, 59); -- Seconds set to 59 for 23:59:59
                                        END;
                                        $$ LANGUAGE plpgsql;"""

def generate_latest_update_tbl_query(siri_table_key="siri_sm_res_monitor_28", events_table_key="events_table"):
    """Generates query for updating trip_performance table with latest time_stamp."""
    trip_performance_filter_query = trip_performance_filter_query_temp.format(performance_table=performance_table,
                                                                              date_str=current_date_str, buffer_minutes=buffer_minutes)
    siri_query_idx1 = siri_query_temp_idx1.format(siri_table=required_tables_dict[siri_table_key],
                                                  buffer_minutes=buffer_minutes)
    siri_query_idx2 = siri_query_temp_idx2.format(siri_table=required_tables_dict[siri_table_key],
                                                  buffer_minutes=buffer_minutes)

    siri_query = siri_selection_query_temp.format(siri_query_idx1=siri_query_idx1, siri_query_idx2=siri_query_idx2)

    trip_performance_select_query = trip_performance_select_query_temp.format(
        trip_performance_filter_query=trip_performance_filter_query,
        siri_selection_query=siri_query,
        late_time_limit=late_time_limit,
        elapsed_time_limit=elapsed_time_limit)

    deactivate_trips_update_query = trip_performance_deactivate_update_query_temp.format(performance_table=performance_table,
                                                                                         deactivate_buffer=deactivate_buffer_minutes)

    trip_performance_event_insert_query = trip_performance_event_insert_query_temp.format(events_table=required_tables_dict[events_table_key])

    trip_performance_update_query = trip_performance_update_query_temp.format(
        performance_table=performance_table,
        trip_performance_event_insert_query=trip_performance_event_insert_query,
        trip_performance_select_query=trip_performance_select_query,
        deactivate_trips_update_query=deactivate_trips_update_query)
    #logger.debug(f"TRIP PERFORMANCE UPDATE QUERY ::\n '{trip_performance_update_query}' \n")
    return trip_performance_update_query

def create_events_table(DB_connection, events_table, current_date_str):
    # delete events data except todays'
    events_delete_query = f"DELETE FROM {events_table} where DATE(event_time)<>'{current_date_str}';"
    DB_connection.execute_query(events_delete_query, commit=True, raise_exception=False)
    event_table_query = f"""CREATE TABLE IF NOT EXISTS {events_table}
                        (id serial PRIMARY KEY,
                        event_type integer,
                        event_index integer,
                        event_name varchar,
                        event_time timestamp without time zone DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'Israel'),
                        sample_time timestamp,
                        siri_trip_id integer,
                        vehicle_number varchar,
                        route_short_name text,
                        route_long_name text,
                        origin_aimed_departure_time timestamp without time zone,
                        trip_perform_id bigint,
                        route_id integer,
                        bunching_meta JSON,
                        geom geometry);
                    CREATE INDEX IF NOT EXISTS idx_all_events_idx   ON  {events_table} (event_index);
                    CREATE INDEX IF NOT EXISTS idx_all_events_type  ON {events_table} (event_type);
					CREATE INDEX IF NOT EXISTS idx_all_events_time  ON {events_table} (event_time);
					CREATE INDEX IF NOT EXISTS idx_all_events_perf_id ON {events_table} (trip_perform_id);
					CREATE INDEX IF NOT EXISTS idx_all_events_id ON {events_table} (id);
					REINDEX TABLE {events_table};"""
    logger.info(f"Creating {events_table} table.")
    DB_connection.execute_query(event_table_query, commit=True)
    table_empty = DB_connection.table_is_empty(events_table)
    if table_empty:
        logger.info(f"Reseting index for {events_table}")
        reset_index_query = f"SELECT setval(pg_get_serial_sequence('{events_table}', 'id'), 1, false);"
        DB_connection.execute_query(reset_index_query, commit=True)

if __name__ == "__main__":
    try:
        log_file_init = "log_trip_performance_"
        logger = logger_setup(console_log_level=console_logging_level, file_log_level=file_logging_level,
                              file_init=log_file_init, date_format='%Y-%m-%d')
        # Delete previous log files older than max_days
        delete_old_log_files(MAX_LOGS_DAYS, logger, file_init=log_file_init, date_format='%Y-%m-%d')

        # redis connection,
        SCRIPT_REDIS_KEY = "trip_performance"
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
        performance_table = performance_table_temp.replace('yyyymmdd', current_date_str)
        table_names = [table.replace('yyyymmdd', current_date_str) for table in required_tables_dict.values()]

        create_events_table(database_connection, required_tables_dict["events_table"], current_date_str=current_date.strftime('%Y-%m-%d'))

        if database_connection.check_table_existance(table_names):
            logger.info("All required tables exist in database.")
            # check if performance table_exists in database,
            table_exists = database_connection.check_table_existance([performance_table])
            if not table_exists or (table_exists and OVERWRITE_TABLE):
                logger.info(f"Creating normalize_timestamp function.")
                database_connection.execute_query(normalize_timestamp_function_query, commit=True)

                drop_table_query = drop_table_query.format(table_name=performance_table)
                # indexes queries
                create_index_query = create_index_query_tmp.format(performance_table=performance_table,
                                                                   date_str=current_date_str)

                create_unique_contraint_query = create_unique_contraint_query_temp.format(performance_tbl=performance_table,
                                                                   date_str=current_date_str)
                logger.info(
                    f"Creating Performance table for date_str:{current_date_str}, day:{today_name},day_no:{day_number}.")
                trip_to_date_tbl_query = trip_to_date_tbl_query_tmp.format(
                    trip_to_date_tbl=required_tables_dict["trip_to_date_table"], day_no=day_number,
                    date_str=current_date_str)
                logger.debug(f"Trip to table query::\n '{trip_to_date_tbl_query}'")
                gtfs_calendar_tbl_query = gtfs_calendar_tbl_query_tmp.format(
                    gtfs_calender_tbl=required_tables_dict["gtfs_calender_table"],
                    date_str=current_date_str, day_name=today_name)
                logger.debug(f"Calender table query::\n '{gtfs_calendar_tbl_query}'")

                trip_ids_stops_query = trip_ids_stops_query_tmp.format(
                    gtfs_stop_times_tbl=required_tables_dict["gtfs_stop_times_table"],
                    gtfs_stops_tbl=required_tables_dict["gtfs_stop_table"])

                create_performance_tbl_query = create_tbl_query_temp.format(performance_tbl=performance_table,
                                                                            trip_to_date_tbl_query=trip_to_date_tbl_query,
                                                                            gtfs_trips_tbl=required_tables_dict[
                                                                                "gtfs_trips_table"],
                                                                            gtfs_calendar_tbl_query=gtfs_calendar_tbl_query,
                                                                            gtfs_stop_times_tbl=required_tables_dict[
                                                                                "gtfs_stop_times_table"],
                                                                            trip_ids_stops_query=trip_ids_stops_query,
                                                                            gtfs_shape_table=required_tables_dict[
                                                                                "gtfs_shape_table"],
                                                                            gtfs_routes_tbl=required_tables_dict[
                                                                                "gtfs_routes_table"],
                                                                            date_str=current_date_str)
                logger.debug(f"DROP EXISTING TABLE QUERY ::\n '{drop_table_query}' \n")
                logger.info(f"PERFORMANCE MONITOR TABLE QUERY ::\n '{create_performance_tbl_query}' \n")
                logger.info(f"PERFORMANCE MONITOR INDEXES QUERY ::\n '{create_index_query}' \n")
                if drop_table_query:
                    main_query_to_run = drop_table_query + create_performance_tbl_query + create_unique_contraint_query + create_index_query
                else:
                    main_query_to_run = create_performance_tbl_query + create_unique_contraint_query + create_index_query
                logger.info(f"PERFORMANCE MONITOR MAIN QUERY ::\n '{main_query_to_run}' \n")
                logger.info(f"Creating Performance table,'{performance_table}'")
                logger.info(f"..............................")
                database_connection.execute_query(main_query_to_run, commit=True)
                logger.info(f"Checking siri_trip_ids without geom:")
                siri_trip_ids_without_shp = database_connection.execute_query(
                    shape_not_found_query_temp.format(performance_table=performance_table), return_value=True)
                siri_trip_ids_without_shp = [siri_id[0] for siri_id in siri_trip_ids_without_shp]
                if siri_trip_ids_without_shp:
                    logger.error(f"siri_trip_ids without geom in '{performance_table}' table.")
                    logger.error(f"..............................")
                    logger.error(f"{siri_trip_ids_without_shp}")
                    logger.error(f"..............................")
                if REBUILD_SIRI_INDEXES:
                    siri_table_name = required_tables_dict["siri_sm_res_monitor_28"]
                    logger.info(f"Rebuilding '{siri_table_name}' indexes...")
                    rebuild_index_query = rebuild_index_query_temp.format(siri_table=siri_table_name)
                    logger.info(f"REBUILDING INDEX QUERY ::\n '{rebuild_index_query}' \n")
                    database_connection.execute_query(rebuild_index_query, commit=True)
            else:
                logger.info(f"'{performance_table}' Already exists in database.\n")

            if database_connection.check_table_existance([performance_table]):
                logger.info(f"Creating {all_trip_perform_monitor}, table.")
                extra_columns_query = """ trip_perform_id bigint, 
                                          the_geom geometry,
                                          date DATE"""
                create_all_trip_deviation_table_script = database_connection.generate_create_table_script(
                    existing_table=performance_table,
                    table_to_create=all_trip_perform_monitor,
                    skip_columns=[], append_query=extra_columns_query,
                    drop=False)
                database_connection.execute_query(create_all_trip_deviation_table_script, commit=True)

                logger.info(f"Creating indexes on :: {all_trip_perform_monitor}'")
                create_all_dev_index_query = create_all_trip_monitor_index_query_temp.format(
                    all_trip_perf_table=all_trip_perform_monitor)
                database_connection.execute_query(create_all_dev_index_query, commit=True)

                if DELETE_PREVIOUS_TABLES:
                    logger.info(f"Dropping previous day's tables.")
                    previous_date = (current_date - datetime.timedelta(days=1)).date().strftime('%Y-%m-%d')
                    tables_config = {"sample_table": performance_table_temp,
                                     "all_samples_table": all_trip_perform_monitor,
                                     "extra_columns": [{"date": previous_date}], "columns2ignore": ["id"],
                                     "source_destination_column_mapping": {"id":"trip_perform_id"}}
                    database_connection.delete_previous_days_tables(table_names=[performance_table_temp],
                                                                    current_date=current_date, tables_config=tables_config,
                                                                    data_expiry_days=DATA_EXPIRY_DAYS)
                records_count = \
                    database_connection.execute_query(count_records_query.format(table_name=performance_table),
                                                      return_value=True)[0][0]
                logger.info(f"{performance_table} successfully created with '{records_count}' records..")
                logger.info("**** Updating Trip_Performance_Montioring Table ****")
                latest_update_query = generate_latest_update_tbl_query()
                logger.info(f"TRIP PERFORMANCE UPDATE QUERY ::\n '{latest_update_query}' \n")
                logger.info(f"***************************************************************")
                logger.info(f"Updating {performance_table}, day:{today_name}, day_no:{day_number}")
                logger.info(f"***************************************************************")
                while True:
                    st_time = time.time()
                    latest_update_query = generate_latest_update_tbl_query()
                    updated_records_cnt = database_connection.execute_query(latest_update_query, commit=True,
                                                                            rows_affected=True, raise_exception=False)
                    end_time = time.time()
                    ex_time = end_time - st_time
                    logger.info(
                        f"Updated {performance_table}({update_tbl_freq}sec), updated_rows:{updated_records_cnt}, query_execution_time:{round(ex_time, 2)} sec.")

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
                logger.error(f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                logger.error(f"{performance_table} was not created in database.")
                logger.error(f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

        else:
            logger.error("Required tables are missing in database.")

    except Exception as e:
        # Log the error or perform any necessary actions
        logger.exception(f"Error occurred: {str(e)}")
        sys.exit(1)

sys.exit(0)
