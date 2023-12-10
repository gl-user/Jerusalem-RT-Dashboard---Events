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

# DEVIATION config.
# flag for overwriting deviation_monitor and deviation_sample table.
OVERWRITE_TABLES = config.getboolean('DEVIATION', 'OVERWRITE_TABLES')
# frequency for updating table in seconds.,
update_tbl_freq = config.getint('DEVIATION', 'UPDATE_FREQUENCY_SEC')
# number of samples to consider for deviation.
sample_limit = config.getint('DEVIATION', 'SAMPLE_LIMIT')
# distance threshold in meters.
distance_threshold = config.getfloat('DEVIATION', 'DISTANCE_THRESHOLD')
# delete previous day's tables and saving data in historical table.
DELETE_PREVIOUS_TABLES = config.getboolean('DEVIATION', 'DELETE_PREVIOUS_TABLES')
DATA_EXPIRY_DAYS = config.getint('DEVIATION', 'SAMPLES_DATA_EXPIRY_DAYS')
# script closing time,
SCRIPT_EXIT_TIME = config.get('DEVIATION', 'SCRIPT_EXIT_TIME')
SCRIPT_EXIT_TIME = datetime.datetime.strptime(SCRIPT_EXIT_TIME, '%H:%M').time()

# script testing paramters.
TEST_DATE = read_testing_date(config=config)

# max time threshold in minutes
ADD_TIME_FILTER = False
max_time_threshold = 20
# column used for differentiating locations. i.e., geom or distancefromstop
location_unique_column = 'geom'
# add sequence filter
enable_sequence_filter = False
# for matching only  by vehicle
ENABLE_VEHICLE_QUERY = False
# required tables to create trip_performance_table.
required_tables_dict_ymd = {"trip_perform_monitor_table": "public.trip_perform_monitor_yyyymmdd",
                        "gtfs_shape_table": "gtfs_data.gtfs_shape_geoms_yyyymmdd",
                        "siri_sm_res_monitor_28": "public.siri_sm_res_monitor_jerusalem",
                        "trip_deviation_table": "public.deviating_samples_yyyymmdd",
                        "trip_deviation_monitor_table": "public.trip_deviation_monitor_yyyymmdd",
                        "trip_all_events_table":"public.trip_all_events",
                        "all_trip_deviation_table": "public.all_deviating_samples"}

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

siri_select_latest_idx2_tmp = """  SELECT * from
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
                                ROUND(CAST(ST_Distance(ST_Transform(siri_selection.geom,32636),trip_monitor.geom_36N) as numeric),2) AS deviation_size,
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

deviated_records_query_tmp = """select trip_perform_id,
                                CASE
                                WHEN COUNT(*) FILTER (WHERE deviation_size > {max_distance} {seq_filter}) = {sample_limit} THEN TRUE
                                ELSE FALSE END AS deviated
                                from trip_deviation
                                group by trip_perform_id"""

sequence_filter = """ and "order">st_seq and  "order"<end_seq """

trip_deviation_main_query_tmp = """ with trip_deviation as  (
                                    {trip_monitor_qry}
                                ),
                                deviated_records as (
                                    {deviated_group_qry}
                                    ),
                                latest_event_index_query as
                                (
                                select trip_dev_mtr.trip_master_id as trip_perform_id,
                                    CASE WHEN deviated_records.deviated  and trip_dev_mtr.status=False 
                                    THEN trip_dev_mtr.event_index + 1 ELSE trip_dev_mtr.event_index END		
                                    as latest_event_index,
                                    CASE WHEN deviated_records.deviated and trip_dev_mtr.status=False 
                                    THEN True ELSE False END as is_latest_event
                                    from
                                    {trip_dev_mntr_tble}  trip_dev_mtr
                                    inner join deviated_records on  deviated_records.trip_perform_id = trip_dev_mtr.trip_master_id
                                ),
                                
                                update_query as 
                                (update   {trip_dev_mntr_tble}    trip_dev_mtr
                                    SET status = deviated_records.deviated,
                                    event_index = CASE WHEN deviated_records.deviated  and status=False THEN event_index + 1 ELSE event_index END,
                                    last_updated = CURRENT_TIMESTAMP AT TIME ZONE 'Israel'
                                    FROM deviated_records
                                    WHERE trip_dev_mtr.trip_master_id = deviated_records.trip_perform_id) 
                                    
                                {deviating_samples_insert_qry}    
                                """

table_columns_query_temp = """SELECT column_name  FROM information_schema.columns WHERE 
                             table_name = '{table_name}' and table_schema='{schema}';"""

deviating_samples_insert_qry_tmp = """ INSERT INTO {deviating_samples_tbl}({siri_dev_columns}, "batch_id",
                                        "route_short_name", "route_long_name", "siri_trip_id","is_latest")	
                                        SELECT {siri_dev_aliased_columns} ,latest_event_index_query.latest_event_index,
                                        trip_deviation.route_short_name, trip_deviation.route_long_name, trip_deviation.siri_trip_id,
                                        latest_event_index_query.is_latest_event
                                        from trip_deviation 
                                        inner join deviated_records on deviated_records.trip_perform_id=trip_deviation.trip_perform_id and deviated_records.deviated=True
                                        inner join latest_event_index_query on latest_event_index_query.trip_perform_id=deviated_records.trip_perform_id
                                        ON CONFLICT (unique_id, siri_trip_id) DO NOTHING;"""

extra_columns_query = """ "unique_id" integer,
                        "geom" geometry,
                         trip_perform_id bigint,
                         deviation_size NUMERIC(15, 2),
                         created_at timestamp without time zone DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'Israel'),
                         batch_id integer,
                         id serial PRIMARY KEY,
                         route_short_name text,
                         route_long_name text,
                         siri_trip_id integer,
                         is_latest boolean"""

create_index_query_temp = """ CREATE INDEX IF NOT EXISTS idx_deviation_perf_id_{date_str}   ON  {deviation_tbl} (trip_perform_id);
                              CREATE INDEX IF NOT EXISTS idx_deviation_batch_id_{date_str}   ON  {deviation_tbl} (batch_id);
                              """

create_all_dev_index_query_temp = """ CREATE INDEX IF NOT EXISTS idx_all_deviation_perf_id   ON  {all_deviation_tbl} (trip_perform_id);
                                     CREATE INDEX IF NOT EXISTS idx_all_deviation_batch_id   ON  {all_deviation_tbl} (batch_id);
                                     CREATE INDEX IF NOT EXISTS idx_all_deviation_date   ON  {all_deviation_tbl} (date);
                                  """

add_unique_index_query_temp = """ ALTER TABLE {deviation_tbl}
                                  ADD CONSTRAINT dev_unique_id_siri_id_{date_str} UNIQUE (unique_id, siri_trip_id);"""

create_trigger_statements_temp = """CREATE OR REPLACE FUNCTION insert_deviation_events()
                                    RETURNS TRIGGER AS $$
                                    BEGIN
                                        INSERT INTO public.trip_all_events(event_type, event_index, event_name, sample_time, 
                                            trip_perform_id,siri_trip_id, vehicle_number, route_short_name, route_long_name,
                                             origin_aimed_departure_time,route_id, geom)
                                        SELECT
                                            3, ds.batch_id, 'Deviation', ds.recordedattime, ds.trip_perform_id,
                                            ds.siri_trip_id, ds.vehicleref, ds.route_short_name, ds.route_long_name,
                                                ds.originaimeddeparturetime, ds.lineref, ds.geom
                                        FROM (
                                            SELECT DISTINCT ON (trip_perform_id)
                                                trip_perform_id, batch_id, recordedattime, lineref,
                                                siri_trip_id, vehicleref,
                                                route_short_name, route_long_name,
                                                originaimeddeparturetime, geom
                                            FROM deviation_events
                                            where deviation_events.is_latest = True
                                            order by trip_perform_id,recordedattime
                                        ) ds;
                                    
                                        RETURN NULL;
                                    END;
                                    $$ LANGUAGE plpgsql;
                                    
                                    DROP TRIGGER IF EXISTS insert_deviation_events_trigger ON {deviation_tble};
                                    CREATE  TRIGGER  insert_deviation_events_trigger
                                    AFTER INSERT ON {deviation_tble}
                                    REFERENCING NEW TABLE AS deviation_events
                                    FOR EACH STATEMENT
                                    EXECUTE FUNCTION insert_deviation_events();"""

drop_table_query_temp = """DROP TABLE IF EXISTS {table} CASCADE;"""

trip_deviation_monitor_table_qry_tmp = """
                                        CREATE TABLE  IF NOT EXISTS {trip_deviation_mntr_tbl} (
                                        id serial PRIMARY KEY,
                                        status boolean DEFAULT false,
                                        event_index integer DEFAULT 0,
                                        last_updated timestamp without time zone DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'Israel'),
                                        trip_master_id bigint UNIQUE REFERENCES {trip_peform_tbl}(ID));
                                        -- creating unique_idx on table.
                                        CREATE INDEX IF NOT EXISTS idx_deviation_trip_master_id_{date_str}   
                                        ON  {trip_deviation_mntr_tbl} (trip_master_id);
                                        ----insert IDs from performance table,
                                        insert into {trip_deviation_mntr_tbl}(trip_master_id)
                                        select id from {trip_peform_tbl}
                                        ON CONFLICT (trip_master_id) DO NOTHING;
                                        """

if __name__ == "__main__":
    try:
        log_file_init = "log_trip_deviation_"
        logger = logger_setup(console_log_level=console_logging_level, file_log_level=file_logging_level,
                              file_init=log_file_init, date_format='%Y-%m-%d')
        # Delete previous log files older than max_days
        delete_old_log_files(MAX_LOGS_DAYS, logger, file_init=log_file_init, date_format='%Y-%m-%d')
        # redis connection,
        SCRIPT_REDIS_KEY = "trip_deviation"
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
        trip_deviation_table = required_tables_dict["trip_deviation_table"]

        logger.info(f"Creating {trip_deviation_table}, table.")
        create_table_script = database_connection.generate_create_table_script(
            existing_table=required_tables_dict["siri_sm_res_monitor_28"], table_to_create=trip_deviation_table,
            skip_columns=["unique_id","created_at"], append_query=extra_columns_query, drop=OVERWRITE_TABLES)
        database_connection.execute_query(create_table_script, commit=True)

        logger.info(f"Adding unique_contraint to  {trip_deviation_table}, table.")
        add_unique_index_query = add_unique_index_query_temp.format(deviation_tbl=trip_deviation_table,
                                                                    date_str=current_date_str)
        database_connection.execute_query(add_unique_index_query, commit=True, raise_exception=False)

        logger.info(f"Creating indexes on :: {trip_deviation_table}'")
        create_index_query = create_index_query_temp.format(date_str=current_date_str, deviation_tbl=trip_deviation_table)
        database_connection.execute_query(create_index_query, commit=True)

        all_trip_deviation_table = required_tables_dict["all_trip_deviation_table"]
        logger.info(f"Creating {all_trip_deviation_table}, table.")
        date_column = ", date DATE"
        create_all_trip_deviation_table_script = database_connection.generate_create_table_script(
            existing_table=required_tables_dict["siri_sm_res_monitor_28"], table_to_create=all_trip_deviation_table,
            skip_columns=["unique_id", "created_at"], append_query=extra_columns_query+date_column , drop=False)
        database_connection.execute_query(create_all_trip_deviation_table_script, commit=True)

        logger.info(f"Creating indexes on :: {all_trip_deviation_table}'")
        create_all_dev_index_query = create_all_dev_index_query_temp.format(all_deviation_tbl=all_trip_deviation_table)
        database_connection.execute_query(create_all_dev_index_query, commit=True)

        trip_deviation_monitor_table = required_tables_dict["trip_deviation_monitor_table"]
        logger.info(f"Creating trip_deviation_monitor table :: '{trip_deviation_monitor_table}'")
        if OVERWRITE_TABLES:
            drop_trip_deviation_query = drop_table_query_temp.format(table=trip_deviation_monitor_table)
            database_connection.execute_query(drop_trip_deviation_query, commit=True)

        if DELETE_PREVIOUS_TABLES:
            logger.info(f"Dropping previous day's tables.")
            previous_date = (current_date - datetime.timedelta(days=1)).date().strftime('%Y-%m-%d')
            tables_config = {"sample_table": required_tables_dict_ymd['trip_deviation_table'],
                             "all_samples_table": all_trip_deviation_table,
                             "extra_columns": [{"date": previous_date}],"columns2ignore":["id"]}

            database_connection.delete_previous_days_tables(table_names=[required_tables_dict_ymd['trip_deviation_monitor_table'],
                                                                         required_tables_dict_ymd['trip_deviation_table']],
                                                            current_date=current_date, tables_config=tables_config,
                                                            data_expiry_days=DATA_EXPIRY_DAYS)

        create_trip_deviation_monitor_table_qry = trip_deviation_monitor_table_qry_tmp.format(
            trip_deviation_mntr_tbl=trip_deviation_monitor_table,
            trip_peform_tbl=required_tables_dict["trip_perform_monitor_table"],
            date_str=current_date_str)

        database_connection.execute_query(create_trip_deviation_monitor_table_qry, commit=True)
        logger.info(f"Creating Triggers on table :: '{trip_deviation_table}'")
        create_trigger_statements = create_trigger_statements_temp.format(deviation_tble=trip_deviation_table)
        database_connection.execute_query(create_trigger_statements, commit=True)
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

            logger.debug(f"SIRI Latest Records::\n '{trip_monitor_select_query}'")
            seq_filter = sequence_filter if enable_sequence_filter else ""
            deviated_records_query = deviated_records_query_tmp.format(max_distance=distance_threshold,
                                                                       sample_limit=sample_limit, seq_filter=seq_filter)
            logger.debug(f"Deviated Records Group Query::\n '{deviated_records_query}'")

            trip_deviation_schema, trip_deviation_table = trip_deviation_table.split(".")
            siri_deviation_columns = database_connection.get_table_columns(schema_name=trip_deviation_schema,
                                                                           table_name=trip_deviation_table)
            column_exceptions = ["created_at", "batch_id", "id", "route_short_name", "route_long_name", "siri_trip_id","is_latest"]
            siri_deviation_columns_str = ','.join(
                ['"{0}"'.format(col[0]) for col in siri_deviation_columns if col[0] not in column_exceptions])

            siri_deviation_columns_aliased_str = ','.join(
                ['trip_deviation."{0}"'.format(col[0]) for col in siri_deviation_columns if
                 col[0] not in column_exceptions])

            deviating_samples_insert_qry = deviating_samples_insert_qry_tmp.format(
                deviating_samples_tbl=trip_deviation_table,
                siri_dev_columns=siri_deviation_columns_str,
                siri_dev_aliased_columns=siri_deviation_columns_aliased_str)

            trip_deviation_main_query = trip_deviation_main_query_tmp.format(trip_dev_mntr_tble=trip_deviation_monitor_table,
                                                                             trip_monitor_qry=trip_monitor_select_query,
                                                                             deviated_group_qry=deviated_records_query,
                                                                             deviating_samples_insert_qry=deviating_samples_insert_qry)

            logger.info(f"Trip Deviation Main Query::\n '{trip_deviation_main_query}'")
            logger.info(f"***************************************************************")
            logger.info(f"Adding data into {trip_deviation_table}, day:{today_name}, day_no:{day_number}")
            logger.info(f"***************************************************************")
            while True:
                st_time = time.time()
                deviated_count = database_connection.execute_query(trip_deviation_main_query, commit=True,
                                                                   rows_affected=True, raise_exception=False)
                end_time = time.time()
                ex_time = end_time - st_time
                logger.info(
                    f"Updated {trip_deviation_table}({update_tbl_freq}sec), inserted_rows:{deviated_count}, query_execution_time:{round(ex_time, 2)} sec.")

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
