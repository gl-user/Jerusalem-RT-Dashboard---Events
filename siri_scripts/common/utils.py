# -*- coding: utf-8 -*-

import logging
import datetime
import psycopg2
import os
import glob
import re
from psycopg2 import pool
import time
import sys
import inspect
from psycopg2 import extras
import redis
import configparser

def config_reader(config_dir, config_file = 'config.ini'):
    config = configparser.ConfigParser()
    config_file_path = os.path.join(config_dir, config_file)
    if not os.path.exists(config_file_path):
        raise Exception(f"Config file,'{config_file_path}' does not exist.")
    config.read(config_file_path)
    return config


def logger_setup(console_log_level=logging.DEBUG, file_log_level=logging.INFO, file_init="log_",
                 date_format='%Y-%m-%d', log_folder='logs'):
    calling_script_path = os.path.abspath(inspect.stack()[1][1])
    script_dir = os.path.dirname(calling_script_path)
    # script_dir = os.path.dirname(os.path.abspath(__file__))
    # Create a logger
    logger = logging.getLogger(__name__)

    # Configure the logger
    logger.setLevel(logging.DEBUG)

    # Create a handler for logging to console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_log_level)

    # Create a handler for logging to file
    current_date = datetime.datetime.now().strftime(date_format)
    if log_folder:
        log_folder = os.path.join(script_dir, log_folder)
        if not os.path.exists(log_folder):
            os.makedirs(log_folder)  # Create the log directory if it doesn't exist
        log_file = os.path.join(log_folder, f"{file_init}{current_date}.log")
    else:
        log_file = os.path.join(script_dir, f"{file_init}{current_date}.log")

    file_handler = logging.FileHandler(log_file, delay=True)
    file_handler.setLevel(file_log_level)

    # Create a formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger

def read_testing_date(config):
        TESTING = config.getboolean('TESTING', 'TEST_SCRIPT', fallback=False)
        if TESTING:
            TEST_DATE_STR = config.get('TESTING', 'TEST_DATE', fallback=None)
            if TEST_DATE_STR is None:
                raise Exception("Provide a valid TEST_DATE incase of TEST_SCRIPT=True")

            return datetime.datetime.strptime(TEST_DATE_STR, '%Y%m%d')

def map_log_level(level_str):
    """ Map string level to logging constant."""
    levels = {'DEBUG': logging.DEBUG, 'INFO': logging.INFO, 'WARNING': logging.WARNING, 'ERROR': logging.ERROR, 'CRITICAL': logging.CRITICAL}
    return levels.get(level_str, logging.INFO)

def get_date_from_filename(file_name, logger, date_format):
    pattern = r"\d{4}-\d{1,2}-\d{1,2}"
    # pattern = r"\b\d{4}-\d{1,2}-\d{1,2}\b"
    match = re.search(pattern, file_name)
    if match:
        date_string = match.group(0)
        parts = date_string.split("-")
        year = int(parts[0])
        month = int(parts[1].zfill(2))  # Convert month to two digits
        day = int(parts[2].zfill(2))  # Convert day to two digits
        date_obj = datetime.datetime(year, month, day)
        formatted_date = date_obj.strftime(date_format)
        logger.debug(f"Formatted Date:{formatted_date} for file_name, {file_name}")
        return formatted_date
    else:
        logger.exception("No date string found in the text.")
        raise Exception(f"Could not find date from filename , {file_name}")


def delete_old_log_files(max_days, logger, file_init="log_", date_format='%Y-%m-%d', log_folder='logs'):
    try:
        # script_dir = os.path.dirname(os.path.abspath(__file__))
        calling_script_path = os.path.abspath(inspect.stack()[1][1])
        script_dir = os.path.dirname(calling_script_path)
        current_date = datetime.datetime.now().strftime(date_format)
        log_files = glob.glob(os.path.join(script_dir, log_folder, f'{file_init}*.log'))
        for log_file in log_files:
            file_date_str = get_date_from_filename(log_file, logger, date_format)
            file_date = datetime.datetime.strptime(file_date_str, date_format)
            days_difference = (datetime.datetime.strptime(current_date, date_format) - file_date).days
            if days_difference > max_days:
                os.remove(os.path.join(script_dir, log_file))
                logger.info(f"Log file removed {log_file}")

    except Exception as error:
        logger.exception(f"Error occurred while deleting old log files.: {str(error)}")

def delete_old_files(max_days, logger, files_dir, file_init="", date_format='%Y-%m-%d', file_extension=".zip"):
    try:
        current_date = datetime.datetime.now().strftime(date_format)
        files = glob.glob(os.path.join(files_dir, f'{file_init}*{file_extension}'))
        for file in files:
            file_date_str = get_date_from_filename(file, logger, date_format)
            file_date = datetime.datetime.strptime(file_date_str, date_format)
            days_difference = (datetime.datetime.strptime(current_date, date_format) - file_date).days
            if days_difference > max_days:
                os.remove(os.path.join(files_dir, file))
                logger.info(f"File removed {file}")

    except Exception as error:
        logger.exception(f"Error occurred while deleting old files.: {str(error)}")

class DatabaseConnection:
    def __init__(self, host, port, database, user, password, logger):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.logger = logger
        self.connection_pool = None

    def create_connection_pool(self, minconn=1, maxconn=5):
        try:
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(
                minconn=minconn,
                maxconn=maxconn,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                connect_timeout=10
            )
            self.logger.debug("Connection pool created.")
        except (psycopg2.OperationalError, psycopg2.Error) as e:
            self.logger.error(f"Error creating connection pool: {str(e)}")

    def get_connection(self, max_retries=3, retry_delay=1, current_retry=0):
        if not self.connection_pool:
            self.create_connection_pool()
        if self.connection_pool is None:
            self.logger.exception("Connection was not created.")
            raise Exception("Connection was not created.")
        try:
            self.logger.debug("Getting connection from pool.")
            return self.connection_pool.getconn()
        except psycopg2.Error as e:
            self.logger.error(f"Error getting connection from pool: {str(e)}")
            if current_retry < max_retries:
                # Implement exponential backoff: wait for 2^current_retry seconds
                backoff_delay = 2 ** current_retry
                self.logger.info(f"Retrying in {backoff_delay} seconds...")
                time.sleep(backoff_delay)
                # Retry with an increased retry count
                return self.get_connection(max_retries, retry_delay, current_retry + 1)
            else:
                self.logger.error("Max retries reached. Could not establish a connection.")
                raise Exception("Max retries reached. Could not establish a connection.")

    def test_connection(self):
        if self.get_connection():
            self.logger.debug("Got connection from pool.")
        else:
            raise Exception("Unable to connect to database.")

    def release_connection(self, connection):
        self.logger.debug("Releasing connection to pool.")
        self.connection_pool.putconn(connection)

    def close_all_connections(self):
        self.logger.info("Closing all connections in the pool.")
        self.connection_pool.closeall()
        self.connection_pool = None

    def execute_query(self, query, values=None, return_value=False, commit=False, rows_affected=False, raise_exception=True):
        st_time = time.time()
        connection = None
        cursor = None
        db_record = None
        try:
            self.logger.debug("Starting Query Execution...")
            connection = self.get_connection()
            # self.logger.debug(f"Executing query:\n{str(query)}\n.....")
            cursor = connection.cursor()
            if values:
                cursor.execute(query, values)
            else:
                self.logger.debug("Executing Query.")
                cursor.execute(query)
            if return_value:
                db_record = cursor.fetchall()
            if commit:
                connection.commit()
            if rows_affected:
                db_record = cursor.rowcount

        except psycopg2.OperationalError as error:
            self.logger.error(f"OperationalError error occured {error}")
        except Exception as error:
            self.logger.error(f"Unable to execute query: {str(error)}")
            if raise_exception:
                raise Exception(f"Unable to execute query: {str(error)}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.release_connection(connection)
            end_time = time.time()
            ex_time = end_time - st_time
            self.logger.debug(f"Query Execution Time{round(ex_time,2)}sec.")
        return db_record

    def execute_extra_query(self, insert_query, insert_data, commit=True, rows_affected=False, raise_exception=True):
        st_time = time.time()
        connection = None
        cursor = None
        db_record = None
        try:
            connection = self.get_connection()
            self.logger.debug(f"Executing query:\n{str(insert_query)}\n.....")
            cursor = connection.cursor()
            extras.execute_values(cursor, insert_query, insert_data)
            if commit:
                connection.commit()
            if rows_affected:
                db_record = cursor.rowcount
        except Exception as error:
            self.logger.error(f"Unable to execute query: {str(error)}")
            if raise_exception:
                raise Exception(f"Unable to execute query: {str(error)}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.release_connection(connection)
            end_time = time.time()
            ex_time = end_time - st_time
            self.logger.debug(f"Query Execution Time{round(ex_time,2)}sec.")
        return db_record

    def check_table_existance(self, table_names):
        """ Check if all the required tables exist in database. All requires tables must exist in database."""
        connection = None
        cursor = None
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            na_tables = []
            for table in table_names:
                table_schema, table_name = table.split('.')
                # Query the pg_tables catalog to check if the table exists
                table_check_query = f"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}' and table_schema = '{table_schema}')"
                self.logger.debug(table_check_query)
                cursor.execute(table_check_query)
                if cursor.fetchone()[0]:
                    self.logger.info(f"{table_schema}.{table_name} exists in the database.")
                else:
                    self.logger.error(f"{table_schema}.{table_name} does not exist in the database!!")
                    na_tables.append(table_name)
        # Close the cursor and the connection
        except Exception as error:
            na_tables = True
            self.logger.error(f"Unable to check table: {str(error)}")
            raise Exception(f"Unable to check table: {str(error)}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.release_connection(connection)
            return not bool(na_tables)

    def table_is_empty(self, table_name):
        """ Checks whether the table is empty or not."""
        query = f"select count(*) from {table_name};"
        count = self.execute_query(query, return_value=True)[0][0]
        is_empty = True
        if count:
            is_empty = False

        return is_empty


    def get_table_columns(self, schema_name, table_name):
        table_columns_query = f"""SELECT column_name  FROM information_schema.columns WHERE 
                                     table_name = '{table_name}' and table_schema='{schema_name}';"""
        return self.execute_query(table_columns_query, return_value=True)

    def get_table_metadata(self, schema_name, table_name):
        # Query to retrieve the table columns and their metadata
        query = f"""
            SELECT column_name, data_type, character_maximum_length,
                   is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = '{table_name}' and table_schema='{schema_name}';
        """
        table_metadata = self.execute_query(query, return_value=True)

        return table_metadata

    def generate_create_table_script(self, existing_table, table_to_create, skip_columns=["unique_id"], append_query=None, drop=True):
        existing_table_schema, existing_table_name = existing_table.split(".")
        table_metadata = self.get_table_metadata(schema_name=existing_table_schema, table_name=existing_table_name)
        if drop:
            script = f" DROP TABLE IF EXISTS {table_to_create} CASCADE; "
        else:
            script = " "
        script += f"CREATE TABLE IF NOT EXISTS {table_to_create} (\n"
        for column_name, data_type, char_max_length, is_nullable, column_default in table_metadata:
            if column_name not in skip_columns and data_type != "USER-DEFINED":
                column_definition = f'    "{column_name}" {data_type}'
                if char_max_length is not None:
                    column_definition += f"({char_max_length})"

                script += column_definition + ",\n"
        if append_query:
            script += append_query + ",\n"
        # Remove the trailing comma and newline
        script = script.rstrip(",\n")
        script += "\n);"
        return script

    def delete_previous_days_tables(self, table_names:list, current_date, tables_config=None, date_format = "%Y%m%d", data_expiry_days=None):
        try:
            previous_date = (current_date - datetime.timedelta(days=1)).date()
            if data_expiry_days:
                last_data_date_str = (current_date - datetime.timedelta(days=int(data_expiry_days))).date().strftime('%Y-%m-%d')
            else:
                last_data_date_str = None
            previous_date_str = previous_date.strftime(date_format)
            self.move_samples_to_historical_table(previous_date_str=previous_date_str,
                                                  tables_config=tables_config,
                                                  last_data_date=last_data_date_str)
            # drop previous day's tables.
            for table_name in table_names:
                table_name = table_name.replace('yyyymmdd', previous_date_str)
                self.logger.info(f"Dropping previous day's table::'{table_name}'")
                drop_table_query = f"DROP TABLE IF EXISTS {table_name} CASCADE; "
                self.execute_query(drop_table_query, commit=True)
        except Exception as error:
            self.logger.exception(f"Skipped deleting previous day's table due to:: {error}.")

    def move_samples_to_historical_table(self, previous_date_str, tables_config=None, last_data_date=None):
        if tables_config:
            sample_table = tables_config["sample_table"].replace('yyyymmdd', previous_date_str)
            all_samples_table = tables_config["all_samples_table"]
            extra_columns = tables_config["extra_columns"]
            ignore_columns = tables_config["columns2ignore"]
            columns_mapping = tables_config.get("source_destination_column_mapping")
            self.logger.info(f"Moving previous day's data from {sample_table} to {all_samples_table} with date:{previous_date_str}")
            insert_query = self.generate_historical_insert_query(sample_table, all_samples_table, extra_columns, ignore_columns, columns_mapping)
            inserted_rows = self.execute_query(insert_query, commit=True, rows_affected=True, raise_exception=True)
            self.logger.info(f"Inserted {inserted_rows} from {sample_table} to {all_samples_table}.")
            if last_data_date:
                self.logger.info(f"DELETING DATA from {all_samples_table} older than {last_data_date}.")
                previous_data_delete_query = f"DELETE FROM {all_samples_table} WHERE date<'{last_data_date}';"
                deleted_rows = self.execute_query(previous_data_delete_query, commit=True, rows_affected=True, raise_exception=False)
                self.logger.info(f"DELETED {deleted_rows} from {all_samples_table}.")
            else:
                self.logger.warning(f"SKIPPING DELETING DATA from {all_samples_table} as no DATA_EXPIRY period specified.")

    def generate_historical_insert_query(self, source_table, destination_table, extra_columns, ignore_columns, columns_mapping):
        common_columns = self.get_common_columns(source_table, destination_table)
        common_columns = list(set(common_columns) - set(ignore_columns))
        extra_column_str = ""
        extra_column_value_str = ""
        mapping_source_column_str = ""
        mapping_destination_column_str = ""
        for column_dict in extra_columns:
            for column, value in column_dict.items():
                extra_column_str+= f',"{column}"'
                extra_column_value_str += f",'{value}'"

        if columns_mapping:
            for source_column, destination_column in columns_mapping.items():
                mapping_source_column_str += f',"{source_column}"'
                mapping_destination_column_str += f',"{destination_column}"'

        if common_columns:
            common_columns_str = '","'.join(common_columns)
            common_columns_str = f'"{common_columns_str}"'
            select_columns_str = common_columns_str + extra_column_value_str + mapping_source_column_str
            insert_columns_str = common_columns_str + extra_column_str + mapping_destination_column_str
            insert_query = f"INSERT INTO {destination_table} ({insert_columns_str})"
            select_query = f"SELECT {select_columns_str} from {source_table};"
            main_insert_query = insert_query + select_query
            return main_insert_query
        else:
            self.logger.exception(f"No common column exist between {source_table} and {destination_table}")
            raise Exception(f"No common column exist between {source_table} and {destination_table}")

    def get_table_column_names(self, table):
        table_schema, table_name = table.split(".")
        table_columns_qs = self.get_table_columns(schema_name=table_schema, table_name=table_name)
        table_columns = []
        for column in table_columns_qs:
            table_columns.append(column[0])
        return table_columns

    def get_common_columns(self, table1, table2):
        table1_columns = self.get_table_column_names(table=table1)
        table2_columns = self.get_table_column_names(table=table2)
        common_columns = list(set(table1_columns).intersection(set(table2_columns)))
        return common_columns


def create_segments_table(trip_perform_tbl, gfts_times_tbl, gtfs_stops_tbl, segments_table,
                          current_date_str, database_connection, logger, overwrite=False):
    try:
        drop_segments_tbl_query = f" DROP TABLE IF EXISTS {segments_table} CASCADE; "

        distinct_routes_query_tmp = """select DISTINCT on (route_id) *, ST_Length(geom_36n) as shape_len  from
                                        {trip_perform_tbl}"""

        max_stop_dist_query_tmp = """select gtfs_times.trip_id,max(gtfs_times.shape_dist_traveled) max_dist
                                     from {gfts_times_tbl} gtfs_times
                                     inner join trip_perf on gtfs_times.trip_id=trip_perf.gtfs_trip_id
                                     group by gtfs_times.trip_id"""

        final_segments_query_tmp = """select ROW_NUMBER() OVER () AS id, trip_perf.route_id,gtfs_times.stop_sequence,
                                            LEAD(gtfs_times.stop_sequence) OVER (PARTITION BY gtfs_times.trip_id ORDER BY gtfs_times.stop_sequence)  as next_seq,
                                            gtfs_stop.stop_code as st_stop_code,
                                            LEAD(gtfs_stop.stop_code) OVER (PARTITION BY gtfs_times.trip_id ORDER BY gtfs_times.stop_sequence) as end_stop_code,
                                            (gtfs_times.shape_dist_traveled/max_stop_dist.max_dist)*trip_perf.shape_len as st_dist_m,
                                            LEAD((gtfs_times.shape_dist_traveled/max_stop_dist.max_dist)*trip_perf.shape_len) OVER (PARTITION BY gtfs_times.trip_id ORDER BY gtfs_times.stop_sequence)  as end_dist_m,
                                            ST_LineSubstring(trip_perf.geom_36n,(gtfs_times.shape_dist_traveled/max_stop_dist.max_dist),
                                            LEAD(gtfs_times.shape_dist_traveled/max_stop_dist.max_dist) OVER (PARTITION BY gtfs_times.trip_id ORDER BY gtfs_times.stop_sequence)) as geom_36n,
                                            ST_Length(ST_LineSubstring(trip_perf.geom_36n,(gtfs_times.shape_dist_traveled/max_stop_dist.max_dist),
                                            LEAD(gtfs_times.shape_dist_traveled/max_stop_dist.max_dist) OVER ( PARTITION BY gtfs_times.trip_id ORDER BY gtfs_times.stop_sequence))) as shape_len,
                                            ST_Transform(ST_LineInterpolatePoint(trip_perf.geom_36n,(gtfs_times.shape_dist_traveled/max_stop_dist.max_dist)),4326) as snapped_geom
                                            from {gfts_times_tbl} gtfs_times
                                            inner join {gtfs_stops_tbl} gtfs_stop on gtfs_stop.stop_id =gtfs_times.stop_id 
                                            inner join trip_perf on gtfs_times.trip_id=trip_perf.gtfs_trip_id
                                            inner join max_stop_dist on gtfs_times.trip_id=max_stop_dist.trip_id"""

        create_segments_query_tmp = """ with trip_perf as ({distinct_routes_query}),
                                        max_stop_dist as
                                        (
                                        {max_stop_query}
                                        ),
                                        final_segments as (
                                        {final_segments_qry}
                                        ),
                                        is_min_max_stop as (
                                        select
                                        route_id, max(stop_sequence) max_seq, min(stop_sequence) as min_seq
                                        from final_segments
                                        where next_seq is not null
                                        group by route_id
                                        )
                                        select final_segments.*,
                                        CASE when final_segments.stop_sequence=is_min_max_stop.min_seq OR
                                        final_segments.stop_sequence=is_min_max_stop.max_seq
                                        THEN True ELSE False END as is_extreme
                                        into {segmemts_table}
                                        from
                                        final_segments
                                        inner join
                                        is_min_max_stop on final_segments.route_id=is_min_max_stop.route_id
                                        where next_seq is not null
                                        order by final_segments.route_id,final_segments.stop_sequence;
                                        """

        create_segments_indexes_query_tmp = """CREATE INDEX  IF NOT EXISTS idx_segment_geom_{date_str} ON  {segmemts_table} USING GIST (geom_36n);
                                                CREATE INDEX IF NOT EXISTS idx_segment_route_id__{date_str} ON  {segmemts_table} (route_id);
                                                CREATE INDEX IF NOT EXISTS idx_segment_stop_sequence__{date_str} ON  {segmemts_table} (stop_sequence);
                                                CREATE INDEX IF NOT EXISTS idx_segment_st_dist_m_{date_str} ON  {segmemts_table} (st_dist_m);
                                                CREATE INDEX IF NOT EXISTS idx_segment_end_dist_m_{date_str} ON  {segmemts_table} (end_dist_m);
                                                CREATE INDEX IF NOT EXISTS idx_segment_is_extreme_{date_str} ON  {segmemts_table} (is_extreme);"""

        distinct_routes_query = distinct_routes_query_tmp.format(trip_perform_tbl=trip_perform_tbl)
        max_stop_dist_query = max_stop_dist_query_tmp.format(gfts_times_tbl=gfts_times_tbl)
        final_segments_query = final_segments_query_tmp.format(gfts_times_tbl=gfts_times_tbl,
                                                               gtfs_stops_tbl=gtfs_stops_tbl)

        create_segments_query = create_segments_query_tmp.format(distinct_routes_query=distinct_routes_query,
                                                                 max_stop_query=max_stop_dist_query,
                                                                 final_segments_qry=final_segments_query,
                                                                 segmemts_table=segments_table)

        create_segments_indexes_query = create_segments_indexes_query_tmp.format(segmemts_table=segments_table,
                                                                                 date_str=current_date_str)
        if overwrite:
            logger.info(f"Dropping {segments_table}, table.")
            logger.info(drop_segments_tbl_query)
            database_connection.execute_query(drop_segments_tbl_query, commit=True)

        if not database_connection.check_table_existance([segments_table]):
            logger.info(f"Creating {segments_table}, table.")
            logger.info(create_segments_query)
            database_connection.execute_query(create_segments_query, commit=True)
            logger.info(f"Creating indexes on {segments_table}, table.")
            logger.info(create_segments_indexes_query)
            database_connection.execute_query(create_segments_indexes_query, commit=True)

    except Exception as e:
        # Log the error or perform any necessary actions
        logger.exception(f"Error occurred: {str(e)}")
        sys.exit(1)

def exit_script(logger, current_date, hour=3, minutes=30, db_connection=None):
    next_day_date = (current_date + datetime.timedelta(days=1)).date()
    # Convert time string to timedelta object
    target_time = datetime.time(hour, minutes, 0)
    # Specify the target time
    script_close_time = datetime.datetime.combine(next_day_date, target_time)

    if datetime.datetime.now() > script_close_time:

        if db_connection:
            logger.info("Closing All connections.")
            db_connection.close_all_connections()

        formatted_time = script_close_time.strftime('%Y-%m-%d %H:%M:%S')
        logger.info("**************************************************************")
        logger.info(f"***.Exiting the script at target time::{formatted_time}.***")
        logger.info("**************************************************************")
        logger.info("****************....End of the script....*********************")
        sys.exit(0)  # Exit the script



class RedisConnection:
    def __init__(self, logger, host='localhost', port=6379, timeout=2, expiry=None):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.logger = logger
        self.expiry = expiry
        self.redis_client = self.test_connection()

    def test_connection(self):
        try:
            # Create a connection to a Redis server
            redis_client = redis.StrictRedis(host=self.host, port=self.port, db=0,
                                             socket_timeout=self.timeout, decode_responses=True)
            # Send a ping command to check the connection
            redis_client.ping()
            return redis_client
        except redis.ConnectionError as error:
            # Handle connection errors
            self.logger.error(f"Unable to connect to Redis, due to: {error}")
            return False
        except Exception as error:
            self.logger.exception(f"Unable to connect to Redis, due to: {error}")
            return False
    def check_redis_status(self):
        try:
            status = self.redis_client.ping()
            return status
        except Exception as error:
            self.reconnect_client()
            return False

    def reconnect_client(self):
        try:
            self.logger.debug(f"Trying to reconnect to redis")
            self.redis_client = redis.StrictRedis(host=self.host, port=self.port, db=0,
                                              socket_timeout=self.timeout, decode_responses=True)
        except:
            self.redis_client = None

    def set_key(self, key, value):
        try:
            self.redis_client.set(key, value, ex=self.expiry)
        except Exception as error:
            self.logger.exception(f"Unable to set,{key}:{value}, due to: {error}")
            self.reconnect_client()

    def get_key(self, key):
        try:
            value = self.redis_client.get(key)
            return value
        except Exception as error:
            self.logger.exception(f"Unable to get ,{key}, due to: {error}")

    def delete_key(self, key):
        try:
            d = self.redis_client.delete(key)
            self.logger.info(f"{d} DELETED {key}")
        except Exception as error:
            self.logger.exception(f"Unable to DELETE,{key} due to: {error}")

