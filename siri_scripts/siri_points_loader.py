# -*- coding: utf-8 -*-
import os
import sys
# Add the parent directory to the Python path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)
from common.utils import (logger_setup, delete_old_log_files, DatabaseConnection,
                          exit_script, config_reader, map_log_level, RedisConnection)
import sys
from datetime import datetime
import time
import requests
import json
from shapely.geometry import Point
import pickle
from psycopg2.extensions import AsIs


# Reading the configuration file
config = config_reader(config_dir=os.path.dirname(__file__), config_file='config.ini')

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

PICKLE_FILE_PATH = config.get('SIRI-LOADER', 'POLYGON_FILE')
SIRI_API_URL = config.get('SIRI-LOADER', 'SIRI_API_URL')
if not os.path.exists(PICKLE_FILE_PATH):
    raise Exception(f"POLYGON_FILE, does not exists at: {PICKLE_FILE_PATH}")
DATA_FETCHING_INTERVAL = config.getint('SIRI-LOADER', 'DATA_FETCHING_INTERVAL_SEC')
# script closing time,
SCRIPT_EXIT_TIME = config.get('SIRI-LOADER', 'SCRIPT_EXIT_TIME')
SCRIPT_EXIT_TIME = datetime.strptime(SCRIPT_EXIT_TIME, '%H:%M').time()

TRUNCATE_SIRI_TABLE = True
REFRESH_MV_DELAY = 0

required_tables_dict = {"siri_sm_res_monitor_table": "public.siri_sm_res_monitor_jerusalem"}
JSON_KEYS_TO_DB_COLUMN_MAPPING =  {"ResponseMessageIdentifier": "responsemessageidentifier",
                                  "RequestMessageRef": "requestmessageref", "ResponseTimestamp": "stopmonitorrestimestamp",
								  "resResponseTimestamp": "responsetimestamp",
                                  "ProducerRef": "producerref", "RecordedAtTime": "recordedattime", "LineRef": "lineref",
                                  "VehicleRef": "vehicleref", "OperatorRef": "operatorref", "Longitude": "vehiclelocationx",
                                  "Latitude": "vehiclelocationy", "OriginAimedDepartureTime": "originaimeddeparturetime",
                                  "DataFrameRef": "dataframeref", "DatedVehicleJourneyRef": "datedvehiclejourneyref",
                                  "StopPointRef": "stoppointref", "Order": "order", "DistanceFromStop": "distancefromstop",
                                  "Bearing": "bearing", "Velocity": "velocity", "Status": "monitorstatus", "resstatus": "resstatus"}

NON_EMPTY_KEYS = ["Longitude", "Latitude"]
LAT_LONG_KEYS = {"long": "Longitude",
                 "lat": "Latitude"}

# global variable for comparing response with last one,
LAST_API_RESPONSE = None

# open the pickle
with open (PICKLE_FILE_PATH, 'rb') as f:
    POLYGON_GEOM = pickle.load (f, encoding="bytes")

def fetch_siri_data(api_url: str) -> dict:
        try:
            global LAST_API_RESPONSE
            logger.debug("Fetching SIRI Data..")
            header = {"Accept-Encoding": "gzip"}
            request = requests.get(api_url, headers=header, timeout=5)
            json_contents = request.content
            json_dict = json.loads(json_contents)

            if json_dict == LAST_API_RESPONSE:
                logger.info("Skipping JSON response as no new data.")
                return None
            else:
                LAST_API_RESPONSE = json_dict
                return format_siri_json_data(json_dict)

        except ValueError as e:
            logger.error(f"JSON_ERROR while fetching siri_data: {str(e)}")
        except Exception as error:
            logger.exception(f"Exception fetching siri_data: {str(error)}")

def flatten_siri_json(value_obj:dict) -> dict:
    flattened = {}
    if isinstance(value_obj, dict):
        for key, value in value_obj.items():
            if isinstance(value, dict):
                flattened.update(flatten_siri_json(value))
            else:
                flattened[key] = value
    else:
        logger.error(f"{value_obj} is not dict.")
    return flattened


def format_siri_json_data(siri_data: dict) -> list:
    """Reads the response JSON and returns list of rows."""
    try:
        formatted_siri_data = []
        service_delivery_keys = ["ResponseTimestamp", "ProducerRef", "ResponseMessageIdentifier","RequestMessageRef", "Status"]
        replace_service_delivery_mapping = {"Status": "resstatus", "ResponseTimestamp": "resResponseTimestamp"}
        service_delivery_dict = {}
        for key in service_delivery_keys:
            siri_key = replace_service_delivery_mapping.get(key, key)
            service_delivery_dict[siri_key] = siri_data["Siri"]["ServiceDelivery"].get(key)

        stop_monitoring_delivery = siri_data["Siri"]["ServiceDelivery"]["StopMonitoringDelivery"][0]
        service_delivery_dict["ResponseTimestamp"] = stop_monitoring_delivery.get("ResponseTimestamp")
        service_delivery_dict["Status"] = stop_monitoring_delivery.get("Status")

        for siri_record_dict in stop_monitoring_delivery["MonitoredStopVisit"]:
            formatted_siri_record = {}
            # adding service_delivery_keys in record
            formatted_siri_record.update(service_delivery_dict)
            # adding MonitoredStopVisit keys in record
            formatted_siri_record.update(flatten_siri_json(siri_record_dict))
            formatted_siri_data.append(formatted_siri_record)

        return formatted_siri_data

    except Exception as error:
        logger.exception(f"Exception formatting SIRI data: {str(error)}")
        logger.error(f"error SIRI data:: {siri_data}")


def process_siri_data(siri_formatted_data: list) -> list:
    """Sorts data as per the columns for inserting data into the database.
       Creates list of data rows."""
    try:
        json_sorted_keys = list(JSON_KEYS_TO_DB_COLUMN_MAPPING.keys())
        all_rows = []
        for siri_record in siri_formatted_data:
            try:
                # create a seperate list to add sorted data.
                db_row = []
                for key in json_sorted_keys:
                    value = siri_record.get(key)
                    # check if required keys are not empty,
                    if value is None and key in list(NON_EMPTY_KEYS+list(LAT_LONG_KEYS.values())):
                        # raise exception to skip adding row into data.
                        raise ValueError(f"MISSING_KEY_DATA:'{key}', Bad row:: {siri_record}")
                    else:
                        db_row.append(siri_record.get(key))
                # adding geometry value
                longitude = siri_record[LAT_LONG_KEYS["long"]]
                latitude = siri_record[LAT_LONG_KEYS["lat"]]
                if point_in_polygon(long=longitude, lat=latitude):
                    logger.debug(f"Point(long={longitude}, lat={latitude}) is within polygon.")
                    geom_str_value = AsIs(f"ST_SetSRID(ST_MakePoint({longitude}, {latitude}), 4326)")
                    db_row.append(geom_str_value)
                else:
                    logger.debug(f"Point(long={longitude}, lat={latitude}) is outside polygon.")
                    # skip adding row.
                    continue

                all_rows.append(db_row)

            except ValueError:
                logger.debug(f"MISSING_KEY_DATA:'{key}', Bad row:: {siri_record}")

            except Exception as error:
                logger.exception(f"Exception Processing SIRI data: {str(error)}")

        return all_rows

    except Exception as error:
        logger.exception(f"Exception Processing SIRI data: {str(error)}")

def insert_siri_data(siri_processed_data: list, siri_table: str, db_connection, bulk_insert=True) ->None:
    """  data into list of tuples and inserts into the SIRI table"""
    siri_geom_column = "geom"
    db_columns = tuple(list(JSON_KEYS_TO_DB_COLUMN_MAPPING.values()) + [siri_geom_column])
    # db_columns = tuple(JSON_KEYS_TO_DB_COLUMN_MAPPING.values())
    siri_table = "public.siri_sm_res_monitor_jerusalem"
    columns_str = ', '.join(f'"{col}"' for col in db_columns)
    insert_data_tuples = [tuple(row) for row in siri_processed_data]
    inserted_rows_count = 0
    if bulk_insert:
        logger.info("Inserting SIRI table using Bulk Insert.")
        insert_query = f"""INSERT INTO {siri_table} ({columns_str})  VALUES %s 
                                        ON CONFLICT (vehicleref, recordedattime, vehiclelocationx,
                                         vehiclelocationy,datedvehiclejourneyref, lineref, 
                                         originaimeddeparturetime) DO NOTHING"""
        try:
            inserted_rows_count=db_connection.execute_extra_query(insert_query, insert_data_tuples, rows_affected=True)
        except Exception as error:
            logger.error(f"BULK_INSERTION_ERROR: Rows Skipped:.ERROR ::{str(error)}")
            inserted_rows_count=0
    else:
        logger.info("Inserting SIRI table using Simple insert.")
        placeholders = ", ".join(["%s"] * (len(db_columns)))
        insert_query = f""" INSERT INTO {siri_table} ({columns_str})  VALUES ({placeholders})  
                            ON CONFLICT (vehicleref, recordedattime, vehiclelocationx, 
                            vehiclelocationy,datedvehiclejourneyref, lineref, 
                            originaimeddeparturetime) DO NOTHING"""
        for insert_row in insert_data_tuples:
            logger.debug(f"Inserting row:: {insert_row}")
            try:
                count = db_connection.execute_query(insert_query, values=insert_row, commit=True, rows_affected=True)
                inserted_rows_count = inserted_rows_count + count
            except Exception as error:
                logger.error(f"INSERTION_ERROR: Row Skipped:: {insert_row}. ERROR ::{str(error)}")

    logger.info(f"{inserted_rows_count}/{len(insert_data_tuples)} rows added into siri table.")



TYPE_CONVERTERS = {"timestamp": lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%dT%H:%M:%S'),
                   "date": lambda x: datetime.strptime(x, '%Y-%m-%d').date(),
                   "str": lambda x: str(x),
                   "int": lambda x: int(x),
                   "float": lambda x: float(x),
                   "bool": lambda x: x if bool(x) else x}
def validate_data(data_rows: list) -> list:
    column_type_mapping = {
        'responsemessageidentifier': TYPE_CONVERTERS["str"],
        'requestmessageref': TYPE_CONVERTERS["str"],
        'stopmonitorrestimestamp': TYPE_CONVERTERS["timestamp"],
        'responsetimestamp': TYPE_CONVERTERS["timestamp"],
        'producerref': TYPE_CONVERTERS["str"],
        'recordedattime': TYPE_CONVERTERS["timestamp"],
        'lineref': TYPE_CONVERTERS["int"],
        'vehicleref': TYPE_CONVERTERS["str"],
        'operatorref': TYPE_CONVERTERS["int"],
        'vehiclelocationx': TYPE_CONVERTERS["float"],
        'vehiclelocationy': TYPE_CONVERTERS["float"],
        'originaimeddeparturetime': TYPE_CONVERTERS["timestamp"],
        'dataframeref': TYPE_CONVERTERS["date"],
        'datedvehiclejourneyref': TYPE_CONVERTERS["int"],
        'stoppointref': TYPE_CONVERTERS["int"],
        'order': TYPE_CONVERTERS["int"],
        'distancefromstop': TYPE_CONVERTERS["float"],
        'bearing': TYPE_CONVERTERS["float"],
        'velocity': TYPE_CONVERTERS["float"],
        'monitorstatus': TYPE_CONVERTERS["bool"],
        'resstatus': TYPE_CONVERTERS["bool"]
    }
    validated_data_converted = []
    validated_data_actual = []
    for row in data_rows:
        validated_row = {}
        try:
            for key, value in row.items():
                key_column = JSON_KEYS_TO_DB_COLUMN_MAPPING.get(key)
                column_type_func = column_type_mapping.get(key_column)

                if column_type_func:
                    validated_row[key] = column_type_func(value)
                else:
                    logger.warning(f"{key} Does not have type defined. Skipping validation.")

            validated_data_converted.append(validated_row)
            validated_data_actual.append(row)

        except ValueError as error:
            logger.error(f"NOT_VALIDATED: Bad row:: {row} ::{str(error)}")

        except Exception as error:
            logger.exception(f"NOT_VALIDATED: Bad row:: {row} : {str(error)}")

    return validated_data_actual, validated_data_converted

def point_in_polygon(long, lat):
    if long is not None and lat is not None:
        point = Point(long, lat)
        is_within = point.within(POLYGON_GEOM)
        return is_within
    else:
        logger.error("Lat and log cannot be null.")


def create_siri_jerusalem_table(DB_connection, siri_table_name="public.siri_sm_res_monitor_jerusalem", reset_index=True, TRUNCATE_TABLE=True):
    logger.info(f"Creating {siri_table_name} if not exists.")
    create_table_query = f"""create table if not exists {siri_table_name} ( 
                            unique_id SERIAL PRIMARY KEY,monitoritemidentifier integer, responsemessageidentifier text,
                            requestmessageref text, responsetimestamp timestamp, producerref text,  "resstatus" boolean,
                            stopmonitoringdeliverynum integer, stopmonitorrestimestamp timestamp, monitorstatus boolean, 
                            monitoringrefidseker integer, monitoredstopvisitnum integer, RecordedAtTime timestamp, LineRef integer, DirectionRef smallint, 
                            PublishedLineName varchar, VehicleRef varchar,OperatorRef smallint, destinationrefidseker integer, 
                            vehiclelocationx real,vehiclelocationy real, OriginAimedDepartureTime timestamp, aimedarrivaltime timestamp, 
                            ExpectedArrivalTime timestamp,geom geometry, DataFrameRef date, DatedVehicleJourneyRef integer,StopPointRef integer, "order" smallint, 
                            DistanceFromStop real, Bearing real, Velocity real,
                            created_at timestamp without time zone DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'Israel'));"""
    create_indexes_query = f"""  CREATE INDEX  IF NOT EXISTS  idx_lineref ON {siri_table_name}  (lineref);
                                CREATE INDEX IF NOT EXISTS  idx_originaimeddeparturetime ON {siri_table_name}  (originaimeddeparturetime); 
                                CREATE INDEX IF NOT EXISTS  idx_datedvehiclejourneyref ON {siri_table_name}  (datedvehiclejourneyref); 
                                CREATE INDEX IF NOT EXISTS  idx_responsetimestamp ON {siri_table_name}  (responsetimestamp); 
                                CREATE INDEX IF NOT EXISTS  idx_recordedattime ON {siri_table_name}  (recordedattime); 
                                CREATE INDEX IF NOT EXISTS  idx_vehicleref ON {siri_table_name} (vehicleref); 
                                CREATE INDEX IF NOT EXISTS  idx_geom_gist ON  {siri_table_name} USING GIST (geom);
                                CREATE INDEX  IF NOT EXISTS idx_distancefromstop ON {siri_table_name} (distancefromstop);"""
    main_query  = create_table_query + create_indexes_query
    DB_connection.execute_query(main_query, commit=True)

    logger.info(f"CREATING UINQUE_CONSTRAINT on {siri_table_name}.")
    create_unique_index_query = f""" ALTER TABLE  {siri_table_name}  
                                       ADD CONSTRAINT idx_siri_unique_row_idx UNIQUE (vehicleref, recordedattime, vehiclelocationx, vehiclelocationy,
                                       datedvehiclejourneyref, lineref, originaimeddeparturetime);"""
    DB_connection.execute_query(create_unique_index_query, commit=True, raise_exception=False)

    # truncate table,
    if TRUNCATE_TABLE:
        logger.info(f"TRUNCATING {siri_table_name}.")
        trunacate_table_query = f"""TRUNCATE TABLE {siri_table_name} RESTART IDENTITY CASCADE;;"""
        DB_connection.execute_query(trunacate_table_query, commit=True)

    if reset_index:
        logger.info(f"REINDEXING {siri_table_name}.")
        reset_indexes_query = f"REINDEX TABLE {siri_table_name};"
        DB_connection.execute_query(reset_indexes_query, commit=True)

def refresh_trips_all_status(db_connection):
    """Refreshes trips_all_status MATERIALIZED VIEW."""
    try:
        logger.info("REFRESHING MATERIALIZED VIEW CONCURRENTLY")
        query = """REFRESH MATERIALIZED VIEW CONCURRENTLY trips_all_status;"""
        db_connection.execute_query(query, commit=True)
    except Exception as error:
        logger.error(f"Unable to refresh materialized view due to {error}")

if __name__ == "__main__":
    try:
        log_file_init = "log_siri_points_loader_"
        logger = logger_setup(console_log_level=console_logging_level,
                              file_log_level=file_logging_level,
                              file_init=log_file_init, date_format='%Y-%m-%d')
        # Delete previous log files older than max_days
        delete_old_log_files(MAX_LOGS_DAYS, logger, file_init=log_file_init, date_format='%Y-%m-%d')
        # redis connection,
        SCRIPT_REDIS_KEY = "siri_loader"
        redis_connection = RedisConnection(logger=logger)
        # check database connection.
        database_connection = DatabaseConnection(host, port, database, user, password, logger)
        database_connection.test_connection()
        date_format = "%Y%m%d"

        current_date = datetime.now()
        dayinweek = current_date.isoweekday()
        # converting day number to make sunday as first day and saturday as last day,
        day_number = dayinweek + 1 if dayinweek < 7 else 1
        today_name = current_date.strftime("%A").lower()

        current_date_str = current_date.strftime(date_format)
        # updating table names
        siri_table_name = required_tables_dict["siri_sm_res_monitor_table"]
        table_to_check = [siri_table_name]
        create_siri_jerusalem_table(DB_connection=database_connection,
                                    siri_table_name=siri_table_name,
                                    reset_index=True,
                                    TRUNCATE_TABLE=TRUNCATE_SIRI_TABLE)
        if database_connection.check_table_existance(table_to_check):
            # truncate existing data except Today's data.
            logger.info(f"***************************************************************")
            logger.info(f"Adding data into {siri_table_name}, day:{today_name}, day_no:{day_number}")
            logger.info(f"***************************************************************")
            logger.info(f"Fetching SIRI data at an interval of {DATA_FETCHING_INTERVAL}sec.")
            while True:
                formatted_data = fetch_siri_data(SIRI_API_URL)
                if formatted_data:
                    logger.info(f"Fetched and formatted_data SIRI Data..{len(formatted_data)}")
                    validated_data_actual, validated_data_converted = validate_data(formatted_data)
                    processed_data = process_siri_data(validated_data_actual)
                    insert_siri_data(processed_data, siri_table_name, database_connection, bulk_insert=True)
                    # sleep for REFRESH_MV_DELAY seconds so that all events tables are updated;
                    time.sleep(REFRESH_MV_DELAY)
                    refresh_trips_all_status(database_connection)

                # setting redis key with timestamp.
                redis_connection.set_key(key=SCRIPT_REDIS_KEY, value=int(time.time()))

                logger.info(f"Sleeping for {DATA_FETCHING_INTERVAL-REFRESH_MV_DELAY}")
                time.sleep(DATA_FETCHING_INTERVAL-REFRESH_MV_DELAY)
                # stopping script next day at a particular time i.e, 3:30AM.
                exit_script(logger=logger, current_date=current_date,
                            hour=SCRIPT_EXIT_TIME.hour, minutes=SCRIPT_EXIT_TIME.minute)
        else:
            logger.error(f"Required table, '{siri_table_name}' missing in database.")
            raise Exception (f"Required tables are missing in database i.e., '{siri_table_name}'.")

    except Exception as e:
        # Log the error or perform any necessary actions
        logger.exception(f"Error occurred: {str(e)}")
        sys.exit(1)

sys.exit(0)
