# -*- coding: utf-8 -*-
import os
import sys
# Add the parent directory to the Python path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)
from common.utils import (config_reader)
import logging
import datetime
import psycopg2
from psycopg2.extras import execute_values
import zipfile
import csv
import io
import re
import glob


# Reading the configuration file
config = config_reader(config_dir=parent_dir, config_file='config.ini')

# DATABASE Connection parameters
host = config.get('DATABASE', 'HOST')
port = config.get('DATABASE', 'PORT')
database = config.get('DATABASE', 'DATABASE')
user = config.get('DATABASE', 'USER')
password = config.get('DATABASE', 'PASSWORD')

folder_path = config.get('GTFS-LOADER', 'DOWNLOAD_DIR')

# validate time only till this hour
MAX_HOUR_LIMIT = 27

# max no. of days to keep the logs for,
MAX_LOGS_DAYS = 7
console_logging_level = logging.DEBUG
file_logging_level = logging.INFO

# to use when column names are different in file and database tables.
file_columns_to_database_column_mapping = {"LineDetailRecordId": "LineDetailRecordId",
                                           "OfficeLineId": "OfficeLineId", "Direction": "Direction",
                                           "LineAlternative": "LineAlternative", "FromDate": "FromDate",
                                           "ToDate": "ToDate", "TripId": "TripId", "DayInWeek": "DayInWeek",
                                           "DepartureTime": "DepartureTime"}

database_column_types = {"LineDetailRecordId": "INTEGER", "OfficeLineId": "INTEGER", "Direction": "INTEGER",
                         "LineAlternative": "TEXT",
                         "FromDate": "timestamp::%d/%m/%Y %H:%M:%S",
                         "ToDate": "timestamp::%d/%m/%Y %H:%M:%S",
                         "TripId": "INTEGER",
                         "DayInWeek": "INTEGER", "DepartureTime": "TIME"}

# create table queries.
TABLE_INITIAL = "TripIdToDate"
TripIdToDate_create_table_query = """
        CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        LineDetailRecordId INTEGER,
        OfficeLineId INTEGER,
        Direction INTEGER,
        LineAlternative TEXT,
        FromDate TIMESTAMP,
        ToDate TIMESTAMP,
        TripId INTEGER,
        DayInWeek INTEGER,
        DepartureTime TIME,
        created_at TIMESTAMP DEFAULT current_timestamp);"""

logs_table_query = """CREATE TABLE IF NOT EXISTS tripidtodate_logs(
                        id SERIAL PRIMARY KEY,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        file_name TEXT,
                        total_rows INTEGER,
                        added_rows INTEGER,
                        skipped_rows INTEGER
                        );"""

class DatabaseConnection:
    def __init__(self, host, port, database, user, password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    def make_connection(self):
        try:
            conn = psycopg2.connect(host=self.host, port=self.port, database=self.database, user=self.user,
                                    password=self.password)
            logger.debug(f"Successfully connected to the database.")
            return conn
        except (psycopg2.OperationalError, psycopg2.Error) as e:
            logger.error(f"Error connecting to the database: {str(e)}")

    def test_connection(self):
        if self.make_connection():
            pass
        else:
            raise Exception("Unable to connect to database.")

    def execute_query(self, query, values=None, return_value=False):
        try:
            logger.debug(f"Executing query : {str(query)}")
            db_record = None
            connection = self.make_connection()
            cursor = connection.cursor()
            if values:
                cursor.execute(query, values)
            else:
                cursor.execute(query)
            if return_value:
                db_record = cursor.fetchall()
            connection.commit()
            cursor.close()
            connection.close()
            return db_record
        except Exception as error:
            logger.error(f"Unable to execute query due to : {str(error)}")
            raise Exception(f"Unable to execute query due to : {str(error)}")

    def add_row2db(self, input_rows, table_name: str, schema: str = "public"):
        try:
            if input_rows:
                logger.debug(f"Executing INSERT query...")
                columns = ['"' + columns.lower() + '"' for columns in input_rows[0].keys()]
                insert_query = "INSERT INTO " + schema + "." + table_name + "({}) VALUES %s".format(','.join(columns))
                # convert rows dict to list of lists
                values = [[value for value in row.values()] for row in input_rows]
                connection = self.make_connection()
                cursor = connection.cursor()
                execute_values(cursor, insert_query, values)
                connection.commit()
                logger.debug(f"INSERT query completed.")
                logger.info(f"Rows inserted in database {len(values)}.")
            else:
                logger.error("No rows to add.")
        except Exception as error:
            logger.error(f"Unable to execute INSERT query due to : {str(error)}")

    def add_logs(self, log):
        log_query = """INSERT into tripidtodate_logs(file_name, total_rows, added_rows, skipped_rows) VALUES (%s, %s, %s, %s)"""
        self.execute_query(log_query, values=log)
        logger.info(f"Added log: {log}")

    def get_file_names(self):
        files_names_query = """select distinct file_name from tripidtodate_logs;"""
        file_names = self.execute_query(files_names_query, return_value=True)
        if file_names:
            file_names = [file[0] for file in file_names]
        return file_names

def delete_old_log_files(max_days):
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        current_date = datetime.datetime.now().strftime('%Y-%m-%d')
        log_files = glob.glob(os.path.join(script_dir, 'log_*.txt'))
        for log_file in log_files:
            file_date_str = log_file[len(script_dir) + 5:-4]
            file_date = datetime.datetime.strptime(file_date_str, '%Y-%m-%d')
            days_difference = (datetime.datetime.strptime(current_date, '%Y-%m-%d') - file_date).days

            if days_difference > max_days:
                os.remove(os.path.join(script_dir, log_file))
                logger.info(f"Log file removed {log_file}")
    except Exception as error:
        logger.exception(f"Error occurred while deleting old log files.: {str(error)}")


def logger_setup(console_log_level=logging.DEBUG, file_log_level=logging.INFO):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Create a logger
    logger = logging.getLogger(__name__)

    # Configure the logger
    logger.setLevel(logging.DEBUG)

    # Create a handler for logging to console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_log_level)

    # Create a handler for logging to file
    current_date = datetime.datetime.now().strftime('%Y-%m-%d')
    log_file = os.path.join(script_dir, f"log_{current_date}.txt")
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(file_log_level)

    # Create a formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)


    return logger


def is_valid_time_str(time_str: str) -> bool:
    try:
        datetime.datetime.strptime(time_str, "%H:%M")
        return True
    except ValueError:
        return False


def update_time_str(value, max_hours_limit=MAX_HOUR_LIMIT-24):
    updated_value = value
    if isinstance(updated_value, str):
        hour, minutes = value.split(':')
        hour = int(hour) - 24
        if hour <= max_hours_limit:
            updated_value = "{:02d}".format(hour) + ":" + minutes
            if not is_valid_time_str(updated_value):
                raise Exception(f"Invalid updated time entry '{updated_value}'")
            logger.debug(f"Updated old time value, '{value}' to new time value '{updated_value}' ")
        else:
            logger.debug(f"Invalid time entry '{value}',Exceeds the given limit, '{MAX_HOUR_LIMIT}' in config.")
            raise Exception(f"Invalid time entry '{value}',exceeds the given hour limit, '{MAX_HOUR_LIMIT}hour' in config.")
    else:
        raise Exception(f"Invalid time entry '{value}'")
    return updated_value


def type_cast_row(input_rows: list, type_mapping: dict) -> tuple[list, list]:
    formatted_rows = []
    bad_rows = []
    total_rows = len(input_rows)
    logger.info(f"Formatting Rows..,{total_rows}.")
    for idx, input_row in enumerate(input_rows, 1):
        # update input row with new column,
        updated_row = {column: input_row.get(key) for key, column in file_columns_to_database_column_mapping.items()}
        logger.debug(f"Formatting Row {idx} of {total_rows}")
        formatted_row = {}
        bad_row = {}
        for key, value in updated_row.items():
            try:
                column_type = type_mapping.get(key)
                if column_type == "INTEGER":
                    value = int(value) if value else None
                elif column_type == "TEXT":
                    value = str(value)
                elif column_type == "TIME":
                    value = value.strip()
                    if not is_valid_time_str(value):
                        # update value incase of hour issue.
                        value = update_time_str(value)
                elif isinstance(column_type, str) and "timestamp::" in column_type:
                    value = datetime.datetime.strptime(value, column_type.split("timestamp::")[1])
                else:
                    raise Exception(f"Unknown Column Type,{column_type}, against '{key}'")

                formatted_row[key] = value
                # logger.debug(formatted_row)
            except Exception as e:
                logger.error(f"{idx}/{total_rows} Bad value, '{value}' found in Row. {e}")
                bad_row = input_row
                break

        if bad_row:
            bad_rows.append(bad_row)
        else:
            formatted_rows.append(formatted_row)
    logger.info("Rows formatting Completed.")
    logger.info(f"Total Formatted Rows={len(formatted_rows)}.")
    logger.info(f"Total Bad Rows={len(bad_rows)}.")
    return formatted_rows, bad_rows


def write_txt_file(bad_rows, filename, remove_header_comma=True):
    try:
        if bad_rows:
            with open(filename, 'w',encoding="utf-8") as txtfile:
                logger.info(f"Writing bad rows file, {filename}...")
                header = ','.join(bad_rows[0].keys())
                # remove trailing comma from header as in the actual file.
                if remove_header_comma:
                    if header[-1] == ',':
                        header = header[0:-1]
                txtfile.write(header + '\n')  # Write the header row to the file
                for row in bad_rows:
                    try:
                        row_str = ','.join(str(value) for value in row.values())
                        txtfile.write(row_str + '\n')
                    except Exception as error:
                        logger.error(f"Error writing row,. {error}")
                logger.info(f"{filename} saved.")
        else:
            logger.info("No badrows found.")
    except Exception as error:
        logger.error(f"Bad value found in Row. {error}")


def get_date_from_filename(file_name):
    pattern = r"\d{4}-\d{1,2}-\d{1,2}"
    # pattern = r"\b\d{4}-\d{1,2}-\d{1,2}\b"
    match = re.search(pattern, file_name)
    if match:
        date_string = match.group(0)
        parts = date_string.split("-")
        year = parts[0]
        month = parts[1].zfill(2)  # Convert month to two digits
        day = parts[2].zfill(2)  # Convert day to two digits
        formatted_date = f"{year}{month}{day}"
        logger.info(f"Formatted Date:{formatted_date} for file_name, {file_name}")
        return formatted_date
    else:
        logger.exception("No date string found in the text.")
        raise Exception(f"Could not find date from filename , {file_name}")


def get_zip_files(folder):
    zip_files = []
    if os.path.exists(folder):
        logger.info("Searching for zip files in folder.")
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            # Check if the path is a file and has a .zip extension
            if os.path.isfile(file_path) and filename.lower().endswith('.zip'):
                # Perform your desired actions on the ZIP file
                zip_files.append(file_path)

        return zip_files
    else:
        logger.error("Path does not exist")




if __name__ == "__main__":
    try:
        logger=logger_setup(console_log_level=console_logging_level, file_log_level=file_logging_level)
        # Delete previous log files older than max_days
        delete_old_log_files(MAX_LOGS_DAYS)

        zip_files = get_zip_files(folder_path)

        if zip_files:
            # check database connection.
            database_connection = DatabaseConnection(host, port, database, user, password)
            database_connection.test_connection()
            # create log table if does not exist.
            database_connection.execute_query(logs_table_query)
            added_files = database_connection.get_file_names()
            zip_files_names = [os.path.basename(zip_file) for zip_file in zip_files]
            # files to add into database
            zipfiles_to_add = list(set(zip_files_names) - set(added_files))

            if zipfiles_to_add:
                for tripid_zip_file in zip_files:
                    tripid_zip_file_name = os.path.basename(tripid_zip_file)
                    if tripid_zip_file_name in zipfiles_to_add:
                        tripid_zip_file_MYD = get_date_from_filename(tripid_zip_file_name)
                        zf = zipfile.ZipFile(tripid_zip_file, 'r')
                        # reading only first file.
                        text_file_name = zf.namelist()[0]
                        file_rows=[]
                        with zf.open(text_file_name) as temp_f:
                            logger.info(f"Reading Rows from files...")
                            reader = csv.DictReader(io.TextIOWrapper(temp_f, encoding='utf-8-sig'), delimiter=',')
                            # reading all rows
                            for row in reader:
                                if None in row:
                                    row[''] = row.pop(None)[0]

                                file_rows.append(row)

                            formatted_rows, bad_rows = type_cast_row(file_rows, database_column_types)
                            # create tripid table
                            TripIdToDate_table_name = TABLE_INITIAL+'_'+tripid_zip_file_MYD
                            database_connection.execute_query(TripIdToDate_create_table_query.format(table_name=TripIdToDate_table_name))
                            logger.info(f"Adding {len(formatted_rows)} rows to database {TripIdToDate_table_name}")
                            database_connection.add_row2db(formatted_rows, TripIdToDate_table_name, schema='public')
                            file_log = [tripid_zip_file_name, len(file_rows), len(formatted_rows), len(bad_rows)]
                            database_connection.add_logs(file_log)
                            bad_rows_file_path = os.path.splitext(tripid_zip_file)[0] + '_error.txt'
                            write_txt_file(bad_rows, os.path.join(folder_path, bad_rows_file_path))
                    else:
                        logger.info(f"{tripid_zip_file_name} already found in database.")
            else:
                logger.warning("No new zipfile found in folder.")
        else:
            logger.exception("No Zip file found in folder")
            raise Exception("No Zip file found in folder..")

    except Exception as e:
        # Log the error or perform any necessary actions
        logger.exception(f"Error occurred: {str(e)}")
        sys.exit(1)

sys.exit(0)
