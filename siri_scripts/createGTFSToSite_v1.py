# -*- coding: utf-8 -*-
__author__ = 'asafeh'
import os
# Set the PYTHONIOENCODING environment variable to UTF-8
os.environ['PYTHONIOENCODING'] = 'utf-8'
from common.utils import (logger_setup, delete_old_log_files, delete_old_files,
                          map_log_level, config_reader, RedisConnection)
import psycopg2
import datetime
from shapely.wkb import loads
import os
import zipfile
from urllib.request import urlopen
import ssl
import sys
import pickle
import subprocess
import time

# Reading the configuration file
config = config_reader(config_dir=os.path.dirname(__file__), config_file='config.ini')

# DATABASE Connection parameters
DB_HOST = config.get('DATABASE', 'HOST')
DB_PORT = config.get('DATABASE', 'PORT')
DB_NAME = config.get('DATABASE', 'DATABASE')
DB_USER = config.get('DATABASE', 'USER')
DB_PASSWORD = config.get('DATABASE', 'PASSWORD')

# LOGGING config.
MAX_LOGS_DAYS = config.getint('LOGGING', 'MAX_LOGS_DAYS')
console_logging_level = map_log_level(config.get('LOGGING', 'CONSOLE_LEVEL'))
file_logging_level = map_log_level(config.get('LOGGING', 'FILE_LEVEL'))

GTFS_SCRIPT_DIR = config.get('GTFS-LOADER', 'GTFS_SCRIPT_DIR')
DOWNLOAD_DIR = config.get('GTFS-LOADER', 'DOWNLOAD_DIR')
EXTRACT_DIR = config.get('GTFS-LOADER', 'EXTRACT_DIR')
CMD_SCRIPT_PATH = config.get('GTFS-LOADER', 'DATA_LOADER_CMD_SCRIPT_PATH')
PICKLE_FILE_PATH = config.get('GTFS-LOADER', 'PICKLE_FILE_PATH')
if not os.path.exists(PICKLE_FILE_PATH):
    raise Exception(f"POLYGON_FILE, does not exists at: {PICKLE_FILE_PATH}")

# how many day's files to keep.
MAX_OLD_FILES_DAYS=1
MAX_OLD_TABLES_DAYS=1

def importGtfsToSQLParams(port, dirname, host, user, database, password, db_type, date, GTFS_SCRIPT_DIR):
    os.chdir(GTFS_SCRIPT_DIR)
    if port is None:
        setenv_command = "set PGPASSWORD=" + password
        drop_and_create_command = "type gtfs_tables_mot.sql | psql -h " + host + " -U " + user + " -d " + database + " -v date=" + date
        insert_command = "python import_gtfs_to_sql.py " + dirname + " " + date + " | psql -h " + host + " -U " + user + " -d " + database
        make_spatial_command = "type gtfs_tables_makespatial_mot.sql | psql -h " + host + " -U " + user + " -d " + database + " -v date=" + date
        create_index_command = "type gtfs_tables_makeindexes_mot.sql | psql -h " + host + " -U " + user + " -d " + database + " -v date=" + date
        vacuum_command = "type vacuumer_mot.sql | psql -h " + host + " -U " + user + " -d " + database + " -v date=" + date
        create_populate_routes_table_command = "type gtfs_tables_makepopulated_params.sql | psql -h " + host + " -U " + user + " -d " + database + " -v date=" + date
        move_command = "type move_tables_params.sql | psql -h " + host + " -p " + port + " -U " + user + " -d " + database + " -v date=" + date
        dtop_after_move_command = "type drop_after_move_params.sql | psql -h " + host + " -p " + port + " -U " + user + " -d " + database + " -v date=" + date
    else:
        setenv_command = "set PGPASSWORD=" + password
        drop_and_create_command = "type gtfs_tables_mot_params.sql | psql -h " + host + " -p " + port + " -U " + user + " -d " + database + " -v date=" + date
        insert_command = "python import_gtfs_to_sql.py " + dirname + " " + date + " | psql -h " + host + " -p " + port + " -U " + user + " -d " + database
        make_spatial_command = "type gtfs_tables_makespatial_mot_params.sql | psql -h " + host + " -p " + port + " -U " + user + " -d " + database + " -v date=" + date
        create_index_command = "type gtfs_tables_makeindexes_mot_params.sql | psql -h " + host + " -p " + port + " -U " + user + " -d " + database + " -v date=" + date
        vacuum_command = "type vacuumer_mot_params.sql | psql -h " + host + " -p " + port + " -U " + user + " -d " + database + " -v date=" + date
        create_populate_routes_table_command = "type gtfs_tables_makepopulated_params.sql | psql -h " + host + " -p " + port + " -U " + user + " -d " + database + " -v date=" + date
        move_command = "type move_tables_params.sql | psql -h " + host + " -p " + port + " -U " + user + " -d " + database + " -v date=" + date
        dtop_after_move_command = "type drop_after_move_params.sql | psql -h " + host + " -p " + port + " -U " + user + " -d " + database + " -v date=" + date

    print(insert_command)
    os.system(setenv_command + "&" + drop_and_create_command)
    os.system(setenv_command + "&" + insert_command)
    if db_type == "gis":
        os.system(setenv_command + "&" + make_spatial_command)
    os.system(setenv_command + "&" + create_index_command)
    os.system(setenv_command + "&" + vacuum_command)
    os.system(setenv_command + "&" + create_populate_routes_table_command)
    os.system(setenv_command + "&" + move_command)
    os.system(setenv_command + "&" + dtop_after_move_command)

def delete_gtfs_tables(dbname, user, password, host, port):
    """
    Deletes tables starting with "gtfs" and ending with yesterday's date .
    """
    # Calculate yesterday date
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    date_str = yesterday.strftime('%Y%m%d')

    # Connect to the database
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    cursor = conn.cursor()

    # Fetch all tables that start with "gtfs" and end with yesterday date
    cursor.execute(f"SELECT tablename FROM pg_tables WHERE tablename LIKE '%gtfs%{date_str}';")
    gtfs_tables = cursor.fetchall()
    gtfs_tables = [f"gtfs_data.{table[0]}" for table in gtfs_tables]
    logger.info(f"Existing GTFS Tables:: {gtfs_tables}")
    all_tables = gtfs_tables + [f"public.tripidtodate_{date_str}"]
    for table in all_tables:
        logger.info(f"DROPPING TABLE {table}")
        # Delete the table
        cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
        conn.commit()

    # Close the connection and cursor
    cursor.close()
    conn.close()

def filter_trips(DB_NAME,DB_USER,DB_PASSWORD,DB_HOST,DB_PORT, date, polygon):

    """
    Filters GTFS trips to be Jerusalem poly only
    """
    filtered_trips_table_name = f"gtfs_data.filtered_trips_gtfs_jeru_{date}"

    conn = psycopg2.connect(f"dbname= {DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port = {DB_PORT}")
    cur = conn.cursor()

    # # Check if the table exists
    # cur.execute(
    #     f"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = '{filtered_trips_table_name}');"
    # )
    # table_exists = cur.fetchone()[0]
    #
    # # If the table doesn't exist, create it
    # if table_exists:
    #     logger.info("Table already exist")

    # If the table doesn't exist, create it
    # Check if the table exists
    cur.execute(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE  table_schema = 'gtfs_data' 
            AND    table_name   = 'gtfs_shape_geoms_{date}'
        );
    """)

    if not cur.fetchone()[0]:
        logger.error(f"Table 'gtfs_shape_geoms_{date}' does not exist.")
        return

    # Query to get all the shapes
    cur.execute(f"SELECT * FROM gtfs_data.gtfs_shape_geoms_{date};")

    # Filter the shapes
    filtered_shape_ids = []
    while True:
        # Fetch a chunk of data
        shapes = cur.fetchmany(10)
        if not shapes:
            break

        for shape in shapes:
            # Convert the geometry to a shapely shape
            geom = loads(shape[1], hex=True)
            # Check if the shape is within the polygon
            if geom.intersects(polygon):
                filtered_shape_ids.append(shape[0])

    # # Query to get the trips with the filtered shape_ids
    # cur.execute(f"SELECT * FROM gtfs_data.gtfs_trips_{date} WHERE shape_id IN %s;", (tuple(filtered_shape_ids),))
    #
    # # Fetch the filtered trips
    # filtered_trips = cur.fetchall()

    cur.execute(f"DROP TABLE IF EXISTS {filtered_trips_table_name}")
    conn.commit()
    # Create a new table and insert the filtered trips
    if filtered_shape_ids:
        cur.execute(f"DROP TABLE IF EXISTS {filtered_trips_table_name}")
        conn.commit()
        cur.execute(
            f"CREATE TABLE gtfs_data.filtered_trips_gtfs_jeru_{date} AS SELECT * FROM gtfs_data.gtfs_trips_{date} WHERE shape_id IN %s;",
            (tuple(filtered_shape_ids),))
        # Commit the changes and close the connection
        conn.commit()
        cur.close()
        conn.close()
    else:
        logger.error("No shape_ids found!!.")



def download_file(file_name, output_directory, date_str=None, extract_file=False, extract_dir=None):
    try:
        file_url = "https://gtfs.mot.gov.il/gtfsfiles/" + file_name
        context = ssl._create_unverified_context()
        output_file_name = os.path.splitext(file_name)[0]+f'-{date_str}'+os.path.splitext(file_name)[1]
        # Download the file and save it to the specified directory
        output_file_path = os.path.join(output_directory, output_file_name)
        with urlopen(file_url, context=context) as zipFile:
            with open(output_file_path, 'wb') as output:
                output.write(zipFile.read())

        if os.path.exists(output_file_path):
            logger.info(f"File Downloaded successfully, '{output_file_path}'.")
        else:
            logger.error(f"Failed to Download file, '{output_file_path}'.")

        if extract_file and os.path.exists(output_file_path):
            if not os.path.exists(extract_dir):
                logger.warning(f"Extract Dir does not exist {extract_dir}.")
                os.makedirs(extract_dir)
            if not os.path.exists(extract_dir):
                logger.error(f"Extract Directory, {extract_dir} could not be created...")
                raise Exception(f"Extract Directory, {extract_dir} could not be created...")
            logger.info(f"Extracting {output_file_path}.")
            # Open the ZIP file in read mode
            with zipfile.ZipFile(output_file_path, 'r') as zip_ref:
                # Extract all contents of the ZIP file to the specified directory
                zip_ref.extractall(extract_dir)
                logger.info(f"Files are extracted in {EXTRACT_DIR}")

    except Exception as error:
        logger.error(f"Unable to download file, {file_name} due to {error}")


if __name__ == "__main__":
    try:
        log_file_init = "logs_gtfs_"
        logger = logger_setup(console_log_level=console_logging_level, file_log_level=file_logging_level,
                              file_init=log_file_init, date_format='%Y-%m-%d')
        # Delete previous log files older than max_days
        delete_old_log_files(MAX_LOGS_DAYS, logger, file_init=log_file_init, date_format='%Y-%m-%d')
        date_format = "%Y%m%d"
        current_date = datetime.datetime.now()
        current_date_str = current_date.strftime(date_format)
        zip_date_str = current_date.strftime('%Y-%m-%d')
        transportation_zip = 'israel-public-transportation.zip'
        clusterFilename = 'ClusterToLine.zip'
        tripFileName = 'TripIdToDate.zip'
        ChargingRavKavFileName = "ChargingRavKav.zip"
        TariffFileName_2022 = "Tariff_2022.zip"
        ZonesFileName_2022 = "zones_2022.zip"
        file_to_download = [transportation_zip,clusterFilename,tripFileName,
                            ChargingRavKavFileName,TariffFileName_2022,
                            ZonesFileName_2022]
        file_to_extract = [transportation_zip]
        logger.info("Starting to Download files...")
        for file in file_to_download:
            logger.info(f"Downloading {file}...")
            extract_file = True if file in file_to_extract else False
            download_file(file_name=file, output_directory=DOWNLOAD_DIR,
                          date_str=zip_date_str, extract_file=extract_file, extract_dir=EXTRACT_DIR)

        logger.info("All files downloaded.")
        logger.info(f"***************************************************************")
        logger.info("************.........Importing GTFS files..........*************")
        logger.info(f"***************************************************************")
        importGtfsToSQLParams(port=DB_PORT, dirname=EXTRACT_DIR,
                              host=DB_HOST, user=DB_USER, database=DB_NAME,
                              password=DB_PASSWORD, db_type='gis',
                              date=current_date_str,
                              GTFS_SCRIPT_DIR=GTFS_SCRIPT_DIR)
        logger.info(f"***************************************************************")
        logger.info("************......... GTFS files Imported..........*************")
        logger.info(f"***************************************************************")
        logger.info("************.........Filtering GTS files...........*************")
        with open(PICKLE_FILE_PATH, 'rb') as f:
            polygon = pickle.load(f, encoding="bytes")

        filter_trips(DB_NAME=DB_NAME, DB_USER=DB_USER,
                     DB_PASSWORD=DB_PASSWORD, DB_HOST=DB_HOST,
                     DB_PORT=DB_PORT, date=current_date_str, polygon=polygon)

        logger.info("************.........Deleting GTFS tables...........*************")
        delete_gtfs_tables(DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT)

        logger.info("DELETE PREVIOUS DAY's Files..")
        delete_old_files(max_days=0, logger=logger, files_dir=DOWNLOAD_DIR, file_init="",
                         file_extension=".zip")
        # Run the CMD script
        try:
            subprocess.run(CMD_SCRIPT_PATH, shell=True, check=True)
            logger.info("CMD script executed successfully.")

            logger.info("Adding timestamp to Redis.")
            # redis connection,
            SCRIPT_REDIS_KEY = "gtfs_loader"
            redis_connection = RedisConnection(logger=logger)
            # setting redis key with timestamp.
            redis_connection.set_key(key=SCRIPT_REDIS_KEY, value=int(time.time()))

        except subprocess.CalledProcessError as e:
            logger.error(f"Error occurred: {e}")

    except Exception as e:
        # Log the error or perform any necessary actions
        logger.exception(f"Error occurred: {str(e)}")
        sys.exit(1)

sys.exit(0)