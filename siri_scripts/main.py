from GTFSSiteUpload.createGTFSToSite_v1 import (importGtfsToSQLParams,
                                                filter_trips,
                                                delete_gtfs_tables)
import datetime
import pickle

today_now = datetime.date.today()
date_name = datetime.datetime.strftime(today_now, '%Y%m%d') # example '20231015'
datetooutput=datetime.datetime.strftime(today_now, '%Y-%m-%d')
import_local_gtfs_dir = "C:\\GTFS\\Download\\Extract"
# importGtfsToSQLParams('5432', 'c://GTFS//Download//Extract//', 'localhost', 'postgres', 'gis', 'postgres','gis', date_name)


DB_PORT='5432'
dirname='c://GTFS//Download//Extract//'
DB_HOST='localhost'
DB_USER='postgres'
DB_PASSWORD='postgres'
DB_NAME='gis'

pickle_file_path = r'D:\Data Engineering Project\Tasks\first_task\src\existing_scripts\Jerusalem_siri_scripts\polygon2.pkl'

output_files ='test'

with open(pickle_file_path, 'rb') as f:
    polygon = pickle.load(f, encoding="bytes")

importGtfsToSQLParams(DB_PORT, dirname, DB_HOST, DB_USER, DB_NAME, DB_PASSWORD, 'gis', date_name)
# print("imported")

filter_trips(DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, date_name, polygon)
print("filtered")
#
#
import psycopg2
import pickle
import requests
from Process_functions import add_jerusalem_point_only, return_Jerusalem_siri
from config import (source_api_url,
                    conn, DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT,
                    date, dirname,
                    CreateTripIDToDate_path, drop_TripIDToDate_path, pickle_file_path
                    )
from Filter_GTFS import filter_trips, delete_gtfs_tables, importGtfsToSQLParams
from table_utils import save_polygon, truncate_table
import subprocess
import os
import Truncate_Jer
import schedule
import time
import threading
from datetime import datetime
from Process_functions import return_Jerusalem_siri, add_jerusalem_point_only
import sys
import logging

sys.path.append(r'C:\DEV\SRC\GLTRANSIT\trip_performance')
import utils

with open(pickle_file_path, 'rb') as f:
    polygon = pickle.load(f, encoding="bytes")

# Create a threading event
stop_event = threading.Event()

# process to kill later
running_processes = []

# get today's date as a sting
date_format = "%Y%m%d"

conn_string = f"dbname='{DB_NAME}' user='{DB_USER}' host='{DB_HOST}' port='{DB_PORT}' password='{DB_PASSWORD}'"

console_logging_level = logging.INFO
file_logging_level = logging.INFO


def scheduled_function():

    # Signal to stop the repeating_function
    stop_event.set()

    kill_running_processes()

    print(f"{datetime.now()}: Running other functions...")

    truncate_table(DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT)
    print("TRUNCATE TABLE Siri_sm_res_monitor_jerusalem")

    importGtfsToSQLParams(DB_PORT, dirname, DB_HOST, DB_USER, DB_NAME, DB_PASSWORD, 'gis', date)
    print("imported")

    filter_trips(DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, date, polygon)
    print("filtered")

    subprocess.run(["C:\Python311\python.exe", CreateTripIDToDate_path])
    print("created_IDtodate")

    delete_gtfs_tables(DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT)
    print("deleted")

    update_database_connection()

    try:
        # Start the process and send an "Enter" key press
        proc1 = subprocess.Popen([r"C:\DEV\SRC\GLTRANSIT\trip_performance\trip_performance.cmd"],
                                 stdin=subprocess.PIPE, text=True)
        running_processes.append(proc1)
        proc1.stdin.write("\n")
        proc1.stdin.flush()
        proc1.stdin.close()
        print("Started running trip_performance.exe")

        # Polling loop: wait for table creation
        table_created = False

        # get today's date name
        current_date = datetime.now()
        current_date_str = current_date.strftime(date_format)
        performance_table = "public.trip_perform_monitor_yyyymmdd"
        performance_table = performance_table.replace('yyyymmdd', current_date_str)
        # log_file_init = "log_main_"
        # logger = utils.logger_setup(console_log_level=console_logging_level, file_log_level=file_logging_level,
        #                             file_init=log_file_init, date_format='%Y-%m-%d')
        # database_connection = utils.DatabaseConnection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, logger)
        while not table_created:
            table_created = database_connection.check_table_existance([performance_table])  # Check if the table exists
            print(f"Check result: {table_created}")
            if not table_created:
                print("Table not created yet. Waiting...")
                time.sleep(15)  # Wait for 15 seconds before checking again
            else:
                print("Table created. Proceeding...")
            print(f"Loop end with table_created={table_created}")
        print("Loop exited.")

        proc2 = subprocess.Popen([r"C:\DEV\SRC\GLTRANSIT\trip_performance\trip_deviation.cmd"],
                                 stdin=subprocess.PIPE, text=True)
        running_processes.append(proc2)
        proc2.stdin.write("\n")
        proc2.stdin.flush()
        proc2.stdin.close()
        print("Started running trip_deviation.exe")

        proc3 = subprocess.Popen([r"C:\DEV\SRC\GLTRANSIT\trip_performance\trip_low_speed.cmd"],
                                 stdin=subprocess.PIPE, text=True)
        running_processes.append(proc3)
        proc3.stdin.write("\n")
        proc3.stdin.flush()
        proc3.stdin.close()
        print("Started running trip_low_speed.exe")

    except Exception as e:
        print(f"An error occurred during execution: {str(e)}")

    subprocess.Popen(["C:\Python27\python.exe", drop_TripIDToDate_path])
    print("deleted old IDtodate")

    # After finishing other functions, clear the stop event and re-activate add_jerusalem_point_only
    start_repeating_function()


def repeating_function():
    while not stop_event.is_set():
        start_time = time.time()  # Save the current time

        add_jerusalem_point_only(source_api_url, conn, polygon, database_connection)

        end_time = time.time()  # Save the time after function has run
        elapsed_time = end_time - start_time  # Calculate how long the function took to run

        # Sleep for the remaining duration (if any)
        sleep_time = max(15 - elapsed_time, 0)  # Ensure non-negative sleep time
        time.sleep(sleep_time)


def start_repeating_function():
    global thread

    # Check if thread is already alive; if yes, then stop it
    if 'thread' in globals() and thread.is_alive():
        thread.join()  # Wait for the thread to finish

    stop_event.clear()  # Clear the stop event
    thread = threading.Thread(target=repeating_function)  # Create a thread
    thread.start()  # Start the thread


def kill_running_processes():
    global running_processes
    for proc in running_processes:
        try:
            proc.terminate()
            proc.wait(timeout=5)  # Give it 5 seconds to shut down gracefully
        except subprocess.TimeoutExpired:
            # Process did not terminate in time, kill it
            proc.kill()
            proc.wait()
        except Exception as e:
            print(f"Error while killing process: {e}")
    running_processes.clear()


def check_table_exists(table_name):
    try:
        with psycopg2.connect(conn_string) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = %s;",
                (table_name,))
            return cursor.fetchone()[0] == 1
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return False


def update_database_connection():
    global database_connection

    current_date = datetime.now()
    current_date_str = current_date.strftime(date_format)
    performance_table = "public.trip_perform_monitor_yyyymmdd"
    performance_table = performance_table.replace('yyyymmdd', current_date_str)
    log_file_init = "log_main_"
    logger = utils.logger_setup(console_log_level=console_logging_level, file_log_level=file_logging_level,
                                file_init=log_file_init, date_format='%Y-%m-%d')
    database_connection = utils.DatabaseConnection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, logger)


if __name__ == "__main__":

    # Initialize the database connection
    update_database_connection()

    # Start repeating_function initially
    start_repeating_function()

    # Schedule the scheduled_function every day at 6 am
    schedule.every().day.at("06:00").do(scheduled_function)

    while True:
        schedule.run_pending()
        time.sleep(1)

# Run the function
if __name__ == '__main__':
    # save poly if needed
    # polygon = save_polygon(wkt_file_path, pickle_file_path)
    # process 2 :
    # return_Jerusalem_siri(source_api_url, polygon)
    # add_jerusalem_point_only(source_api_url, conn, polygon)
    # process 3:
    # response = requests.get (destination_api_url)
    # data = response.json ()
    # print(data)
    if os.path.exists(continue_requests_flag):
        os.remove(continue_requests_flag)
        print("flag file deleted")
    truncate_table(DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT)
    print("TRUNCATE TABLE Siri_sm_res_monitor_jerusalem")
    importGtfsToSQLParams(DB_PORT, dirname, DB_HOST, DB_USER,DB_NAME, DB_PASSWORD, 'gis', date)
    print("imported")
    filter_trips(DB_NAME,DB_USER,DB_PASSWORD,DB_HOST,DB_PORT, date, polygon)
    print("filtered")
    subprocess.run(["C:\Python311\python.exe", CreateTripIDToDate_path])
    print("created_IDtodate")
    subprocess.run(["C:\Python27\python.exe", drop_TripIDToDate_path])
    print("deleted old IDtodate")
    delete_gtfs_tables(DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT)
    print("deleted")
    with open(continue_requests_flag, 'w') as f:
        f.write('continue')
        print("flag file created")

