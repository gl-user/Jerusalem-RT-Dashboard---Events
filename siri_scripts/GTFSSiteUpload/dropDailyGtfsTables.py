import psycopg2
import datetime as dttime
import runDropTables as dr
import os

conn = psycopg2.connect("dbname='postgis_siri' user='postgres' password='123qwe' host='localhost' port = '5432'")

threeWeeksBeforeNow=dttime.datetime.now().date()-dttime.timedelta(days=22)
datetooutput=dttime.datetime.strftime(threeWeeksBeforeNow,'%Y%m%d')
date_json_file_name = dttime.datetime.strftime(threeWeeksBeforeNow,'%d_%m_%Y')
siri_date_json_file_name = dttime.datetime.strftime(threeWeeksBeforeNow,'%m-%d-%Y')
weekDay = threeWeeksBeforeNow.weekday()
date_json_path_file_name = "C:\\WebFiles\\gtfs_siri_comp_upload\\trip_list\\" + date_json_file_name
siri_date_json_path_file_name = "C:\\WebFiles\\tfs_siri_comp_upload\\siri_points\\" + siri_date_json_file_name
if os.path.isfile(date_json_path_file_name):
    os.remove(date_json_path_file_name)
if os.path.isfile(date_json_path_file_name):
    os.remove(siri_date_json_path_file_name)
port = '5432'
dirname = None
host = 'localhost'
db = 'postgres'
user = 'postgis_siri'
password = '123qwe'
db_type = None

if weekDay != 1:
    dr.dropTable(port, dirname, host, db, user, password, db_type,datetooutput)