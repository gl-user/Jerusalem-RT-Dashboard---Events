import psycopg2
import pickle
from shapely.geometry import shape
from shapely.wkb import loads
import subprocess
import os
import datetime


def filter_trips(DB_NAME,DB_USER,DB_PASSWORD,DB_HOST,DB_PORT, date, polygon):

    """
    Filters GTFS trips to be Jerusalem poly only
    """
    filtered_trips_table_name = "'gtfs_data.filtered_trips_gtfs_jeru_{date}'"

    conn = psycopg2.connect(f"dbname= {DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port = {DB_PORT}")
    cur = conn.cursor()

    # Check if the table exists
    cur.execute(
        f"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'gtfs_data.filtered_trips_gtfs_jeru_{date}');"
    )
    table_exists = cur.fetchone()[0]

    # If the table doesn't exist, create it
    if table_exists:
        print("table already exist")

    cur.execute(f"DROP TABLE IF EXISTS gtfs_data.filtered_trips_gtfs_jeru_{date}")
    conn.commit()

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
        print(f"Table 'gtfs_data.gtfs_shape_geoms_{date}' does not exist.")
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

    # Query to get the trips with the filtered shape_ids
    cur.execute(f"SELECT * FROM gtfs_data.gtfs_trips_{date} WHERE shape_id IN %s;", (tuple(filtered_shape_ids),))

    # Fetch the filtered trips
    filtered_trips = cur.fetchall()

    # Create a new table and insert the filtered trips
    cur.execute(
        f"CREATE TABLE gtfs_data.filtered_trips_gtfs_jeru_{date} AS SELECT * FROM gtfs_data.gtfs_trips_{date} WHERE shape_id IN %s;",
        (tuple(filtered_shape_ids),))

    # Commit the changes and close the connection
    conn.commit()
    cur.close()
    conn.close()

    return filtered_trips



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
    cursor.execute(f"SELECT tablename FROM pg_tables WHERE tablename LIKE 'gtfs%{date_str}';")
    tables = cursor.fetchall()
    print(tables)

    for table in tables:
        table_name = table[0]

        # Check if the table exists
        cursor.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE  table_schema = 'gtfs_data' 
                AND    table_name   = '{table_name}'
            );
        """)
        table_exists = cursor.fetchone()[0]
        if not table_exists:
            print(f"Table '{table_name}' does not exist.")
            continue

        # Delete the table
        cursor.execute(f"DROP TABLE IF EXISTS gtfs_data.{table_name};")
        conn.commit()

    # Close the connection and cursor
    cursor.close()
    conn.close()


def importGtfsToSQLParams(port, dirname, host, db, user, password, db_type, date):
    dir_name = 'C:\\DEV\\SRC\\GLTRANSIT\\GTFSSiteUpload\\'
    os.chdir(dir_name)

    if port is None:
        print("port is none")
        setenv_command = f"set PGPASSWORD={password}"
        drop_and_create_command = f"type gtfs_tables_mot.sql | psql -h {host} -U {db} -d {user} -v date={date}"
        insert_command = f"python import_gtfs_to_sql.py {dirname} {date} | psql -h {host} -U {db} -d {user}"
        make_spatial_command = f"type gtfs_tables_makespatial_mot.sql | psql -h {host} -U {db} -d {user} -v date={date}"
        create_index_command = f"type gtfs_tables_makeindexes_mot.sql | psql -h {host} -U {db} -d {user} -v date={date}"
        vacuum_command = f"type vacuumer_mot.sql | psql -h {host} -U {db} -d {user} -v date={date}"
        create_populate_routes_table_command = f"type gtfs_tables_makepopulated_params.sql | psql -h {host} -U {db} -d {user} -v date={date}"
        move_command = f"type move_tables_params.sql | psql -h {host} -p {port} -U {db} -d {user} -v date={date}"
        dtop_after_move_command = f"type drop_after_move_params.sql | psql -h {host} -p {port} -U {db} -d {user} -v date={date}"
    else:
        setenv_command = f"set PGPASSWORD={password}"
        drop_and_create_command = f"type gtfs_tables_mot_params.sql | psql -h {host} -p {port} -U {db} -d {user} -v date={date}"
        insert_command = f"python import_gtfs_to_sql.py {dirname} {date} | psql -h {host} -p {port} -U {db} -d {user}"
        make_spatial_command = f"type gtfs_tables_makespatial_mot_params.sql | psql -h {host} -p {port} -U {db} -d {user} -v date={date}"
        # rows for checking issue - Doron
        drop_rows_command = f"psql -h {host} -p {port} -U {db} -d {user} -c \"DELETE FROM gtfs_trips_{date} WHERE route_id NOT IN (SELECT route_id FROM gtfs_routes_{date});\""

        create_index_command = f"type gtfs_tables_makeindexes_mot_params.sql | psql -h {host} -p {port} -U {db} -d {user} -v date={date}"
        vacuum_command = f"type vacuumer_mot_params.sql | psql -h {host} -p {port} -U {db} -d {user} -v date={date}"
        create_populate_routes_table_command = f"type gtfs_tables_makepopulated_params.sql | psql -h {host} -p {port} -U {db} -d {user} -v date={date}"
        move_command = f"type move_tables_params.sql | psql -h {host} -p {port} -U {db} -d {user} -v date={date}"
        dtop_after_move_command = f"type drop_after_move_params.sql | psql -h {host} -p {port} -U {db} -d {user} -v date={date}"

    os.system(setenv_command + "&" + drop_and_create_command)
    os.system(setenv_command + "&" + insert_command)
    if db_type == "gis":
        os.system(setenv_command + "&" + make_spatial_command)
        print("done")
    # line added for checking
    os.system(setenv_command + "&" + drop_rows_command)
    os.system(setenv_command + "&" + create_index_command)
    os.system(setenv_command + "&" + vacuum_command)
    os.system(setenv_command + "&" + create_populate_routes_table_command)
    os.system(setenv_command + "&" + move_command)
    os.system(setenv_command + "&" + dtop_after_move_command)
