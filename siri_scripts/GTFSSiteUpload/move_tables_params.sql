begin;
--- drop tables
DROP TABLE IF EXISTS gtfs_data.gtfs_agency_:date cascade;
DROP TABLE IF EXISTS gtfs_data.gtfs_stops_:date cascade;
DROP TABLE IF EXISTS gtfs_data.gtfs_routes_:date cascade;
DROP TABLE IF EXISTS gtfs_data.gtfs_calendar_:date cascade;
DROP TABLE IF EXISTS gtfs_data.gtfs_shapes_:date cascade;
DROP TABLE IF EXISTS gtfs_data.gtfs_shape_geoms_:date cascade;
DROP TABLE IF EXISTS gtfs_data.gtfs_populated_routes_:date cascade;
DROP TABLE IF EXISTS gtfs_data.gtfs_trips_:date cascade;
DROP TABLE IF EXISTS gtfs_data.gtfs_stop_times_:date cascade;
commit;

begin;
create table gtfs_data.gtfs_agency_:date as select * from gtfs_agency_:date;
create table gtfs_data.gtfs_calendar_:date as select * from gtfs_calendar_:date;
create table gtfs_data.gtfs_routes_:date as select * from gtfs_routes_:date;
create table gtfs_data.gtfs_populated_routes_:date as select * from gtfs_populated_routes_:date;
create table gtfs_data.gtfs_shape_geoms_:date as select * from gtfs_shape_geoms_:date;
create table gtfs_data.gtfs_shapes_:date as select * from gtfs_shapes_:date;
create table gtfs_data.gtfs_stop_times_:date as select * from gtfs_stop_times_:date;
create table gtfs_data.gtfs_stops_:date as select * from gtfs_stops_:date;
create table gtfs_data.gtfs_trips_:date as select * from gtfs_trips_:date;
commit;

