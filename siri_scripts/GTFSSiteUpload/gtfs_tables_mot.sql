begin;
drop table gtfs_agency cascade;
drop table gtfs_stops cascade;
drop table gtfs_routes cascade;
drop table gtfs_calendar cascade;
drop table gtfs_shapes cascade;
drop table gtfs_trips cascade;
drop table gtfs_stop_times cascade;
commit;

begin;

create table gtfs_agency (
  agency_id    text ,--PRIMARY KEY,
  agency_name  text ,--NOT NULL,
  agency_url   text ,--NOT NULL,
  agency_timezone    text ,--NOT NULL,
  agency_lang  text,
  agency_phone text,
  agency_fare_url text
);

create table gtfs_stops (
  stop_id    text ,--PRIMARY KEY,
  stop_name  text , --NOT NULL,
  stop_desc  text,
  stop_lat   double precision,
  stop_lon   double precision,
  zone_id    text,
  stop_url   text,
  stop_code  text,

  -- new
  stop_street text,
  stop_city   text,
  stop_region text,
  stop_postcode text,
  stop_country text,

  -- unofficial features

  location_type int, --FOREIGN KEY REFERENCES gtfs_location_types(location_type)
  parent_station text, --FOREIGN KEY REFERENCES gtfs_stops(stop_id)
  stop_timezone text,
  wheelchair_boarding int --FOREIGN KEY REFERENCES gtfs_wheelchair_boardings(wheelchair_boarding)
  -- Unofficial fields
  ,
  direction text,
  position text
);

create table gtfs_routes (
  route_id    text ,--PRIMARY KEY,
  agency_id   text , --REFERENCES gtfs_agency(agency_id),
  route_short_name  text DEFAULT '',
  route_long_name   text DEFAULT '',
  route_desc  text,
  route_type  int , --REFERENCES gtfs_route_types(route_type),
  route_url   text,
  route_color text,
  route_text_color  text
);

-- CREATE INDEX gst_trip_id_stop_sequence ON gtfs_stop_times (trip_id, stop_sequence);

create table gtfs_calendar (
  service_id   text ,--PRIMARY KEY,
  monday int , --NOT NULL,
  tuesday int , --NOT NULL,
  wednesday    int , --NOT NULL,
  thursday     int , --NOT NULL,
  friday int , --NOT NULL,
  saturday     int , --NOT NULL,
  sunday int , --NOT NULL,
  start_date   date , --NOT NULL,
  end_date     date  --NOT NULL
);

create table gtfs_shapes (
  shape_id    text , --NOT NULL,
  shape_pt_lat double precision , --NOT NULL,
  shape_pt_lon double precision , --NOT NULL,
  shape_pt_sequence int , --NOT NULL,
  shape_dist_traveled double precision
);

create table gtfs_trips (
  route_id text , --REFERENCES gtfs_routes(route_id),
  service_id    text , --REFERENCES gtfs_calendar(service_id),
  trip_id text ,--PRIMARY KEY,
  trip_headsign text,
  direction_id  int , --REFERENCES gtfs_directions(direction_id),
  block_id text,
  shape_id text,
  trip_short_name text,
  -- unofficial features
  trip_type text
);

create table gtfs_stop_times (
  trip_id text , --REFERENCES gtfs_trips(trip_id),
  arrival_time text, -- CHECK (arrival_time LIKE '__:__:__'),
  departure_time text, -- CHECK (departure_time LIKE '__:__:__'),
  stop_id text , --REFERENCES gtfs_stops(stop_id),
  stop_sequence int , --NOT NULL,
  stop_headsign text,
  pickup_type   int , --REFERENCES gtfs_pickup_dropoff_types(type_id),
  drop_off_type int , --REFERENCES gtfs_pickup_dropoff_types(type_id),
  shape_dist_traveled double precision

  -- unofficial features
  ,
  timepoint int

  -- the following are not in the spec
  ,
  arrival_time_seconds int,
  departure_time_seconds int

);

--create index arr_time_index on gtfs_stop_times(arrival_time_seconds);
--create index dep_time_index on gtfs_stop_times(departure_time_seconds);

commit;
