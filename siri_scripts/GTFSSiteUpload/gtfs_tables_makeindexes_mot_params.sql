

begin;

ALTER TABLE gtfs_agency_:date ADD CONSTRAINT agency_name_pkey_:date
      PRIMARY KEY (agency_id);
ALTER TABLE gtfs_agency_:date
      ALTER COLUMN agency_name SET NOT NULL;
ALTER TABLE gtfs_agency_:date
      ALTER COLUMN agency_url SET NOT NULL;
ALTER TABLE gtfs_agency_:date
      ALTER COLUMN agency_timezone SET NOT NULL;

ALTER TABLE gtfs_stops_:date ADD CONSTRAINT stops_id_pkey_:date
      PRIMARY KEY (stop_id);
ALTER TABLE gtfs_stops_:date
      ALTER COLUMN stop_name SET NOT NULL;


ALTER TABLE gtfs_routes_:date ADD CONSTRAINT routes_id_pkey_:date
      PRIMARY KEY (route_id);
ALTER TABLE gtfs_routes_:date ADD CONSTRAINT routes_agencyf_key_:date
      FOREIGN KEY (agency_id) 
      REFERENCES gtfs_agency_:date(agency_id);

ALTER TABLE gtfs_calendar_:date ADD CONSTRAINT calendar_sid_pkey_:date
      PRIMARY KEY (service_id);
ALTER TABLE gtfs_calendar_:date
      ALTER COLUMN monday SET NOT NULL;
ALTER TABLE gtfs_calendar_:date
      ALTER COLUMN tuesday SET NOT NULL;
ALTER TABLE gtfs_calendar_:date
      ALTER COLUMN wednesday SET NOT NULL;
ALTER TABLE gtfs_calendar_:date
      ALTER COLUMN thursday SET NOT NULL;
ALTER TABLE gtfs_calendar_:date
      ALTER COLUMN friday SET NOT NULL;
ALTER TABLE gtfs_calendar_:date
      ALTER COLUMN saturday SET NOT NULL;
ALTER TABLE gtfs_calendar_:date
      ALTER COLUMN sunday SET NOT NULL;
ALTER TABLE gtfs_calendar_:date
      ALTER COLUMN start_date SET NOT NULL;
ALTER TABLE gtfs_calendar_:date
      ALTER COLUMN end_date SET NOT NULL;

ALTER TABLE gtfs_shapes_:date
      ALTER COLUMN shape_id SET NOT NULL;
ALTER TABLE gtfs_shapes_:date
      ALTER COLUMN shape_pt_lat SET NOT NULL;
ALTER TABLE gtfs_shapes_:date
      ALTER COLUMN shape_pt_lon SET NOT NULL;
ALTER TABLE gtfs_shapes_:date
      ALTER COLUMN shape_pt_sequence SET NOT NULL;

--ALTER TABLE gtfs_trips_:date ADD CONSTRAINT trip_id_pkey_:date
--      PRIMARY KEY (trip_id);
--ALTER TABLE gtfs_trips_:date ADD CONSTRAINT trip_rid_fkey_:date
 --     FOREIGN KEY (route_id)
 --     REFERENCES gtfs_routes_:date(route_id);
--ALTER TABLE gtfs_trips ADD CONSTRAINT trip_sid_fkey
--      FOREIGN KEY (service_id)
--      REFERENCES gtfs_calendar(service_id);
ALTER TABLE gtfs_trips_:date
      ALTER COLUMN direction_id SET NOT NULL;

--ALTER TABLE gtfs_stop_times_:date ADD CONSTRAINT times_tid_fkey_:date
--      FOREIGN KEY (trip_id)
--      REFERENCES gtfs_trips_:date(trip_id);
ALTER TABLE gtfs_stop_times_:date ADD CONSTRAINT times_sid_fkey_:date
      FOREIGN KEY (stop_id)
      REFERENCES gtfs_stops_:date(stop_id);
ALTER TABLE gtfs_stop_times_:date ADD CONSTRAINT times_arrtime_check_:date
      CHECK (arrival_time LIKE '__:__:__');
ALTER TABLE gtfs_stop_times_:date ADD CONSTRAINT times_deptime_check_:date
      CHECK (departure_time LIKE '__:__:__');
ALTER TABLE gtfs_stop_times_:date
      ALTER COLUMN stop_sequence SET NOT NULL;

create index arr_time_index_:date on gtfs_stop_times_:date(arrival_time_seconds);
create index dep_time_index_:date on gtfs_stop_times_:date(departure_time_seconds);
create index stop_seq_index_:date on gtfs_stop_times_:date(trip_id,stop_sequence);
create index shapeid_index_:date on gtfs_shapes_:date(shape_id);

commit;
