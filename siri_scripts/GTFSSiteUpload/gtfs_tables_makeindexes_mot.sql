

begin;

ALTER TABLE gtfs_agency ADD CONSTRAINT agency_name_pkey 
      PRIMARY KEY (agency_id);
ALTER TABLE gtfs_agency 
      ALTER COLUMN agency_name SET NOT NULL;
ALTER TABLE gtfs_agency 
      ALTER COLUMN agency_url SET NOT NULL;
ALTER TABLE gtfs_agency 
      ALTER COLUMN agency_timezone SET NOT NULL;

ALTER TABLE gtfs_stops ADD CONSTRAINT stops_id_pkey
      PRIMARY KEY (stop_id);
ALTER TABLE gtfs_stops 
      ALTER COLUMN stop_name SET NOT NULL;


ALTER TABLE gtfs_routes ADD CONSTRAINT routes_id_pkey
      PRIMARY KEY (route_id);
ALTER TABLE gtfs_routes ADD CONSTRAINT routes_agency_fkey
      FOREIGN KEY (agency_id) 
      REFERENCES gtfs_agency(agency_id);

ALTER TABLE gtfs_calendar ADD CONSTRAINT calendar_sid_pkey
      PRIMARY KEY (service_id);
ALTER TABLE gtfs_calendar 
      ALTER COLUMN monday SET NOT NULL;
ALTER TABLE gtfs_calendar 
      ALTER COLUMN tuesday SET NOT NULL;
ALTER TABLE gtfs_calendar 
      ALTER COLUMN wednesday SET NOT NULL;
ALTER TABLE gtfs_calendar 
      ALTER COLUMN thursday SET NOT NULL;
ALTER TABLE gtfs_calendar 
      ALTER COLUMN friday SET NOT NULL;
ALTER TABLE gtfs_calendar 
      ALTER COLUMN saturday SET NOT NULL;
ALTER TABLE gtfs_calendar 
      ALTER COLUMN sunday SET NOT NULL;
ALTER TABLE gtfs_calendar 
      ALTER COLUMN start_date SET NOT NULL;
ALTER TABLE gtfs_calendar 
      ALTER COLUMN end_date SET NOT NULL;

ALTER TABLE gtfs_shapes  
      ALTER COLUMN shape_id SET NOT NULL;
ALTER TABLE gtfs_shapes 
      ALTER COLUMN shape_pt_lat SET NOT NULL;
ALTER TABLE gtfs_shapes 
      ALTER COLUMN shape_pt_lon SET NOT NULL;
ALTER TABLE gtfs_shapes 
      ALTER COLUMN shape_pt_sequence SET NOT NULL;

ALTER TABLE gtfs_trips ADD CONSTRAINT trip_id_pkey
      PRIMARY KEY (trip_id);
ALTER TABLE gtfs_trips ADD CONSTRAINT trip_rid_fkey
      FOREIGN KEY (route_id)
      REFERENCES gtfs_routes(route_id);
--ALTER TABLE gtfs_trips ADD CONSTRAINT trip_sid_fkey
--      FOREIGN KEY (service_id)
--      REFERENCES gtfs_calendar(service_id);
ALTER TABLE gtfs_trips
      ALTER COLUMN direction_id SET NOT NULL;

ALTER TABLE gtfs_stop_times ADD CONSTRAINT times_tid_fkey
      FOREIGN KEY (trip_id)
      REFERENCES gtfs_trips(trip_id);
ALTER TABLE gtfs_stop_times ADD CONSTRAINT times_sid_fkey
      FOREIGN KEY (stop_id)
      REFERENCES gtfs_stops(stop_id);
ALTER TABLE gtfs_stop_times ADD CONSTRAINT times_arrtime_check
      CHECK (arrival_time LIKE '__:__:__');
ALTER TABLE gtfs_stop_times ADD CONSTRAINT times_deptime_check
      CHECK (departure_time LIKE '__:__:__');
ALTER TABLE gtfs_stop_times 
      ALTER COLUMN stop_sequence SET NOT NULL;

create index arr_time_index on gtfs_stop_times(arrival_time_seconds);
create index dep_time_index on gtfs_stop_times(departure_time_seconds);
create index stop_seq_index on gtfs_stop_times(trip_id,stop_sequence);
create index shapeid_index on gtfs_shapes(shape_id);

commit;
