DROP TABLE gtfs_populated_routes_:date;
BEGIN;

-- Creating a populated gtfs routes table.
-- 1. Create table.
CREATE TABLE gtfs_populated_routes_:date (
    route_id text COLLATE pg_catalog."default" NOT NULL,
    agency_id text COLLATE pg_catalog."default",
    route_short_name text COLLATE pg_catalog."default" DEFAULT '' :: text,
    route_long_name text COLLATE pg_catalog."default" DEFAULT '' :: text,
    route_desc text COLLATE pg_catalog."default",
    route_type integer,
    geom_id text COLLATE pg_catalog."default",
    calendar_id text COLLATE pg_catalog."default",
    first_trip_id text COLLATE pg_catalog."default",
    first_trip_stops_ids text [] COLLATE pg_catalog."default",
    CONSTRAINT populated_routes_id_pkey_:date PRIMARY KEY (route_id),
    CONSTRAINT populated_routes_agencyf_key_:date FOREIGN KEY (agency_id) REFERENCES gtfs_agency_:date (agency_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
    CONSTRAINT populated_routes_calendarf_key_:date FOREIGN KEY (calendar_id) REFERENCES gtfs_calendar_:date (service_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
--    CONSTRAINT populated_routes_shapef_key_:date FOREIGN KEY (geom_id) REFERENCES gtfs_shape_geoms_:date (shape_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
);
--    CONSTRAINT populated_routes_shapef_key_:date FOREIGN KEY (geom_id) REFERENCES gtfs_shape_geoms_:date (shape_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
--);

-- 2. Populate with basic route data and shape&calendar fks.
INSERT INTO
    gtfs_populated_routes_:date (
        SELECT
            gtfs_routes_:date.route_id,
            gtfs_routes_:date.agency_id,
            gtfs_routes_:date.route_short_name,
            gtfs_routes_:date.route_long_name,
            gtfs_routes_:date.route_desc,
            gtfs_routes_:date.route_type,
            top_trip.shape_id,
            top_trip.calendar_id
        from
            gtfs_routes_:date
            right JOIN (
                select
                    distinct on (route_id) route_id,
                    shape_id,
                    service_id as calendar_id
                from
                    gtfs_trips_:date
            ) as top_trip on gtfs_routes_:date.route_id = top_trip.route_id
    );

-- 3. Find the first trip.
UPDATE
    gtfs_populated_routes_:date as basePopulate
SET
    first_trip_id = tripsJoin.trip_id
FROM
    (
        SELECT
            trip_id,
            route_id
        FROM
            gtfs_trips_:date
        order by
            trip_id
    ) as tripsJoin
WHERE
    tripsJoin.route_id = basePopulate.route_id; 
COMMIT;

-- 4. For each route find its stops.
UPDATE
    gtfs_populated_routes_:date
SET
    first_trip_stops_ids = (
        select
            stops
        from
            (
                SELECT
                    pr.route_desc,
                    array_agg(stops.stop_id) as stops_ids
                FROM
                    gtfs_populated_routes_:date pr
                    LEFT JOIN gtfs_stop_times_:date stops ON stops.trip_id = pr.first_trip_id
                GROUP BY
                    pr.route_desc
            ) AS stops_trips(ids, stops)
        where
            ids = gtfs_populated_routes_:date.route_desc
    );

-- 5. Add the geom_json field to the geoms table.
ALTER TABLE
    gtfs_shape_geoms_:date
ADD
    geom_json text;

update
    gtfs_shape_geoms_:date
set
    geom_json = ST_AsGeoJSON(the_geom);

COMMIT;