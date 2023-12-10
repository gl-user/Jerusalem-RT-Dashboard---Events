-- Add spatial support for PostGIS databases only

-- Drop everything first
DROP TABLE gtfs_shape_geoms_:date CASCADE;

BEGIN;

-- On the first run on new database only!! - Install postgis extension
-- CREATE EXTENSION postgis;
-- CREATE EXTENSION postgis_topology;

-- Add the_geom column to the gtfs_stops table - a 2D point geometry
--select '''||gtfs_stops_:date||''' as con;
--t_string_combined = CONCAT('',gtfs_stops_:date,'')
\set datestr gtfs_stops_:date
SELECT AddGeometryColumn(:'datestr' , 'the_geom', 4326, 'POINT', 2);
--SELECT AddGeometryColumn(gtfs_stops_:date, 'the_geom', 4326, 'POINT', 2);

-- Update the the_geom column
UPDATE gtfs_stops_:date SET the_geom = ST_SetSRID(ST_MakePoint(stop_lon, stop_lat), 4326);

-- Create spatial index
CREATE INDEX gtfs_stops_the_geom_gist_:date ON gtfs_stops_:date using gist ("the_geom");

-- Create new table to store the shape geometries
CREATE TABLE gtfs_shape_geoms_:date (
  shape_id    text
);

-- Add the_geom column to the gtfs_shape_geoms table - a 2D linestring geometry
\set datestr1 gtfs_shape_geoms_:date
SELECT AddGeometryColumn(:'datestr1', 'the_geom', 4326, 'LINESTRING', 2);

-- Populate gtfs_shape_geoms
INSERT INTO gtfs_shape_geoms_:date
SELECT shape.shape_id, ST_SetSRID(ST_MakeLine(shape.the_geom), 4326) As new_geom
  FROM (
    SELECT shape_id, ST_MakePoint(shape_pt_lon, shape_pt_lat) AS the_geom
    FROM gtfs_shapes_:date
    ORDER BY shape_id, shape_pt_sequence
  ) AS shape
GROUP BY shape.shape_id;

-- Create spatial index
CREATE INDEX gtfs_shape_geoms_the_geom_gist_:date ON gtfs_shape_geoms_:date using gist ("the_geom");

COMMIT;