DROP MATERIALIZED VIEW IF EXISTS trips_all_status;
CREATE  MATERIALIZED VIEW trips_all_status AS
select trips_all_status.id,
trips_all_status.route_id,
trips_all_status.route_short_name,trips_all_status.route_long_name,
trips_all_status.siri_trip_id,
trips_all_status.origin_aimed_departure_time,
trips_all_status.vehicle_number,
siri_selection.geom,
siri_selection.recordedattime,
trips_all_status.status_code,
case
when trips_all_status.status_code=9 then 'Late, Bunching & Low-speed'
when trips_all_status.status_code=8 then 'Bunching & Low-speed'
when trips_all_status.status_code=7 then 'Late & Bunching'
when trips_all_status.status_code=6 then 'Late & Low-speed'
when trips_all_status.status_code=5 then 'Late & Deviation'
when trips_all_status.status_code=4 then 'Late'
when trips_all_status.status_code=3 then 'Bunching'
when trips_all_status.status_code=2 then 'Low-speed'
when trips_all_status.status_code=1 then 'Deviation'
else 'Unknown' end as status
from
(select trips_all_monitors.id,trips_all_monitors.route_id,
trips_all_monitors.route_short_name,trips_all_monitors.route_long_name,
trips_all_monitors.siri_trip_id,trips_all_monitors.origin_aimed_departure_time,
trips_all_monitors.vehicle_number,
case
--- 9 = late + bunching + low_speed
when
trips_all_monitors.late_status=true
and  trips_all_monitors.bunching_status=true
and trips_all_monitors.low_sp_status then 9
-- 8 = bunching + low_speed
when
 trips_all_monitors.bunching_status=true
and trips_all_monitors.low_sp_status then 8
-- 7 = bunching + late
when
trips_all_monitors.late_status=true
and  trips_all_monitors.bunching_status=true then 7
-- 6 = low_speed + late
when
trips_all_monitors.late_status=true
and trips_all_monitors.low_sp_status then 6
-- 5 = deviation + late
when
trips_all_monitors.late_status=true
and trips_all_monitors.dev_status then 5
-- 4 =  late
when
trips_all_monitors.late_status=true  then 4
-- 3 = bunching
when
trips_all_monitors.bunching_status=true  then 3
-- 2 = low_speed
when
trips_all_monitors.low_sp_status=true  then 2
-- 1 = deviation
when
trips_all_monitors.dev_status=true  then 1
else null
end as status_code,
late_status,dev_status,low_sp_status,bunching_status
from
(select perf.id,perf.route_id,
perf.route_short_name,
perf.route_long_name,
perf.siri_trip_id,
perf.origin_aimed_departure_time,
perf.vehicle_number,
case
 when perf.trip_performance_status=2 then true
 when perf.trip_performance_status=3 then true
else false end as late_status,
dev.status as dev_status,
low_sp.status as low_sp_status,
bunching.status as bunching_status
from
public.trip_perform_monitor_20230708 perf
inner join public.trip_deviation_monitor_20230708 dev
on perf.id=dev.trip_master_id
inner join public.trip_low_speed_monitor_20230708 low_sp
on perf.id=low_sp.trip_master_id
inner join public.trip_bunching_monitor_20230708 bunching
on perf.id=bunching.trip_master_id
-- where perf.activated=true and perf.de_activated=false
) trips_all_monitors
where
trips_all_monitors.late_status is not false or
trips_all_monitors.dev_status is not false or
trips_all_monitors.low_sp_status is not false or
trips_all_monitors.bunching_status is not false
limit 10
) trips_all_status
inner JOIN LATERAL (
SELECT "unique_id", "geom", "created_at", "recordedattime"
FROM
(
(SELECT  1 as idx,"unique_id","geom","created_at","recordedattime"
FROM public.siri_sm_res_monitor_jerusalem
WHERE
datedvehiclejourneyref<>'0'
AND datedvehiclejourneyref = trips_all_status.siri_trip_id
AND lineref = trips_all_status.route_id
AND originaimeddeparturetime= trips_all_status.origin_aimed_departure_time
-- AND recordedattime>=CURRENT_TIMESTAMP AT TIME ZONE 'Israel' - INTERVAL '30 minutes'
ORDER BY recordedattime DESC
LIMIT 1)
UNION
(SELECT  2 as idx,"unique_id","geom","created_at","recordedattime"
FROM public.siri_sm_res_monitor_jerusalem
WHERE
datedvehiclejourneyref='0'
AND lineref = trips_all_status.route_id
AND originaimeddeparturetime= trips_all_status.origin_aimed_departure_time
-- AND recordedattime>=CURRENT_TIMESTAMP AT TIME ZONE 'Israel' - INTERVAL '30 minutes'
ORDER BY recordedattime DESC
LIMIT 1)
) joined_data
order by idx
LIMIT 1
) siri_selection on true;
-- Create a unique index on the materialized view
CREATE UNIQUE INDEX idx_id_mv
ON public.trips_all_status (id);
-- creating indexes
CREATE INDEX idx_route_id_mv ON trips_all_status (route_id);
CREATE INDEX idx_status_code_mv ON trips_all_status (status_code);
CREATE INDEX idx_departure_time_code_mv ON trips_all_status (origin_aimed_departure_time);

REFRESH MATERIALIZED VIEW CONCURRENTLY trips_all_status;

select * from trips_all_status

