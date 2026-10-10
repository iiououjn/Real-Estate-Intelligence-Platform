-- Hourly event counts (UTC) by type and community: the "real-time" dashboard reads this after each micro-batch load.
DELETE FROM mart_realtime_hourly;
INSERT INTO mart_realtime_hourly (event_date, event_hour, event_type, dld_area_name, events, unique_sessions)
SELECT d.full_date, e.event_hour, e.event_type, e.dld_area_name, COUNT(*), COUNT(DISTINCT e.session_id)
FROM fact_events e
JOIN dim_date d ON d.date_key = e.date_key
GROUP BY d.full_date, e.event_hour, e.event_type, e.dld_area_name;
