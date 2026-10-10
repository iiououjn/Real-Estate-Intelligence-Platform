-- Incremental + idempotent: only events not already in fact_events are inserted, so replays are safe.
-- Kinesis is at-least-once, so duplicates across batches are collapsed with ROW_NUMBER().
-- Marketplace community names ("Dubai Marina") are mapped to DLD area names ("Marsa Dubai") via area_alias.
INSERT INTO fact_events (event_id, event_ts, date_key, event_hour, event_type, session_id, user_id, listing_id, property_type,
                         area_name, dld_area_name, bedrooms, price_aed, size_sqft, search_query, agent_id, device, ingested_at)
SELECT s.event_id, s.event_ts, d.date_key, s.event_hour, s.event_type, s.session_id, s.user_id, s.listing_id, s.property_type,
       s.area_name, COALESCE(al.area_name, s.area_name), s.bedrooms, s.price_aed, s.size_sqft, s.search_query, s.agent_id,
       s.device, s.ingested_at
FROM (
  SELECT e.*, ROW_NUMBER() OVER (PARTITION BY e.event_id ORDER BY e.ingested_at) AS rn FROM stg_events e
) s
JOIN dim_date d ON d.full_date = s.event_date
LEFT JOIN area_alias al ON al.alias = LOWER(s.area_name)
WHERE s.rn = 1
  AND NOT EXISTS (SELECT 1 FROM fact_events f WHERE f.event_id = s.event_id);
