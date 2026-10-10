-- Demand by community: 30-day marketplace engagement (from the streaming path) next to 90-day registered sales.
-- engagement_score is a transparent weighted count (view=1, save=3, agent contact=5), not a model.
DELETE FROM mart_area_demand;
INSERT INTO mart_area_demand (area_name, searches_30d, views_30d, saves_30d, contacts_30d, sessions_30d, contact_rate_pct,
                              engagement_score, demand_rank, txn_count_90d, median_psf_90d)
WITH eb AS (
  SELECT MAX(d.day_num) AS max_day FROM fact_events e JOIN dim_date d ON d.date_key = e.date_key
),
sb AS (
  SELECT MAX(d.day_num) AS max_day FROM fact_transactions f JOIN dim_date d ON d.date_key = f.date_key
),
ev AS (
  SELECT e.dld_area_name AS area_name,
         SUM(CASE WHEN e.event_type = 'property_searched' THEN 1 ELSE 0 END) AS searches,
         SUM(CASE WHEN e.event_type = 'property_viewed' THEN 1 ELSE 0 END) AS views,
         SUM(CASE WHEN e.event_type = 'listing_saved' THEN 1 ELSE 0 END) AS saves,
         SUM(CASE WHEN e.event_type = 'agent_contacted' THEN 1 ELSE 0 END) AS contacts,
         COUNT(DISTINCT e.session_id) AS sessions
  FROM fact_events e
  JOIN dim_date d ON d.date_key = e.date_key
  CROSS JOIN eb
  WHERE d.day_num > eb.max_day - 30 AND e.dld_area_name IS NOT NULL
  GROUP BY e.dld_area_name
),
tx AS (
  SELECT a.area_name, COUNT(*) AS txn_count, MEDIAN(f.price_per_sqft) AS median_psf
  FROM fact_transactions f
  JOIN dim_date d ON d.date_key = f.date_key
  JOIN dim_area a ON a.area_key = f.area_key
  CROSS JOIN sb
  WHERE f.trans_group = 'Sales' AND f.price_per_sqft IS NOT NULL AND d.day_num > sb.max_day - 90
  GROUP BY a.area_name
),
joined AS (
  SELECT a.area_name, COALESCE(ev.searches, 0) AS searches, COALESCE(ev.views, 0) AS views, COALESCE(ev.saves, 0) AS saves,
         COALESCE(ev.contacts, 0) AS contacts, COALESCE(ev.sessions, 0) AS sessions,
         CASE WHEN COALESCE(ev.views, 0) > 0 THEN 100.0 * ev.contacts / ev.views END AS contact_rate_pct,
         COALESCE(ev.views, 0) + 3.0 * COALESCE(ev.saves, 0) + 5.0 * COALESCE(ev.contacts, 0) AS engagement_score,
         tx.txn_count, tx.median_psf
  FROM dim_area a
  LEFT JOIN ev ON ev.area_name = a.area_name
  LEFT JOIN tx ON tx.area_name = a.area_name
  WHERE ev.area_name IS NOT NULL OR tx.area_name IS NOT NULL
)
SELECT area_name, searches, views, saves, contacts, sessions, contact_rate_pct, engagement_score,
       RANK() OVER (ORDER BY engagement_score DESC), COALESCE(txn_count, 0), median_psf
FROM joined;
