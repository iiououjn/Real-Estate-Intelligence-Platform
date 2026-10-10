-- Which bedroom types actually transact (last 365 days) and which ones people look at / save in the marketplace.
DELETE FROM mart_bedroom_popularity;
INSERT INTO mart_bedroom_popularity (area_name, bedrooms, txn_count, share_of_area_txn_pct, median_psf, median_price_aed,
                                     event_views, event_saves)
WITH sb AS (
  SELECT MAX(d.day_num) AS max_day FROM fact_transactions f JOIN dim_date d ON d.date_key = f.date_key
),
txn AS (
  SELECT a.area_name, p.bedrooms, COUNT(*) AS txn_count, MEDIAN(f.price_per_sqft) AS median_psf, MEDIAN(f.amount_aed) AS median_price_aed
  FROM fact_transactions f
  JOIN dim_date d ON d.date_key = f.date_key
  JOIN dim_area a ON a.area_key = f.area_key
  JOIN dim_property p ON p.property_key = f.property_key
  CROSS JOIN sb
  WHERE f.trans_group = 'Sales' AND p.bedrooms >= 0 AND f.price_per_sqft IS NOT NULL AND d.day_num > sb.max_day - 365
  GROUP BY a.area_name, p.bedrooms
),
tot AS (SELECT area_name, SUM(txn_count) AS total FROM txn GROUP BY area_name),
interest AS (
  SELECT dld_area_name AS area_name, bedrooms,
         SUM(CASE WHEN event_type = 'property_viewed' THEN 1 ELSE 0 END) AS views,
         SUM(CASE WHEN event_type = 'listing_saved' THEN 1 ELSE 0 END) AS saves
  FROM fact_events WHERE bedrooms IS NOT NULL GROUP BY dld_area_name, bedrooms
)
SELECT t.area_name, t.bedrooms, t.txn_count, 100.0 * t.txn_count / tot.total, t.median_psf, t.median_price_aed,
       COALESCE(i.views, 0), COALESCE(i.saves, 0)
FROM txn t
JOIN tot ON tot.area_name = t.area_name
LEFT JOIN interest i ON i.area_name = t.area_name AND i.bedrooms = t.bedrooms;
