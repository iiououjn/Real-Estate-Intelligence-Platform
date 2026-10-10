-- Paste each block into Metabase as a "New > SQL query" question, then arrange them on one dashboard.
-- All read the marts, so they are cheap and identical on Redshift / DuckDB.

-- Q1  Median AED/sq.ft. over time (line; filter by area)
SELECT month_start, area_name, median_psf FROM mart_price_trends_monthly
WHERE property_type = 'Unit' AND reg_type = 'Existing Properties' ORDER BY month_start;

-- Q2  Monthly sales volume and value (bar)
SELECT month_start, SUM(txn_count) AS transactions, SUM(total_value_aed) AS value_aed
FROM mart_price_trends_monthly GROUP BY month_start ORDER BY month_start;

-- Q3  Biggest 12-month price movers (table)
SELECT area_name, property_type, txn_count, median_psf, ROUND(yoy_change_pct, 1) AS yoy_pct
FROM mart_price_trends_monthly
WHERE month_start = (SELECT MAX(month_start) FROM mart_price_trends_monthly) AND yoy_change_pct IS NOT NULL AND txn_count >= 10
ORDER BY yoy_pct DESC;

-- Q4  Rent vs buy: gross yield and price/rent ratio by community (bar)
SELECT area_name, property_type, ROUND(gross_yield_pct, 2) AS gross_yield_pct,
       ROUND(median_sale_psf / median_annual_rent_psf, 1) AS years_of_rent_to_buy,
       median_sale_psf, median_annual_rent_psf
FROM mart_rental_yield WHERE gross_yield_pct IS NOT NULL ORDER BY gross_yield_pct DESC;

-- Q5  Popular bedroom types (stacked bar by area)
SELECT area_name, bedrooms, txn_count, ROUND(share_of_area_txn_pct, 1) AS share_pct
FROM mart_bedroom_popularity ORDER BY area_name, bedrooms;

-- Q6  Community demand: marketplace engagement vs registered sales (scatter / table)
SELECT area_name, demand_rank, engagement_score, views_30d, saves_30d, contacts_30d,
       ROUND(contact_rate_pct, 2) AS contact_rate_pct, txn_count_90d, median_psf_90d
FROM mart_area_demand ORDER BY demand_rank;

-- Q7  Real-time: events per hour by type (line, auto-refresh 1 min)
SELECT event_date, event_hour, event_type, SUM(events) AS events
FROM mart_realtime_hourly GROUP BY event_date, event_hour, event_type ORDER BY event_date, event_hour;

-- Q8  Pipeline health: latest run per dataset
SELECT dataset, rows_in, rows_out, rows_rejected, ROUND(reject_pct, 2) AS reject_pct, checks_failed, checks_warned, finished_at
FROM mart_pipeline_health ORDER BY finished_at DESC;

-- Q9  Why rows were rejected (bar)
SELECT dataset, reject_reasons, COUNT(*) AS rows_rejected FROM dq_rejected_records GROUP BY dataset, reject_reasons ORDER BY rows_rejected DESC;

-- Q10 Failed / warned checks (table)
SELECT dataset, check_name, severity, status, failed_count, total_count, detail, checked_at
FROM dq_run_log WHERE status <> 'PASS' ORDER BY checked_at DESC;
