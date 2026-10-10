-- Monthly median AED/sq.ft. and volumes per area / property type / registration type (Sales only),
-- with month-over-month and year-over-year change. Prior-period lookups go through dim_date, not date maths.
DELETE FROM mart_price_trends_monthly;
INSERT INTO mart_price_trends_monthly (month_start, area_name, property_type, reg_type, txn_count, median_price_aed, median_psf,
                                       total_value_aed, prev_month_median_psf, mom_change_pct, prev_year_median_psf, yoy_change_pct)
WITH monthly AS (
  SELECT d.month_start, d.prev_month_start, d.prev_year_month_start, a.area_name, p.property_type, p.reg_type,
         COUNT(*) AS txn_count, MEDIAN(f.amount_aed) AS median_price_aed, MEDIAN(f.price_per_sqft) AS median_psf,
         SUM(f.amount_aed) AS total_value_aed
  FROM fact_transactions f
  JOIN dim_date d ON d.date_key = f.date_key
  JOIN dim_area a ON a.area_key = f.area_key
  JOIN dim_property p ON p.property_key = f.property_key
  WHERE f.trans_group = 'Sales' AND f.price_per_sqft IS NOT NULL
  GROUP BY d.month_start, d.prev_month_start, d.prev_year_month_start, a.area_name, p.property_type, p.reg_type
)
SELECT m.month_start, m.area_name, m.property_type, m.reg_type, m.txn_count, m.median_price_aed, m.median_psf, m.total_value_aed,
       pm.median_psf,
       CASE WHEN pm.median_psf > 0 THEN 100.0 * (m.median_psf / pm.median_psf - 1) END,
       py.median_psf,
       CASE WHEN py.median_psf > 0 THEN 100.0 * (m.median_psf / py.median_psf - 1) END
FROM monthly m
LEFT JOIN monthly pm ON pm.area_name = m.area_name AND pm.property_type = m.property_type AND pm.reg_type = m.reg_type
                    AND pm.month_start = m.prev_month_start
LEFT JOIN monthly py ON py.area_name = m.area_name AND py.property_type = m.property_type AND py.reg_type = m.reg_type
                    AND py.month_start = m.prev_year_month_start;
