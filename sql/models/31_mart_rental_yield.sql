-- Gross rental yield = median annual rent per sq.ft. / median sale price per sq.ft., trailing 365 days,
-- per area and property type. Rentals in this open data carry no bedroom count, so yield is not split by bedrooms.
DELETE FROM mart_rental_yield;
INSERT INTO mart_rental_yield (area_name, property_type, as_of_date, sales_count, median_sale_psf, rental_count,
                               median_annual_rent_psf, gross_yield_pct)
WITH sb AS (
  SELECT MAX(d.day_num) AS max_day, MAX(d.full_date) AS max_date
  FROM fact_transactions f JOIN dim_date d ON d.date_key = f.date_key
),
rb AS (
  SELECT MAX(d.day_num) AS max_day
  FROM fact_rentals f JOIN dim_date d ON d.date_key = f.start_date_key
),
sales AS (
  SELECT a.area_name, p.property_type, COUNT(*) AS sales_count, MEDIAN(f.price_per_sqft) AS median_sale_psf
  FROM fact_transactions f
  JOIN dim_date d ON d.date_key = f.date_key
  JOIN dim_area a ON a.area_key = f.area_key
  JOIN dim_property p ON p.property_key = f.property_key
  CROSS JOIN sb
  WHERE f.trans_group = 'Sales' AND f.price_per_sqft IS NOT NULL AND d.day_num > sb.max_day - 365
  GROUP BY a.area_name, p.property_type
),
rents AS (
  SELECT a.area_name, p.property_type, COUNT(*) AS rental_count, MEDIAN(f.annual_rent_per_sqft) AS median_rent_psf
  FROM fact_rentals f
  JOIN dim_date d ON d.date_key = f.start_date_key
  JOIN dim_area a ON a.area_key = f.area_key
  JOIN dim_property p ON p.property_key = f.property_key
  CROSS JOIN rb
  WHERE f.annual_rent_per_sqft IS NOT NULL AND d.day_num > rb.max_day - 365
  GROUP BY a.area_name, p.property_type
)
SELECT s.area_name, s.property_type, sb.max_date, s.sales_count, s.median_sale_psf, r.rental_count, r.median_rent_psf,
       CASE WHEN r.median_rent_psf IS NOT NULL AND s.median_sale_psf > 0 THEN 100.0 * r.median_rent_psf / s.median_sale_psf END
FROM sales s
CROSS JOIN sb
LEFT JOIN rents r ON r.area_name = s.area_name AND r.property_type = s.property_type;
