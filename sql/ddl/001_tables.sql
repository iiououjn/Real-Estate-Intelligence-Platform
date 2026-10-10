-- Portable DDL (DuckDB / SQLite / Redshift). {{PHYS_*}} tokens become DISTKEY/SORTKEY only on Redshift.
-- Conventions: stg_* = loaded from the clean layer, dim_* / fact_* = star schema, mart_* = analytics tables.
-- Surrogate keys are rebuilt each run (full refresh), so facts and dims are always rebuilt together.

CREATE TABLE IF NOT EXISTS stg_transactions (
  transaction_id VARCHAR(64), trans_group VARCHAR(64), procedure_name VARCHAR(256), transaction_date DATE,
  property_id VARCHAR(64), property_type VARCHAR(128), property_sub_type VARCHAR(128), property_usage VARCHAR(128),
  reg_type VARCHAR(128), area_name VARCHAR(256), building_name VARCHAR(512), project_name VARCHAR(512),
  master_project VARCHAR(512), nearest_landmark VARCHAR(256), nearest_metro VARCHAR(256), nearest_mall VARCHAR(256),
  rooms VARCHAR(64), bedrooms INTEGER, has_parking DOUBLE PRECISION, area_sqm DOUBLE PRECISION,
  amount_aed DOUBLE PRECISION, price_per_sqm DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS stg_rentals (
  contract_id VARCHAR(64), contract_reg_type VARCHAR(64), contract_start_date DATE, contract_end_date DATE,
  property_id VARCHAR(64), property_type VARCHAR(128), property_sub_type VARCHAR(128), property_usage VARCHAR(128),
  area_name VARCHAR(256), project_name VARCHAR(512), master_project VARCHAR(512), nearest_landmark VARCHAR(256),
  nearest_metro VARCHAR(256), nearest_mall VARCHAR(256), tenant_type VARCHAR(64), rooms VARCHAR(64), bedrooms INTEGER,
  area_sqm DOUBLE PRECISION, annual_amount DOUBLE PRECISION, contract_amount DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS stg_projects (
  project_id VARCHAR(64), project_name VARCHAR(512), master_project VARCHAR(512), area_name VARCHAR(256),
  project_status VARCHAR(64), percent_completed DOUBLE PRECISION, start_date DATE, completion_date DATE
);

CREATE TABLE IF NOT EXISTS stg_events (
  event_id VARCHAR(64), event_type VARCHAR(64), event_ts TIMESTAMP, event_date DATE, event_hour INTEGER,
  session_id VARCHAR(64), user_id VARCHAR(64), listing_id VARCHAR(64), property_type VARCHAR(64),
  area_name VARCHAR(256), bedrooms INTEGER, price_aed DOUBLE PRECISION, size_sqft DOUBLE PRECISION,
  search_query VARCHAR(512), agent_id VARCHAR(64), device VARCHAR(32), schema_version INTEGER, ingested_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS area_alias (alias VARCHAR(256), area_name VARCHAR(256));

-- ---------------------------------------------------------------- dimensions
CREATE TABLE IF NOT EXISTS dim_date (
  date_key INTEGER, full_date DATE, day_num INTEGER, year INTEGER, quarter INTEGER, month INTEGER,
  month_name VARCHAR(16), month_start DATE, prev_month_start DATE, prev_year_month_start DATE,
  day_of_week INTEGER, is_weekend INTEGER
) {{PHYS_DIM}};

CREATE TABLE IF NOT EXISTS dim_area (area_key INTEGER, area_name VARCHAR(256)) {{PHYS_DIM}};

CREATE TABLE IF NOT EXISTS dim_project (
  project_key INTEGER, project_name VARCHAR(512), master_project VARCHAR(512),
  project_status VARCHAR(64), percent_completed DOUBLE PRECISION
) {{PHYS_DIM}};

CREATE TABLE IF NOT EXISTS dim_property (
  property_key INTEGER, property_type VARCHAR(128), property_sub_type VARCHAR(128), property_usage VARCHAR(128),
  reg_type VARCHAR(128), bedrooms INTEGER, rooms_label VARCHAR(64)
) {{PHYS_DIM}};

-- ---------------------------------------------------------------- facts
CREATE TABLE IF NOT EXISTS fact_transactions (
  transaction_id VARCHAR(64), date_key INTEGER, area_key INTEGER, project_key INTEGER, property_key INTEGER,
  trans_group VARCHAR(64), procedure_name VARCHAR(256), property_id VARCHAR(64), has_parking DOUBLE PRECISION,
  area_sqm DOUBLE PRECISION, area_sqft DOUBLE PRECISION, amount_aed DOUBLE PRECISION, price_per_sqft DOUBLE PRECISION
) {{PHYS_FACT_TX}};

CREATE TABLE IF NOT EXISTS fact_rentals (
  contract_id VARCHAR(64), start_date_key INTEGER, end_date_key INTEGER, area_key INTEGER, project_key INTEGER,
  property_key INTEGER, contract_reg_type VARCHAR(64), tenant_type VARCHAR(64), property_id VARCHAR(64),
  area_sqm DOUBLE PRECISION, area_sqft DOUBLE PRECISION, annual_rent_aed DOUBLE PRECISION,
  annual_rent_per_sqft DOUBLE PRECISION, duration_days INTEGER
) {{PHYS_FACT_RENT}};

CREATE TABLE IF NOT EXISTS fact_events (
  event_id VARCHAR(64), event_ts TIMESTAMP, date_key INTEGER, event_hour INTEGER, event_type VARCHAR(64),
  session_id VARCHAR(64), user_id VARCHAR(64), listing_id VARCHAR(64), property_type VARCHAR(64),
  area_name VARCHAR(256), dld_area_name VARCHAR(256), bedrooms INTEGER, price_aed DOUBLE PRECISION,
  size_sqft DOUBLE PRECISION, search_query VARCHAR(512), agent_id VARCHAR(64), device VARCHAR(32),
  ingested_at TIMESTAMP
) {{PHYS_FACT_EVENTS}};

-- ---------------------------------------------------------------- pipeline metadata
CREATE TABLE IF NOT EXISTS pipeline_run_log (
  run_id VARCHAR(64), stage VARCHAR(64), dataset VARCHAR(64), status VARCHAR(16), rows_in INTEGER,
  rows_out INTEGER, rows_rejected INTEGER, started_at TIMESTAMP, finished_at TIMESTAMP, message VARCHAR(2048)
);

CREATE TABLE IF NOT EXISTS dq_run_log (
  run_id VARCHAR(64), dataset VARCHAR(64), check_name VARCHAR(128), severity VARCHAR(16), status VARCHAR(8),
  failed_count INTEGER, total_count INTEGER, detail VARCHAR(2048), checked_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dq_rejected_records (
  run_id VARCHAR(64), dataset VARCHAR(64), record_key VARCHAR(256), reject_reasons VARCHAR(1024),
  payload VARCHAR(65535), rejected_at TIMESTAMP
);

-- ---------------------------------------------------------------- marts
CREATE TABLE IF NOT EXISTS mart_price_trends_monthly (
  month_start DATE, area_name VARCHAR(256), property_type VARCHAR(128), reg_type VARCHAR(128), txn_count INTEGER,
  median_price_aed DOUBLE PRECISION, median_psf DOUBLE PRECISION, total_value_aed DOUBLE PRECISION,
  prev_month_median_psf DOUBLE PRECISION, mom_change_pct DOUBLE PRECISION,
  prev_year_median_psf DOUBLE PRECISION, yoy_change_pct DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS mart_rental_yield (
  area_name VARCHAR(256), property_type VARCHAR(128), as_of_date DATE, sales_count INTEGER,
  median_sale_psf DOUBLE PRECISION, rental_count INTEGER, median_annual_rent_psf DOUBLE PRECISION,
  gross_yield_pct DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS mart_bedroom_popularity (
  area_name VARCHAR(256), bedrooms INTEGER, txn_count INTEGER, share_of_area_txn_pct DOUBLE PRECISION,
  median_psf DOUBLE PRECISION, median_price_aed DOUBLE PRECISION, event_views INTEGER, event_saves INTEGER
);

CREATE TABLE IF NOT EXISTS mart_area_demand (
  area_name VARCHAR(256), searches_30d INTEGER, views_30d INTEGER, saves_30d INTEGER, contacts_30d INTEGER,
  sessions_30d INTEGER, contact_rate_pct DOUBLE PRECISION, engagement_score DOUBLE PRECISION, demand_rank INTEGER,
  txn_count_90d INTEGER, median_psf_90d DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS mart_realtime_hourly (
  event_date DATE, event_hour INTEGER, event_type VARCHAR(64), dld_area_name VARCHAR(256),
  events INTEGER, unique_sessions INTEGER
);

CREATE TABLE IF NOT EXISTS mart_pipeline_health (
  run_id VARCHAR(64), dataset VARCHAR(64), rows_in INTEGER, rows_out INTEGER, rows_rejected INTEGER,
  reject_pct DOUBLE PRECISION, checks_failed INTEGER, checks_warned INTEGER, finished_at TIMESTAMP
);
