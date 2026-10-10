# Data-quality rules

Severity `reject` = row quarantined into `dq_rejected_records` (and `rejected/` in the lake). `warn` = kept, counted.

## Transactions
| Rule | Severity |
|---|---|
| missing_transaction_id, duplicate_transaction_id | reject |
| missing_property_id (only if the source file has that column) | reject |
| missing_area, missing_transaction_date, date_out_of_range (future / before 1990) | reject |
| invalid_area (null, zero, negative, absurdly large) | reject |
| impossible_sale_price (Sales rows only: null, < AED 10k, > AED 10bn) | reject |
| implausible_price_per_sqft (unit/villa sales outside AED 100-30,000 per sqft) | reject |
| missing_property_type, price_vs_meter_price_mismatch (>25%) | warn |

Mortgages and gifts legitimately carry odd values, so price rules apply to `Sales` only.

## Rentals
missing/duplicate contract_id, missing_area, missing_start_date, date_out_of_range, end_before_start, invalid_area,
impossible_annual_rent, implausible_rent_per_sqft (reject); missing_property_type (warn).

## Dataset level
`schema_change` (columns added/removed vs the last ingest), `volume_vs_previous_ingest` (>30% drop),
`type_coercion_failures`, `freshness` (newest record older than 14 days).

## Warehouse level (after load)
Referential integrity for every fact-to-dimension key, staging-vs-fact row reconciliation (catches join fan-out),
duplicate business keys in facts, rental areas without sales, projects missing from the reference dataset,
unresolved event communities.

All results land in `dq_run_log`; `mart_pipeline_health` summarises them per run.
