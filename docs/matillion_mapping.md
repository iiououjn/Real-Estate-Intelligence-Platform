# Translating this pipeline to Matillion

I did not have Matillion access, so the ETL is hand-built in Python + SQL. It is structured so that every step
maps onto a Matillion job. Component names below are the general Matillion ETL for Redshift vocabulary and can
differ slightly between versions, so check them against your instance.

| Step in this repo | Where | Matillion equivalent |
|---|---|---|
| Land raw CSV in S3 | `ingestion/pipeline.py` | Orchestration job: Python Script / AWS-CLI step, or an S3 landing zone fed by an external process |
| Standardize columns / types / nulls | `ingestion/standardize.py` | Transformation job: Table Input (`raw_*`) -> Rename -> Calculator (casts, `NULLIF`, `TRIM`) -> Table Output (`std_*`) |
| Quality rules, split clean vs rejected | `quality/rules.py` | Transformation job: Calculator (one flag column per rule) -> Filter into two branches -> Table Output (`stg_*`, `dq_rejected_records`) |
| Quality gate (abort if >20% rejected) | `quality/runner.py` | Orchestration: If component on a reject-percentage variable -> End Failure + SNS/e-mail alert |
| S3 -> Redshift bulk load | `warehouse.copy_from_s3` | S3 Load component (Parquet / CSV) into `stg_*` |
| Dimensions | `sql/models/1x_*.sql` | Transformation jobs: Table Input -> Distinct -> Rank (row_number surrogate key) -> Rewrite Table |
| Facts | `sql/models/2x_*.sql` | Transformation jobs: Table Input -> Join to each dimension -> Calculator (sqft, price/sqft) -> Rewrite Table (or Table Update for incremental events) |
| Marts (medians, YoY, yield) | `sql/models/3x_*.sql` | Transformation jobs; medians via SQL Script or Window Calculation (`PERCENTILE_CONT`) |
| Warehouse checks (RI, reconciliation) | `etl/checks.py` | Orchestration: SQL Script / Assert-style components after the loads; failures route to an alert |
| Run order | `etl/pipeline.py::run_transform` | One Orchestration job: ingest -> stg loads -> dims -> facts -> marts -> checks -> health mart |
| Schedule | (cron / EventBridge) | Matillion Schedule on the orchestration job |
| Environment settings | `.env` | Matillion Environment + Job Variables (bucket, prefix, thresholds) |

## Design choices that make the port straightforward
* **All business logic that can be SQL is SQL** (`sql/models/`). Those files can be pasted into Matillion's SQL Script
  component, or rebuilt as visual transformation components.
* **Each Python function is one step with explicit inputs/outputs**, so it becomes one Python Script or one component.
* **The SQL is a portable subset** (see `warehouse.py`). Only physical design (`DISTKEY`/`SORTKEY`) differs on Redshift.
* **Rejected rows are a first-class output**, which maps to a second Table Output branch.

## Suggested Matillion job layout
```
orch_daily_batch
 |- orch_land_raw            (S3 landing / Python Script)
 |- trn_standardize_*        (3 jobs)
 |- trn_quality_*            (3 jobs: clean + rejected outputs)
 |- s3load_stg_*
 |- trn_dim_*  -> trn_fact_*  -> trn_mart_*
 |- sql_warehouse_checks
 '- trn_pipeline_health
```
