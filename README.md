# Real Estate Intelligence

A **data-engineering** platform on Dubai Land Department (DLD) open data: batch ingestion, a quality gate, a star-schema
warehouse, a simulated real-time event pipeline, SQL data products and a dashboard, plus a small statistics API.
It is intentionally *not* an ML project. The "intelligence" is medians, yields and comparables computed in SQL.

```
DLD / Dubai Pulse -> Python ingestion -> S3 -> quality gate -> Redshift (star schema) -> SQL marts -> dashboard / API
simulated events  -> Kinesis -> Lambda -> S3 (events/dt=/hr=) -> micro-batch load -> fact_events -> demand marts
```
See [docs/architecture.md](docs/architecture.md) for the diagram and design decisions.

## What is real and what is simulated
* **Batch data:** designed for the real DLD transactions, Ejari rental and projects datasets from Dubai Pulse.
* **Offline mode:** `generate-sample` writes *synthetic* files in the same shape (real area names, invented prices,
  deliberately injected defects). That is what the demo and tests use. **The demo numbers are not market data.**
* **Events:** always simulated (there is no public marketplace event feed).


## Using real DLD data
1. Request access to the datasets on [Dubai Pulse](https://www.dubaipulse.gov.ae) (DLD Transactions, Ejari rental
   contracts, projects). Some downloads need an account and approval; put credentials in `.env` (see `.env.example`).
2. Download the CSVs, then **check the column mapping before loading**:
   ```bash
   python -m dre.cli profile transactions path/to/Transactions.csv
   ```
   `schemas.py` lists candidate source names per column (e.g. `instance_date`, `area_name_en`, `property_type_en`).
   I matched them to the published DLD schema, but I could not download the full files here, so treat the profile
   output as the source of truth and edit `schemas.py` if a column differs.


## Commands
| Command | Purpose |
|---|---|
| `generate-sample` | synthetic DLD-shaped CSVs |
| `profile <dataset> <file>` | show how a real file maps onto the schema |
| `ingest` | validate, land raw, standardize into the lake |
| `transform` | quality gate, staging, star schema, marts, checks |
| `stream-sim` | generate events (local Lambda handler or Kinesis) |
| `stream-load` | load event files into the warehouse (idempotent) |
| `estimate "<query>"` | median price, yield, comparables |
| `health` | latest run, reject reasons |
| `export` | CSVs of marts and star schema for Power BI / Tableau |
| `demo` | everything, locally |

## Dashboard
[dashboards/metabase/questions.sql](dashboards/metabase/questions.sql) has 10 ready SQL questions (price trends,
volumes, movers, rent-vs-buy, bedroom mix, community demand, real-time activity, pipeline health, reject reasons, failed
checks). Run `docker compose up` in that folder, connect Metabase to Redshift, paste the questions and pin them to one
dashboard. For Power BI or Tableau, run `python -m dre.cli export` and load the CSVs.

## API
`GET /estimate?q=Dubai Marina, 2-bedroom, 1,200 sq ft` returns recent median AED/sqft with quartiles, an indicative
median-based figure for the size given, gross yield and the ten closest comparable transactions. It uses statistics only, so it is
**not AI and not a valuation**; the response says so. Endpoints: `/estimate`, `/estimate/structured`, `/areas`, `/health`.

## Layout
```
src/dre/{ingestion,quality,etl,streaming,analytics,api}   Python
sql/ddl, sql/models                                       portable SQL (DuckDB / SQLite / Redshift)
infra/terraform                                           S3, Kinesis, Lambda, Redshift Serverless
docs/                                                     architecture, data quality, Matillion mapping
tests/                                                    27 tests
```
Matillion: no access, so see [docs/matillion_mapping.md](docs/matillion_mapping.md) for how each step translates.

## Testing status (please read)
| Area | Status |
|---|---|
| Ingestion, standardization, quality rules, star schema, marts, streaming handler, estimate service | Unit and end-to-end tests pass on SQLite (27 tests) |
| DuckDB backend, Parquet lake | Default demo successfully run on 2026-10-05; CI repeats the demo |
| Redshift adapter, COPY loads, Terraform, Kinesis producer | **Not run against AWS.** Expect small fixes on first deploy |
| FastAPI app | Live HTTP checks passed on 2026-10-05; CI runs `python scripts/smoke_api.py` after the demo |

