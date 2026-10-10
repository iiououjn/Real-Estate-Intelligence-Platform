# Dubai Real Estate Intelligence

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

## Quick start (no cloud, no downloads)
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt && pip install -e .
python -m dre.cli demo                       # synthetic data -> full pipeline -> DuckDB
python -m dre.cli health                     # pipeline + data-quality summary
python -m dre.cli estimate "Dubai Marina, 2-bedroom, 1,200 sq ft"
uvicorn dre.api.main:app --reload            # then open http://127.0.0.1:8000/docs
python -m pytest -q
```
Minimal-dependency mode (only pandas + numpy): `DRE_WAREHOUSE=sqlite DRE_CLEAN_FORMAT=csv python -m dre.cli demo`.

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
3. Ingest and transform:
   ```bash
   python -m dre.cli ingest --transactions Transactions.csv --rentals Rents.csv --projects Projects.csv
   python -m dre.cli transform
   ```
Real data has junk (1 AED prices, near-zero areas). That is what the quality layer quarantines. If more than 20% of a
file is rejected, the load aborts and tells you to inspect the mapping.

## Running on AWS
```bash
make lambda-zip
cd infra/terraform && terraform init && terraform plan -var-file=example.tfvars   # read the plan; it costs money
```
Then set `DRE_MODE=aws`, `DRE_WAREHOUSE=redshift`, the S3/Redshift variables in `.env`, and run the same CLI commands.
`stream-sim --target kinesis` publishes to the real stream. Destroy with `terraform destroy` when done.

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

## Known limitations
* Full refresh of dimensions and facts each run (fine at DLD scale; incremental loads would be a next step).
* Rental yield is by area and property type only (no bedroom count in open Ejari data).
* Area aliases cover 10 communities; extend `seeds/area_aliases.csv`.
* Data is licensed under the Dubai Pulse open data licence; check its terms before republishing.

See [docs/testing.md](docs/testing.md) for the validation results and remaining gaps.
