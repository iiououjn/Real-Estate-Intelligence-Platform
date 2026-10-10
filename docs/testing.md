# Validation results — 2026-10-05

## Executed successfully

- Python 3.12: `python -m pytest -q` — 27 passed.
- Default `PYTHONPATH=src python -m dre.cli demo` using DuckDB and Parquet.
- Input: 20,120 synthetic transaction rows and 8,040 synthetic rental rows (including injected defects), plus 30 projects.
- Accepted: 19,532 transactions and 7,864 rentals; quarantined: 588 transactions and 176 rentals.
- Warehouse checks reported zero failures. Source quality failures reflect deliberately injected defects.
- Simulated stream: 12,126 received, 11,962 valid, 164 rejected; 11,961 unique events loaded.
- `stream-load --full` replay: zero new events, total unchanged at 11,961.
- `health`: completed and reported pipeline/quality results.
- `python scripts/smoke_api.py`: started a real Uvicorn HTTP server, verified health, areas, free-text and structured estimates, response parity, comparables and disclaimer, unknown-area 404, and invalid/missing-input 422 responses. Server stopped after checks.
- `profile transactions data/sample/transactions.csv`: all 21 synthetic columns mapped, no unmapped columns.
- `make lambda-zip`: package built successfully; this is not an AWS execution test.

Runtime packages: DuckDB 1.5.6, PyArrow 25.0.1, FastAPI 0.142.2, Uvicorn 0.54.0, pytest 9.1.1. These are observed versions, not a dependency lockfile.

## Changes

Added `scripts/smoke_api.py` and connected it to the existing DuckDB demo CI job. Updated README testing status. No application logic changes were required by the executed checks.

## Still unverified

- Terraform CLI was unavailable: no `terraform validate`, plan, or apply was run.
- No live Redshift, S3, Kinesis, or deployed Lambda execution. AWS credentials/network configuration and a reviewed Terraform plan are needed for deployment testing.
- No real DLD files were supplied or downloaded. Synthetic profiling verifies the command, not the real source schema. Run profile against the actual downloaded files before ingestion.
- All demo figures remain synthetic and must not be used as real market statistics.

## Reproduce locally

```bash
pip install -r requirements.txt
pip install -e .
python -m pytest -q
python -m dre.cli demo
python -m dre.cli health
python -m dre.cli stream-load --full
python scripts/smoke_api.py
make lambda-zip
```

Generated demo data and build files are excluded from the delivered source ZIP.
