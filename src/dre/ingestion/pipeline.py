"""Ingestion orchestration: (download) -> validate -> land raw in the lake -> standardize -> land standardized."""
from __future__ import annotations

import os
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from ..config import Settings
from ..schemas import DATASETS
from ..storage import Storage
from .download import download_file, get_dubai_pulse_token
from .standardize import StandardizeReport, standardize_frame
from .validate import IngestError, validate_file

CHUNK_ROWS = 250_000
URL_ENV = {"transactions": "DLD_TRANSACTIONS_URL", "rentals": "DLD_RENTS_URL", "projects": "DLD_PROJECTS_URL"}


def resolve_source(dataset: str, source: str | None, settings: Settings) -> Path:
    """A local path wins; otherwise download from the URL given (or from the env var for that dataset)."""
    if source and not source.lower().startswith(("http://", "https://")):
        return Path(source)
    url = source or os.environ.get(URL_ENV[dataset])
    if not url:
        raise IngestError(f"No file or URL for '{dataset}'. Pass --file/--url or set {URL_ENV[dataset]}.")
    headers = {}
    cid, secret = os.environ.get("DUBAI_PULSE_CLIENT_ID"), os.environ.get("DUBAI_PULSE_CLIENT_SECRET")
    if cid and secret:
        headers["Authorization"] = f"Bearer {get_dubai_pulse_token(cid, secret)}"
    dest = settings.data_dir / "downloads" / f"{dataset}_{date.today():%Y%m%d}.csv"
    return download_file(url, dest, headers=headers)


def ingest_dataset(settings: Settings, storage: Storage, dataset: str, source: str | None, run_id: str) -> dict:
    spec = DATASETS[dataset]
    path = resolve_source(dataset, source, settings)
    ingest_date = f"{date.today():%Y-%m-%d}"
    checks: list[dict] = []
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")

    # 1. validate the file (raises IngestError -> nothing is loaded)
    snapshot_key = f"dq/schema_snapshots/{dataset}.json"
    previous = storage.get_json(snapshot_key)
    validation = validate_file(path, spec, previous["columns"] if previous else None)

    if validation.removed_columns or validation.added_columns:
        checks.append({
            "check_name": "schema_change", "severity": "warn", "status": "WARN",
            "failed_count": len(validation.removed_columns) + len(validation.added_columns),
            "total_count": len(validation.source_columns),
            "detail": f"added={validation.added_columns[:10]} removed={validation.removed_columns[:10]}",
        })
    else:
        checks.append({"check_name": "schema_change", "severity": "warn", "status": "PASS",
                       "failed_count": 0, "total_count": len(validation.source_columns), "detail": ""})

    # 2. land the untouched source file (immutable raw layer)
    raw_key = f"raw/{dataset}/ingest_date={ingest_date}/{run_id}_{path.name}"
    storage.put_file(raw_key, path)

    # 3. standardize in chunks so a 1.5M-row national file never has to sit in memory as strings
    report = StandardizeReport()
    parts = []
    for chunk in pd.read_csv(path, dtype=str, keep_default_na=False, chunksize=CHUNK_ROWS, encoding=validation.encoding):
        std, rep = standardize_frame(chunk, spec)
        report.merge(rep)
        parts.append(std)
    if not parts:
        raise IngestError(f"{path} has a header but no data rows")
    df = pd.concat(parts, ignore_index=True)

    # 4. volume sanity check against the previous ingest
    last = storage.get_json(f"dq/last_ingest/{dataset}.json")
    drop_pct = 0.0
    if last and last.get("rows"):
        drop_pct = 100.0 * (last["rows"] - len(df)) / last["rows"]
    checks.append({
        "check_name": "volume_vs_previous_ingest", "severity": "warn",
        "status": "WARN" if drop_pct > settings.volume_drop_pct else "PASS",
        "failed_count": 1 if drop_pct > settings.volume_drop_pct else 0, "total_count": len(df),
        "detail": f"rows={len(df)} previous={last['rows'] if last else 'n/a'} change_pct={-drop_pct:.1f}",
    })
    if report.coerced_to_null:
        checks.append({"check_name": "type_coercion_failures", "severity": "warn", "status": "WARN",
                       "failed_count": sum(report.coerced_to_null.values()), "total_count": len(df),
                       "detail": str(report.coerced_to_null)})

    # 5. land standardized data + a machine-readable ingest report
    std_key = storage.put_frame(f"standardized/{dataset}/ingest_date={ingest_date}/data", df, settings.clean_format)
    summary = {
        "run_id": run_id, "dataset": dataset, "source_file": path.name, "ingest_date": ingest_date,
        "ingested_at": now, "rows": len(df), "raw_key": raw_key, "standardized_key": std_key,
        "mapped_columns": report.mapped, "unmapped_source_columns": report.unmapped_source_columns,
        "coerced_to_null": report.coerced_to_null, "source_columns": validation.source_columns,
        "checks": checks,
    }
    storage.put_json(f"standardized/{dataset}/ingest_date={ingest_date}/_report.json", summary)
    storage.put_json(snapshot_key, {"columns": validation.source_columns, "updated": now})
    storage.put_json(f"dq/last_ingest/{dataset}.json", {"rows": len(df), "run_id": run_id, "at": now})
    return summary
