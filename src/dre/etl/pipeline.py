"""Transform + load: standardized -> data-quality gate -> clean/rejected layers -> staging -> star schema -> marts.

Every step is a small function so it maps 1:1 onto a Matillion component (see docs/matillion_mapping.md).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from ..config import Settings
from ..quality.runner import QualityGateError, run_quality
from ..schemas import DATASETS, cast_frame
from ..storage import Storage
from ..warehouse import RedshiftWarehouse, Warehouse
from .checks import run_warehouse_checks
from .dim_date import build_dim_date

log = logging.getLogger("dre.etl")

DIMENSION_MODELS = ["10_dim_area.sql", "11_dim_project.sql", "12_dim_property.sql"]
FACT_MODELS = ["20_fact_transactions.sql", "21_fact_rentals.sql", "22_fact_events.sql"]
MART_MODELS = ["30_mart_price_trends.sql", "31_mart_rental_yield.sql", "32_mart_bedroom_popularity.sql",
               "33_mart_area_demand.sql", "34_mart_realtime_hourly.sql"]
HEALTH_MODEL = "40_mart_pipeline_health.sql"   # runs last, after checks are logged


def _now() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")


def init_warehouse(settings: Settings, wh: Warehouse) -> None:
    wh.run_file(settings.sql_dir / "ddl" / "001_tables.sql")
    wh.load_dataframe("dim_date", build_dim_date())
    load_area_aliases(settings, wh)


def load_area_aliases(settings: Settings, wh: Warehouse) -> None:
    aliases = pd.read_csv(settings.seeds_dir / "area_aliases.csv", dtype=str)
    aliases["alias"] = aliases["alias"].str.strip().str.lower()
    wh.load_dataframe("area_alias", aliases)


def _log_run(wh: Warehouse, run_id: str, stage: str, dataset: str, status: str, rows_in=0, rows_out=0, rows_rejected=0,
             started_at: str | None = None, message: str = "") -> None:
    row = pd.DataFrame([{"run_id": run_id, "stage": stage, "dataset": dataset, "status": status, "rows_in": rows_in,
                         "rows_out": rows_out, "rows_rejected": rows_rejected, "started_at": pd.Timestamp(started_at or _now()),
                         "finished_at": pd.Timestamp(_now()), "message": message[:2000]}])
    wh.load_dataframe("pipeline_run_log", row, replace=False)


def _load_stage(settings: Settings, storage: Storage, wh: Warehouse, table: str, df: pd.DataFrame, clean_key: str) -> None:
    """Redshift: bulk COPY straight from S3. Local engines: insert the DataFrame."""
    if isinstance(wh, RedshiftWarehouse) and storage.is_s3:
        wh.copy_from_s3(table, storage.uri(clean_key), settings.clean_format)
    else:
        wh.load_dataframe(table, df)


def process_dataset(settings: Settings, storage: Storage, wh: Warehouse, dataset: str, run_id: str) -> dict:
    spec = DATASETS[dataset]
    started = _now()
    key = storage.latest_partition_file(f"standardized/{dataset}/")
    if key is None:
        _log_run(wh, run_id, "transform", dataset, "SKIPPED", started_at=started, message="no standardized data")
        log.warning("no standardized data for %s, skipping", dataset)
        return {"dataset": dataset, "status": "SKIPPED"}

    report_key = key.rsplit("/", 1)[0] + "/_report.json"
    report = storage.get_json(report_key, {}) or {}
    df = cast_frame(storage.get_frame(key), spec)

    try:
        result = run_quality(df, spec, settings, run_id, source_columns=report.get("source_columns"),
                             ingest_checks=report.get("checks"))
    except QualityGateError as err:
        _log_run(wh, run_id, "transform", dataset, "ABORTED", rows_in=len(df), started_at=started, message=str(err))
        raise

    clean_key = storage.put_frame(f"clean/{dataset}/run_id={run_id}/data", result.clean, settings.clean_format)
    if len(result.rejected):
        storage.put_frame(f"rejected/{dataset}/run_id={run_id}/data", result.rejected, settings.clean_format)

    _load_stage(settings, storage, wh, f"stg_{dataset}", result.clean, clean_key)
    rej_cols = ["run_id", "dataset", "record_key", "reject_reasons", "payload", "rejected_at"]
    rejected = result.rejected[rej_cols].copy()
    rejected["rejected_at"] = pd.to_datetime(rejected["rejected_at"])
    wh.load_dataframe("dq_rejected_records", rejected, replace=False)
    checks = result.checks.copy()
    checks["checked_at"] = pd.to_datetime(checks["checked_at"])
    wh.load_dataframe("dq_run_log", checks, replace=False)
    _log_run(wh, run_id, "transform", dataset, "OK", rows_in=len(df), rows_out=len(result.clean),
             rows_rejected=len(result.rejected), started_at=started)
    log.info("%s: %d in, %d clean, %d rejected (%.2f%%)", dataset, len(df), len(result.clean), len(result.rejected), result.reject_pct)
    return {"dataset": dataset, "status": "OK", "rows_in": len(df), "rows_clean": len(result.clean),
            "rows_rejected": len(result.rejected), "reject_pct": round(result.reject_pct, 2)}


def run_models(settings: Settings, wh: Warehouse, names: list[str]) -> None:
    for name in names:
        log.info("running model %s", name)
        wh.run_file(settings.sql_dir / "models" / name)


def run_transform(settings: Settings, storage: Storage, wh: Warehouse, run_id: str,
                  datasets: tuple[str, ...] = ("transactions", "rentals", "projects")) -> list[dict]:
    init_warehouse(settings, wh)
    summaries = [process_dataset(settings, storage, wh, ds, run_id) for ds in datasets]
    run_models(settings, wh, DIMENSION_MODELS + FACT_MODELS[:2])
    run_models(settings, wh, FACT_MODELS[2:] + MART_MODELS)
    checks = run_warehouse_checks(wh, run_id)
    checks["checked_at"] = pd.to_datetime(checks["checked_at"])
    wh.load_dataframe("dq_run_log", checks, replace=False)
    _log_run(wh, run_id, "warehouse_checks", "warehouse", "FAIL" if (checks.status == "FAIL").any() else "OK")
    run_models(settings, wh, [HEALTH_MODEL])
    return summaries
