"""Micro-batch load of Lambda-written event files into the warehouse (stg_events -> fact_events -> event marts).

Idempotent: fact_events only inserts event_ids it has not seen, so re-loading a partition is harmless.
On Redshift this is one COPY per dt= partition; locally the JSONL files are read with pandas.
"""
from __future__ import annotations

import gzip
import io
import json
import logging
import re

import pandas as pd

from ..config import Settings
from ..etl.pipeline import init_warehouse, run_models
from ..storage import Storage
from ..warehouse import RedshiftWarehouse, Warehouse

log = logging.getLogger("dre.streaming")

EVENT_COLUMNS = ["event_id", "event_type", "event_ts", "event_date", "event_hour", "session_id", "user_id", "listing_id",
                 "property_type", "area_name", "bedrooms", "price_aed", "size_sqft", "search_query", "agent_id", "device",
                 "schema_version", "ingested_at"]
EVENT_MODELS = ["22_fact_events.sql", "32_mart_bedroom_popularity.sql", "33_mart_area_demand.sql", "34_mart_realtime_hourly.sql"]


def _partition_date(key: str) -> str | None:
    m = re.search(r"dt=(\d{4}-\d{2}-\d{2})", key)
    return m.group(1) if m else None


def read_event_files(storage: Storage, since_date: str | None = None) -> pd.DataFrame:
    keys = [k for k in storage.list_keys("events/") if k.endswith(".jsonl.gz")]
    if since_date:
        keys = [k for k in keys if (_partition_date(k) or "9999") >= since_date]
    rows = []
    for key in keys:
        text = gzip.decompress(storage.get_bytes(key)).decode()
        rows += [json.loads(line) for line in text.splitlines() if line.strip()]
    df = pd.DataFrame(rows, columns=EVENT_COLUMNS)
    if df.empty:
        return df
    df["event_ts"] = pd.to_datetime(df["event_ts"])
    df["event_date"] = pd.to_datetime(df["event_date"])
    df["ingested_at"] = pd.to_datetime(df["ingested_at"])
    for c in ("event_hour", "bedrooms", "schema_version"):
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    for c in ("price_aed", "size_sqft"):
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")
    return df


def load_events(settings: Settings, storage: Storage, wh: Warehouse, since_date: str | None = None, full: bool = False) -> dict:
    init_warehouse(settings, wh)
    if since_date is None and not full:
        last = wh.scalar("SELECT MAX(d.full_date) FROM fact_events e JOIN dim_date d ON d.date_key = e.date_key")
        if last is not None and not pd.isna(last):
            since_date = (pd.Timestamp(last) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")   # re-read one day back for late data

    before = int(wh.scalar("SELECT COUNT(*) FROM fact_events") or 0)
    if isinstance(wh, RedshiftWarehouse) and storage.is_s3:
        wh.execute("DELETE FROM stg_events")
        dates = sorted({d for k in storage.list_keys("events/") if (d := _partition_date(k)) and (not since_date or d >= since_date)})
        for d in dates:
            wh.execute(f"COPY stg_events ({','.join(EVENT_COLUMNS)}) FROM '{storage.uri(f'events/dt={d}/')}' "
                       f"IAM_ROLE '{settings.redshift_iam_role_arn}' FORMAT AS JSON 'auto' GZIP TIMEFORMAT 'auto'")
        staged = int(wh.scalar("SELECT COUNT(*) FROM stg_events") or 0)
    else:
        df = read_event_files(storage, since_date)
        staged = wh.load_dataframe("stg_events", df)

    run_models(settings, wh, EVENT_MODELS)
    after = int(wh.scalar("SELECT COUNT(*) FROM fact_events") or 0)
    log.info("events staged=%d new=%d total=%d", staged, after - before, after)
    return {"staged": staged, "new_events": after - before, "total_events": after}
