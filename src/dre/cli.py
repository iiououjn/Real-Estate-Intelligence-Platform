"""Command-line entry point:  python -m dre.cli <command> --help"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .analytics.estimate import estimate, parse_query
from .config import load_settings
from .etl.pipeline import init_warehouse, run_transform
from .ingestion.pipeline import ingest_dataset
from .ingestion.sample_data import generate
from .ingestion.standardize import normalize_col_name
from .ingestion.validate import IngestError
from .quality.runner import QualityGateError
from .schemas import DATASETS
from .storage import Storage
from .streaming.event_generator import EventGenerator
from .streaming.lambda_handler import LocalSink
from .streaming.loader import load_events
from .streaming.producer import KinesisProducer, LocalProducer
from .warehouse import connect_warehouse

MARTS = ["mart_price_trends_monthly", "mart_rental_yield", "mart_bedroom_popularity", "mart_area_demand",
         "mart_realtime_hourly", "mart_pipeline_health"]
EXPORT_TABLES = MARTS + ["dim_date", "dim_area", "dim_project", "dim_property", "fact_transactions", "fact_rentals",
                         "fact_events", "dq_run_log", "dq_rejected_records", "pipeline_run_log"]


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]


def cmd_sample(a, s):
    paths = generate(Path(a.out), a.transactions, a.rentals, seed=a.seed)
    for name, p in paths.items():
        print(f"{name:13s} {p}")


def cmd_profile(a, s):
    """Show which standard columns a downloaded file maps to. Run this first on real DLD files."""
    spec = DATASETS[a.dataset]
    cols = [normalize_col_name(c) for c in pd.read_csv(a.file, nrows=0).columns]
    print(f"{a.file}: {len(cols)} columns\n")
    for c in spec.columns:
        if not c.sources:
            continue
        hit = next((x for x in c.sources if x in cols), None)
        flag = "OK     " if hit else ("MISSING" if c.required else "absent ")
        print(f"  {flag} {c.name:22s} <- {hit or '(tried: ' + ', '.join(c.sources) + ')'}")
    used = {next((x for x in c.sources if x in cols), None) for c in spec.columns}
    extra = [c for c in cols if c not in used]
    print(f"\nUnmapped source columns ({len(extra)}): {extra[:40]}")


def cmd_ingest(a, s):
    storage, run_id = Storage(s), a.run_id or new_run_id()
    sources = {"transactions": a.transactions, "rentals": a.rentals, "projects": a.projects}
    for ds, src in sources.items():
        if src is None and not a.all_from_env:
            continue
        try:
            summary = ingest_dataset(s, storage, ds, src, run_id)
            print(f"[ingest] {ds}: {summary['rows']:,} rows -> {summary['standardized_key']}")
        except IngestError as err:
            print(f"[ingest] {ds}: REJECTED FILE - {err}", file=sys.stderr)
            return 2
    print(f"run_id={run_id}")


def cmd_transform(a, s):
    storage, wh, run_id = Storage(s), connect_warehouse(s), a.run_id or new_run_id()
    try:
        for row in run_transform(s, storage, wh, run_id):
            print(json.dumps(row))
    except QualityGateError as err:
        print(f"[transform] ABORTED by quality gate: {err}", file=sys.stderr)
        return 3
    finally:
        wh.close()
    print(f"run_id={run_id}")


def cmd_stream_sim(a, s):
    gen = EventGenerator(seed=a.seed, bad_event_rate=a.bad_rate)
    payloads = gen.stream(a.sessions, spread_minutes=a.minutes)
    if a.target == "kinesis":
        prod = KinesisProducer(s.kinesis_stream, s.aws_region)
        key = lambda p: (json.loads(p).get("session_id", "bad") if p.startswith("{\"") else "bad")  # noqa: E731
        print(json.dumps(prod.send(payloads, key)))
    else:
        storage = Storage(s)
        sink = LocalSink(storage.root) if not storage.is_s3 else None
        if sink is None:
            print("--target local requires DRE_MODE=local", file=sys.stderr)
            return 2
        print(json.dumps(LocalProducer(sink).send(payloads)))


def cmd_stream_load(a, s):
    storage, wh = Storage(s), connect_warehouse(s)
    try:
        print(json.dumps(load_events(s, storage, wh, since_date=a.since, full=a.full)))
    finally:
        wh.close()


def cmd_estimate(a, s):
    p = parse_query(a.query)
    wh = connect_warehouse(s)
    try:
        print(json.dumps(estimate(wh, p.area or a.query, p.bedrooms, p.size_sqft), indent=2, default=str))
    finally:
        wh.close()


def cmd_health(a, s):
    wh = connect_warehouse(s)
    try:
        print(wh.df("SELECT dataset, rows_in, rows_out, rows_rejected, ROUND(reject_pct, 2) AS reject_pct, checks_failed, "
                    "checks_warned, finished_at FROM mart_pipeline_health ORDER BY finished_at DESC").head(15).to_string(index=False))
        print("\nTop reject reasons (latest run):")
        print(wh.df("SELECT dataset, reject_reasons, COUNT(*) AS n FROM dq_rejected_records WHERE run_id = "
                    "(SELECT run_id FROM pipeline_run_log ORDER BY finished_at DESC LIMIT 1) "
                    "GROUP BY dataset, reject_reasons ORDER BY n DESC").head(15).to_string(index=False))
    finally:
        wh.close()


def cmd_export(a, s):
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    wh = connect_warehouse(s)
    try:
        for t in EXPORT_TABLES:
            df = wh.df(f"SELECT * FROM {t}")
            df.to_csv(out / f"{t}.csv", index=False)
            print(f"{t:28s} {len(df):>9,} rows")
    finally:
        wh.close()


def cmd_demo(a, s):
    """Whole local pipeline in one go: sample data -> ingest -> transform -> stream -> load -> marts."""
    paths = generate(s.data_dir / "sample", a.transactions, a.rentals)
    run_id = new_run_id()
    storage, wh = Storage(s), connect_warehouse(s)
    try:
        for ds, p in paths.items():
            ingest_dataset(s, storage, ds, str(p), run_id)
        for row in run_transform(s, storage, wh, run_id):
            print(json.dumps(row))
        sink = LocalSink(storage.root)
        print("stream:", LocalProducer(sink).send(EventGenerator(seed=7).stream(a.sessions, spread_minutes=1440 * 7)))
        print("events:", load_events(s, storage, wh, full=True))
        # rebuild health mart so it shows this run
        from .etl.pipeline import HEALTH_MODEL, run_models
        run_models(s, wh, [HEALTH_MODEL])
    finally:
        wh.close()
    print(f"\nDone. Try:  python -m dre.cli health   |   python -m dre.cli estimate \"Dubai Marina, 2-bedroom, 1,200 sq ft\"")


def build_parser():
    p = argparse.ArgumentParser(prog="dre", description="Dubai Real Estate Intelligence platform")
    sub = p.add_subparsers(dest="cmd", required=True)

    x = sub.add_parser("generate-sample", help="write synthetic DLD-shaped CSVs for offline use")
    x.add_argument("--out", default="data/sample"); x.add_argument("--transactions", type=int, default=20000)
    x.add_argument("--rentals", type=int, default=8000); x.add_argument("--seed", type=int, default=42)
    x.set_defaults(fn=cmd_sample)

    x = sub.add_parser("profile", help="check how a real DLD file maps onto the standard schema")
    x.add_argument("dataset", choices=list(DATASETS)); x.add_argument("file"); x.set_defaults(fn=cmd_profile)

    x = sub.add_parser("ingest", help="validate + land raw + standardize into the lake")
    x.add_argument("--transactions"); x.add_argument("--rentals"); x.add_argument("--projects")
    x.add_argument("--all-from-env", action="store_true", help="download all three from DLD_*_URL env vars")
    x.add_argument("--run-id"); x.set_defaults(fn=cmd_ingest)

    x = sub.add_parser("transform", help="quality gate -> staging -> star schema -> marts")
    x.add_argument("--run-id"); x.set_defaults(fn=cmd_transform)

    x = sub.add_parser("stream-sim", help="generate marketplace events (local handler or real Kinesis)")
    x.add_argument("--sessions", type=int, default=500); x.add_argument("--minutes", type=float, default=60)
    x.add_argument("--target", choices=["local", "kinesis"], default="local"); x.add_argument("--seed", type=int, default=7)
    x.add_argument("--bad-rate", type=float, default=0.01); x.set_defaults(fn=cmd_stream_sim)

    x = sub.add_parser("stream-load", help="load event files from the lake into the warehouse")
    x.add_argument("--since"); x.add_argument("--full", action="store_true"); x.set_defaults(fn=cmd_stream_load)

    x = sub.add_parser("estimate", help="market statistics for a free-text query")
    x.add_argument("query"); x.set_defaults(fn=cmd_estimate)

    sub.add_parser("health", help="latest pipeline / data-quality summary").set_defaults(fn=cmd_health)

    x = sub.add_parser("export", help="dump marts + star schema to CSV for Power BI / Tableau")
    x.add_argument("--out", default="data/export"); x.set_defaults(fn=cmd_export)

    x = sub.add_parser("demo", help="run everything locally with synthetic data")
    x.add_argument("--transactions", type=int, default=20000); x.add_argument("--rentals", type=int, default=8000)
    x.add_argument("--sessions", type=int, default=3000); x.set_defaults(fn=cmd_demo)
    return p


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    args = build_parser().parse_args(argv)
    return args.fn(args, load_settings()) or 0


if __name__ == "__main__":
    sys.exit(main())
