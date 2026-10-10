"""AWS Lambda consumer for the Kinesis stream of marketplace events.

Self-contained on purpose (stdlib + boto3 only) so it can be zipped and deployed as-is.

Flow per invocation (a Kinesis batch):
  decode base64 -> parse JSON -> validate -> de-duplicate on event_id -> enrich (event_date/hour, ingested_at)
  -> write valid events to  s3://<bucket>/<prefix>/events/dt=YYYY-MM-DD/hr=HH/<uuid>.jsonl.gz
  -> write invalid events to s3://<bucket>/<prefix>/events_rejected/...  (dead-letter, with the reason)

Malformed records are NOT retried (retrying cannot fix them); they are stored with a reason instead.
If S3 itself fails we return the first sequence number in batchItemFailures so Kinesis retries the batch
(enable ReportBatchItemFailures on the event source mapping).
"""
from __future__ import annotations

import base64
import gzip
import json
import os
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone

SCHEMA_VERSION = 1
EVENT_TYPES = {"property_searched", "property_viewed", "listing_saved", "agent_contacted"}
NEEDS_LISTING = {"property_viewed", "listing_saved", "agent_contacted"}
STRING_FIELDS = ("event_id", "event_type", "session_id", "user_id", "listing_id", "property_type", "area_name",
                 "search_query", "agent_id", "device")
MAX_FUTURE_SKEW = timedelta(minutes=5)


def parse_ts(value) -> datetime:
    """ISO-8601 (with or without offset / 'Z') -> naive UTC datetime."""
    dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def validate_event(e, now: datetime | None = None) -> list[str]:
    """Return a list of problems (empty list = valid)."""
    now = now or datetime.now(timezone.utc).replace(tzinfo=None)
    if not isinstance(e, dict):
        return ["not_a_json_object"]
    errs: list[str] = []
    if not e.get("event_id") or not isinstance(e.get("event_id"), str):
        errs.append("missing_event_id")
    etype = e.get("event_type")
    if etype not in EVENT_TYPES:
        errs.append(f"unknown_event_type:{etype}")
    if not e.get("session_id"):
        errs.append("missing_session_id")
    try:
        ts = parse_ts(e.get("event_ts"))
        if ts - now > MAX_FUTURE_SKEW:
            errs.append("event_ts_in_future")
    except (TypeError, ValueError):
        errs.append("invalid_event_ts")
    if etype in NEEDS_LISTING and not e.get("listing_id"):
        errs.append("missing_listing_id")
    if etype == "property_searched" and not (e.get("search_query") or e.get("area_name")):
        errs.append("search_without_query_or_area")
    if etype == "agent_contacted" and not e.get("agent_id"):
        errs.append("missing_agent_id")
    for field in ("price_aed", "size_sqft"):
        v = e.get(field)
        if v is not None and (not isinstance(v, (int, float)) or isinstance(v, bool) or v < 0):
            errs.append(f"invalid_{field}")
    b = e.get("bedrooms")
    if b is not None and (not isinstance(b, int) or isinstance(b, bool) or not 0 <= b <= 10):
        errs.append("invalid_bedrooms")
    return errs


def enrich(e: dict, ingested_at: datetime) -> dict:
    ts = parse_ts(e["event_ts"])
    out = {k: e.get(k) for k in STRING_FIELDS}
    out.update({
        "event_ts": ts.strftime("%Y-%m-%d %H:%M:%S"), "event_date": ts.strftime("%Y-%m-%d"), "event_hour": ts.hour,
        "bedrooms": e.get("bedrooms"), "price_aed": e.get("price_aed"), "size_sqft": e.get("size_sqft"),
        "schema_version": SCHEMA_VERSION, "ingested_at": ingested_at.strftime("%Y-%m-%d %H:%M:%S"),
    })
    return out


# ------------------------------------------------------------------------------ sinks
class S3Sink:
    def __init__(self, bucket: str, prefix: str = "", client=None):
        import boto3

        self.bucket, self.prefix = bucket, prefix.strip("/")
        self.s3 = client or boto3.client("s3")

    @classmethod
    def from_env(cls):
        return cls(os.environ["EVENTS_BUCKET"], os.environ.get("EVENTS_PREFIX", "dubai-re"))

    def write(self, key: str, records: list[dict]) -> None:
        body = gzip.compress(("\n".join(json.dumps(r, default=str) for r in records) + "\n").encode())
        full = f"{self.prefix}/{key}" if self.prefix else key
        self.s3.put_object(Bucket=self.bucket, Key=full, Body=body, ContentEncoding="gzip")


class LocalSink:
    """Writes the same layout to disk. Used by the local simulator and by tests."""

    def __init__(self, root):
        from pathlib import Path

        self.root = Path(root)

    def write(self, key: str, records: list[dict]) -> None:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(gzip.compress(("\n".join(json.dumps(r, default=str) for r in records) + "\n").encode()))


# ------------------------------------------------------------------------------ handler
def process_records(records: list[dict], now: datetime | None = None) -> tuple[list[dict], list[dict], dict]:
    now = now or datetime.now(timezone.utc).replace(tzinfo=None)
    valid, rejected, seen = [], [], set()
    for rec in records:
        raw = ""
        try:
            raw = base64.b64decode(rec["kinesis"]["data"]).decode("utf-8")
            obj = json.loads(raw)
        except Exception:  # noqa: BLE001 - anything undecodable is a reject, not a crash
            rejected.append({"reason": "malformed_json", "raw": raw[:2000], "rejected_at": now.strftime("%Y-%m-%d %H:%M:%S")})
            continue
        problems = validate_event(obj, now)
        if not problems and obj["event_id"] in seen:
            problems = ["duplicate_event_id_in_batch"]
        if problems:
            rejected.append({"reason": ";".join(problems), "raw": raw[:2000], "rejected_at": now.strftime("%Y-%m-%d %H:%M:%S")})
            continue
        seen.add(obj["event_id"])
        valid.append(enrich(obj, now))
    return valid, rejected, {"received": len(records), "valid": len(valid), "rejected": len(rejected)}


def handler(event, context=None, sink=None, now=None):
    records = event.get("Records", [])
    valid, rejected, stats = process_records(records, now)
    try:
        sink = sink or S3Sink.from_env()
        batch_id = uuid.uuid4().hex
        groups = defaultdict(list)
        for e in valid:
            groups[(e["event_date"], e["event_hour"])].append(e)
        for (dt, hr), rows in groups.items():
            sink.write(f"events/dt={dt}/hr={hr:02d}/{batch_id}.jsonl.gz", rows)
        if rejected:
            stamp = (now or datetime.now(timezone.utc).replace(tzinfo=None))
            sink.write(f"events_rejected/dt={stamp:%Y-%m-%d}/hr={stamp:%H}/{batch_id}.jsonl.gz", rejected)
    except Exception as err:  # noqa: BLE001 - infrastructure failure: ask Kinesis to retry from the first record
        print(json.dumps({"level": "ERROR", "msg": "sink_write_failed", "error": str(err)}))
        first = records[0]["kinesis"]["sequenceNumber"] if records else None
        return {"batchItemFailures": [{"itemIdentifier": first}] if first else [], "stats": stats}
    print(json.dumps({"level": "INFO", "msg": "batch_processed", **stats}))
    return {"batchItemFailures": [], "stats": stats}
