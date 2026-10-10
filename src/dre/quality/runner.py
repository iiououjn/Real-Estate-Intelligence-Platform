"""Apply the rules, split clean vs rejected, and produce check rows for dq_run_log."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timezone

import pandas as pd

from ..config import Settings
from ..schemas import DatasetSpec
from .rules import RULES, RuleContext, Thresholds


class QualityGateError(Exception):
    """Too many rows rejected: refuse to load, so dashboards never show numbers built on junk."""


@dataclass
class QualityResult:
    clean: pd.DataFrame
    rejected: pd.DataFrame          # original columns + record_key, reject_reasons, payload, run_id, dataset, rejected_at
    checks: pd.DataFrame            # rows for dq_run_log
    reject_pct: float


CHECK_COLUMNS = ["run_id", "dataset", "check_name", "severity", "status", "failed_count", "total_count", "detail", "checked_at"]


def _now() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")


def run_quality(df: pd.DataFrame, spec: DatasetSpec, settings: Settings, run_id: str,
                source_columns: list[str] | None = None, ingest_checks: list[dict] | None = None,
                today: date | None = None) -> QualityResult:
    today = today or date.today()
    ctx = RuleContext(source_columns=source_columns or list(df.columns), today=today, thresholds=Thresholds())
    total = len(df)
    check_rows: list[dict] = []
    reject_reasons = pd.Series("", index=df.index, dtype=object)
    reject_mask = pd.Series(False, index=df.index)

    for rule in RULES[spec.name]:
        violated = rule.fn(df, ctx).fillna(False).astype(bool)
        n = int(violated.sum())
        if rule.severity == "reject":
            reject_reasons = reject_reasons.where(~violated, reject_reasons + rule.name + ";")
            reject_mask |= violated
            status = "FAIL" if n else "PASS"
        else:
            status = "WARN" if n else "PASS"
        check_rows.append({"check_name": rule.name, "severity": rule.severity, "status": status,
                           "failed_count": n, "total_count": total, "detail": rule.description})

    # dataset-level: freshness
    newest = df[spec.date_col].max() if total else pd.NaT
    if pd.notna(newest):
        age = (pd.Timestamp(today) - newest).days
        stale = age > settings.stale_days
        check_rows.append({"check_name": "freshness", "severity": "warn", "status": "WARN" if stale else "PASS",
                           "failed_count": int(stale), "total_count": 1,
                           "detail": f"newest {spec.date_col}={newest:%Y-%m-%d}, age={age}d, threshold={settings.stale_days}d"})
    else:
        check_rows.append({"check_name": "freshness", "severity": "warn", "status": "WARN", "failed_count": 1,
                           "total_count": 1, "detail": f"no valid {spec.date_col} found"})

    for extra in ingest_checks or []:      # schema_change, volume_vs_previous_ingest, ...
        check_rows.append(dict(extra))

    now = _now()
    checks = pd.DataFrame(check_rows)
    checks.insert(0, "dataset", spec.name)
    checks.insert(0, "run_id", run_id)
    checks["checked_at"] = now
    checks = checks[CHECK_COLUMNS]

    rejected = df[reject_mask].copy()
    rejected["record_key"] = rejected[spec.key].astype("string").fillna("<null>")
    rejected["reject_reasons"] = reject_reasons[reject_mask].str.rstrip(";")
    rejected["payload"] = [json.dumps({k: (None if pd.isna(v) else str(v)) for k, v in row.items()})
                           for row in df[reject_mask].to_dict("records")]
    rejected["run_id"], rejected["dataset"], rejected["rejected_at"] = run_id, spec.name, now

    reject_pct = 100.0 * len(rejected) / total if total else 0.0
    if reject_pct > settings.max_reject_pct:
        raise QualityGateError(
            f"{spec.name}: {reject_pct:.1f}% of rows rejected (limit {settings.max_reject_pct}%). "
            "Load aborted; inspect the rejected layer and the schema mapping (run `dre profile`)."
        )
    return QualityResult(clean=df[~reject_mask].reset_index(drop=True), rejected=rejected.reset_index(drop=True),
                         checks=checks, reject_pct=reject_pct)
