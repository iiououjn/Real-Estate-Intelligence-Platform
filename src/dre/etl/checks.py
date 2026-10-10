"""Post-load warehouse checks: referential integrity, reconciliation, cross-dataset consistency.
Each check is SQL returning one number (violations). Results go to dq_run_log with dataset='warehouse'."""
from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

# (name, severity, violations_sql, total_sql, description)
WAREHOUSE_CHECKS = [
    ("ri_fact_transactions_area", "fail",
     "SELECT COUNT(*) FROM fact_transactions f LEFT JOIN dim_area d ON d.area_key = f.area_key WHERE d.area_key IS NULL",
     "SELECT COUNT(*) FROM fact_transactions", "fact_transactions rows pointing at a missing dim_area row"),
    ("ri_fact_transactions_project", "fail",
     "SELECT COUNT(*) FROM fact_transactions f LEFT JOIN dim_project d ON d.project_key = f.project_key WHERE d.project_key IS NULL",
     "SELECT COUNT(*) FROM fact_transactions", "fact_transactions rows pointing at a missing dim_project row"),
    ("ri_fact_transactions_property", "fail",
     "SELECT COUNT(*) FROM fact_transactions f LEFT JOIN dim_property d ON d.property_key = f.property_key WHERE d.property_key IS NULL",
     "SELECT COUNT(*) FROM fact_transactions", "fact_transactions rows pointing at a missing dim_property row"),
    ("ri_fact_transactions_date", "fail",
     "SELECT COUNT(*) FROM fact_transactions f LEFT JOIN dim_date d ON d.date_key = f.date_key WHERE d.date_key IS NULL",
     "SELECT COUNT(*) FROM fact_transactions", "fact_transactions rows pointing at a missing dim_date row"),
    ("ri_fact_rentals_area", "fail",
     "SELECT COUNT(*) FROM fact_rentals f LEFT JOIN dim_area d ON d.area_key = f.area_key WHERE d.area_key IS NULL",
     "SELECT COUNT(*) FROM fact_rentals", "fact_rentals rows pointing at a missing dim_area row"),
    ("ri_fact_rentals_property", "fail",
     "SELECT COUNT(*) FROM fact_rentals f LEFT JOIN dim_property d ON d.property_key = f.property_key WHERE d.property_key IS NULL",
     "SELECT COUNT(*) FROM fact_rentals", "fact_rentals rows pointing at a missing dim_property row"),
    ("reconcile_transactions_stage_vs_fact", "fail",
     "SELECT ABS((SELECT COUNT(*) FROM stg_transactions) - (SELECT COUNT(*) FROM fact_transactions))",
     "SELECT COUNT(*) FROM stg_transactions", "rows lost or duplicated between staging and fact (join fan-out / drop)"),
    ("reconcile_rentals_stage_vs_fact", "fail",
     "SELECT ABS((SELECT COUNT(*) FROM stg_rentals) - (SELECT COUNT(*) FROM fact_rentals))",
     "SELECT COUNT(*) FROM stg_rentals", "rows lost or duplicated between staging and fact (join fan-out / drop)"),
    ("duplicate_keys_fact_transactions", "fail",
     "SELECT COUNT(*) - COUNT(DISTINCT transaction_id) FROM fact_transactions",
     "SELECT COUNT(*) FROM fact_transactions", "transaction_id appears more than once in fact_transactions"),
    ("rental_area_without_sales", "warn",
     "SELECT COUNT(*) FROM (SELECT DISTINCT area_key FROM fact_rentals) r "
     "WHERE r.area_key NOT IN (SELECT area_key FROM fact_transactions WHERE trans_group = 'Sales')",
     "SELECT COUNT(DISTINCT area_key) FROM fact_rentals", "areas with rentals but no sales: yield cannot be computed"),
    ("project_missing_from_reference", "warn",
     "SELECT COUNT(DISTINCT project_name) FROM stg_transactions WHERE project_name IS NOT NULL "
     "AND EXISTS (SELECT 1 FROM stg_projects) AND project_name NOT IN (SELECT project_name FROM stg_projects)",
     "SELECT COUNT(DISTINCT project_name) FROM stg_transactions", "projects in transactions that are absent from the projects dataset"),
    ("event_area_unresolved", "warn",
     "SELECT COUNT(*) FROM fact_events WHERE dld_area_name IS NOT NULL AND dld_area_name NOT IN (SELECT area_name FROM dim_area)",
     "SELECT COUNT(*) FROM fact_events", "events whose community could not be mapped to a DLD area (extend seeds/area_aliases.csv)"),
]


def run_warehouse_checks(wh, run_id: str) -> pd.DataFrame:
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")
    rows = []
    for name, severity, violations_sql, total_sql, description in WAREHOUSE_CHECKS:
        failed = int(wh.scalar(violations_sql) or 0)
        total = int(wh.scalar(total_sql) or 0)
        status = "PASS" if failed == 0 else ("FAIL" if severity == "fail" else "WARN")
        rows.append({"run_id": run_id, "dataset": "warehouse", "check_name": name, "severity": severity, "status": status,
                     "failed_count": failed, "total_count": total, "detail": description, "checked_at": now})
    return pd.DataFrame(rows)
