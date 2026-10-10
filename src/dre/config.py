"""Central configuration. Everything is driven by environment variables (see .env.example)."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SQM_TO_SQFT = 10.7639


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


@dataclass(frozen=True)
class Settings:
    # --- where things run -------------------------------------------------
    mode: str = "local"                # local | aws  (where the data lake lives)
    data_dir: Path = REPO_ROOT / "data"
    s3_bucket: str | None = None
    s3_prefix: str = "dubai-re"
    aws_region: str = "eu-west-1"
    clean_format: str = "parquet"      # parquet | csv  (csv = zero-dependency fallback)

    # --- warehouse --------------------------------------------------------
    warehouse: str = "duckdb"          # duckdb | sqlite | redshift
    duckdb_path: Path = REPO_ROOT / "data" / "warehouse" / "dubai_re.duckdb"
    sqlite_path: str = str(REPO_ROOT / "data" / "warehouse" / "dubai_re.sqlite")
    redshift_host: str | None = None
    redshift_port: int = 5439
    redshift_db: str = "dev"
    redshift_user: str | None = None
    redshift_password: str | None = None
    redshift_schema: str = "dre"
    redshift_iam_role_arn: str | None = None

    # --- streaming --------------------------------------------------------
    kinesis_stream: str = "dre-property-events"

    # --- data quality thresholds -----------------------------------------
    stale_days: int = 14               # newest record older than this => freshness WARN
    max_reject_pct: float = 20.0       # above this the load is aborted (quality gate)
    volume_drop_pct: float = 30.0      # row-count drop vs previous ingest => WARN

    sql_dir: Path = REPO_ROOT / "sql"
    seeds_dir: Path = REPO_ROOT / "seeds"


def load_settings() -> Settings:
    data_dir = Path(_env("DRE_DATA_DIR", str(REPO_ROOT / "data")))
    return Settings(
        mode=_env("DRE_MODE", "local"),
        data_dir=data_dir,
        s3_bucket=_env("DRE_S3_BUCKET"),
        s3_prefix=_env("DRE_S3_PREFIX", "dubai-re"),
        aws_region=_env("AWS_REGION", "eu-west-1"),
        clean_format=_env("DRE_CLEAN_FORMAT", "parquet"),
        warehouse=_env("DRE_WAREHOUSE", "duckdb"),
        duckdb_path=Path(_env("DRE_DUCKDB_PATH", str(data_dir / "warehouse" / "dubai_re.duckdb"))),
        sqlite_path=_env("DRE_SQLITE_PATH", str(data_dir / "warehouse" / "dubai_re.sqlite")),
        redshift_host=_env("REDSHIFT_HOST"),
        redshift_port=int(_env("REDSHIFT_PORT", "5439")),
        redshift_db=_env("REDSHIFT_DB", "dev"),
        redshift_user=_env("REDSHIFT_USER"),
        redshift_password=_env("REDSHIFT_PASSWORD"),
        redshift_schema=_env("REDSHIFT_SCHEMA", "dre"),
        redshift_iam_role_arn=_env("REDSHIFT_IAM_ROLE_ARN"),
        kinesis_stream=_env("DRE_KINESIS_STREAM", "dre-property-events"),
        stale_days=int(_env("DRE_STALE_DAYS", "14")),
        max_reject_pct=float(_env("DRE_MAX_REJECT_PCT", "20")),
        volume_drop_pct=float(_env("DRE_VOLUME_DROP_PCT", "30")),
        sql_dir=Path(_env("DRE_SQL_DIR", str(REPO_ROOT / "sql"))),
        seeds_dir=Path(_env("DRE_SEEDS_DIR", str(REPO_ROOT / "seeds"))),
    )
