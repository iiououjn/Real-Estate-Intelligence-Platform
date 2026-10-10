"""Warehouse abstraction so the same SQL models run on three engines:

    duckdb    local development (fast, columnar, zero setup)
    sqlite    unit tests / CI (stdlib only; a MEDIAN aggregate is registered)
    redshift  production (Amazon Redshift / Redshift Serverless)

The SQL in sql/ is deliberately a portable subset: no vendor date functions, no QUALIFY, no
CREATE OR REPLACE. Date arithmetic goes through dim_date (day_num, month_start, prev_year_month_start).
Physical design (DISTKEY / SORTKEY) is injected with {{placeholders}} only on Redshift.
"""
from __future__ import annotations

import re
import sqlite3
import statistics
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from .config import Settings

REDSHIFT_PHYSICAL = {
    "{{PHYS_FACT_TX}}": "DISTKEY(area_key) SORTKEY(date_key)",
    "{{PHYS_FACT_RENT}}": "DISTKEY(area_key) SORTKEY(start_date_key)",
    "{{PHYS_FACT_EVENTS}}": "DISTKEY(session_id) SORTKEY(event_ts)",
    "{{PHYS_DIM}}": "DISTSTYLE ALL",
}


def split_statements(sql: str) -> list[str]:
    sql = re.sub(r"--[^\n]*", "", sql)  # strip line comments (keep them free of ';')
    return [s.strip() for s in sql.split(";") if s.strip()]


class Warehouse:
    name = "base"
    sql_vars: dict[str, str] = {}

    def __init__(self, settings: Settings):
        self.settings = settings

    # ---- to be provided by engines ----------------------------------------
    def _execute(self, sql: str, params=None): ...
    def _fetch_df(self, sql: str, params=None) -> pd.DataFrame: ...
    def _insert_frame(self, table: str, df: pd.DataFrame): ...
    def close(self): ...

    # ---- shared ------------------------------------------------------------
    def render(self, sql: str) -> str:
        for k, v in self.sql_vars.items():
            sql = sql.replace(k, v)
        return re.sub(r"\{\{[A-Z_]+\}\}", "", sql)  # unknown/other-engine placeholders vanish

    def execute(self, sql: str, params=None):
        return self._execute(self.render(sql), params)

    def run_script(self, sql: str) -> None:
        for stmt in split_statements(self.render(sql)):
            self._execute(stmt, None)

    def run_file(self, path: Path) -> None:
        self.run_script(Path(path).read_text())

    def df(self, sql: str, params=None) -> pd.DataFrame:
        return self._fetch_df(self.render(sql), params)

    def scalar(self, sql: str, params=None):
        out = self.df(sql, params)
        return None if out.empty else out.iloc[0, 0]

    def load_dataframe(self, table: str, df: pd.DataFrame, replace: bool = True) -> int:
        if replace:
            self._execute(f"DELETE FROM {table}", None)
        if len(df):
            self._insert_frame(table, self._prepare(df))
        return len(df)

    def _prepare(self, df: pd.DataFrame) -> pd.DataFrame:
        """pandas NA / NaT / NaN -> None, Timestamps -> datetime, so every driver sees plain Python values."""
        out = df.copy()
        for c in out.columns:
            if pd.api.types.is_datetime64_any_dtype(out[c]):
                out[c] = [None if pd.isna(v) else v.to_pydatetime() for v in out[c]]
            else:
                out[c] = out[c].astype(object).where(out[c].notna(), None)
        return out.astype(object)


# ------------------------------------------------------------------------------ SQLite
class _Median:
    def __init__(self):
        self.values: list[float] = []

    def step(self, v):
        if v is not None:
            self.values.append(float(v))

    def finalize(self):
        return statistics.median(self.values) if self.values else None


class SQLiteWarehouse(Warehouse):
    name = "sqlite"

    def __init__(self, settings: Settings, path: str | None = None):
        super().__init__(settings)
        path = path or settings.sqlite_path
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.con = sqlite3.connect(path)
        self.con.create_aggregate("median", 1, _Median)

    def _execute(self, sql, params=None):
        cur = self.con.execute(sql, params or [])
        self.con.commit()
        return cur

    def _fetch_df(self, sql, params=None):
        return pd.read_sql_query(sql, self.con, params=params or [])

    @staticmethod
    def _value(v):
        if isinstance(v, datetime):          # includes pandas Timestamp
            return v.strftime("%Y-%m-%d") if (v.hour, v.minute, v.second) == (0, 0, 0) else v.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(v, date):
            return v.strftime("%Y-%m-%d")
        if isinstance(v, bool):
            return int(v)
        return v

    def _insert_frame(self, table, df):
        cols = list(df.columns)
        rows = [[self._value(v) for v in rec] for rec in df.itertuples(index=False, name=None)]
        self.con.executemany(f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join('?' * len(cols))})", rows)
        self.con.commit()

    def close(self):
        self.con.close()


# ------------------------------------------------------------------------------ DuckDB
class DuckDBWarehouse(Warehouse):
    name = "duckdb"

    def __init__(self, settings: Settings, path: str | None = None):
        super().__init__(settings)
        import duckdb

        path = path or str(settings.duckdb_path)
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(path)

    def _execute(self, sql, params=None):
        return self.con.execute(sql, params or [])

    def _fetch_df(self, sql, params=None):
        return self.con.execute(sql, params or []).df()

    def _prepare(self, df):
        return df  # DuckDB reads pandas nullable dtypes (Int64/string/datetime64) natively; no object conversion needed

    def _insert_frame(self, table, df):
        cols = ",".join(df.columns)
        self.con.register("_stage_df", df)
        try:
            self.con.execute(f"INSERT INTO {table} ({cols}) SELECT {cols} FROM _stage_df")
        finally:
            self.con.unregister("_stage_df")

    def close(self):
        self.con.close()


# ------------------------------------------------------------------------------ Redshift
class RedshiftWarehouse(Warehouse):
    """Thin adapter over redshift_connector. Written against the documented API; not exercised against a
    live cluster in this repo's CI (see docs/testing.md)."""

    name = "redshift"
    sql_vars = REDSHIFT_PHYSICAL

    def __init__(self, settings: Settings):
        super().__init__(settings)
        import redshift_connector

        self.con = redshift_connector.connect(
            host=settings.redshift_host, port=settings.redshift_port, database=settings.redshift_db,
            user=settings.redshift_user, password=settings.redshift_password,
        )
        self.con.autocommit = True
        with self.con.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {settings.redshift_schema}")
            cur.execute(f"SET search_path TO {settings.redshift_schema}")

    @staticmethod
    def _q(sql):  # our SQL uses '?' placeholders; redshift_connector uses '%s'
        return sql.replace("?", "%s")

    def _execute(self, sql, params=None):
        with self.con.cursor() as cur:
            cur.execute(self._q(sql), params or None)

    def _fetch_df(self, sql, params=None):
        with self.con.cursor() as cur:
            cur.execute(self._q(sql), params or None)
            return cur.fetch_dataframe()

    def _insert_frame(self, table, df):
        cols = list(df.columns)
        sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join(['%s'] * len(cols))})"
        rows = [tuple(r) for r in df.itertuples(index=False, name=None)]
        with self.con.cursor() as cur:
            for i in range(0, len(rows), 1000):   # small tables only; big loads use copy_from_s3
                cur.executemany(sql, rows[i:i + 1000])

    def copy_from_s3(self, table: str, s3_uri: str, fmt: str = "parquet") -> None:
        """Bulk load. Redshift COPY reads Parquet by column position, so files must follow table column order."""
        role = self.settings.redshift_iam_role_arn
        if not role:
            raise ValueError("REDSHIFT_IAM_ROLE_ARN is required for COPY")
        options = "FORMAT AS PARQUET" if fmt == "parquet" else "FORMAT AS CSV GZIP IGNOREHEADER 1 TIMEFORMAT 'auto' DATEFORMAT 'auto' BLANKSASNULL EMPTYASNULL"
        self._execute(f"DELETE FROM {table}")
        self._execute(f"COPY {table} FROM '{s3_uri}' IAM_ROLE '{role}' {options}")

    def close(self):
        self.con.close()


def connect_warehouse(settings: Settings, **kwargs) -> Warehouse:
    if settings.warehouse == "duckdb":
        return DuckDBWarehouse(settings, **kwargs)
    if settings.warehouse == "sqlite":
        return SQLiteWarehouse(settings, **kwargs)
    if settings.warehouse == "redshift":
        return RedshiftWarehouse(settings)
    raise ValueError(f"Unknown warehouse {settings.warehouse!r}")
