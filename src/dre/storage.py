"""Data-lake abstraction: the same key layout works on local disk and on S3.

Layout (all keys relative to the lake root / S3 prefix):
    raw/<dataset>/ingest_date=YYYY-MM-DD/<file>          untouched source files
    standardized/<dataset>/ingest_date=.../data.<ext>    renamed columns, typed, nulls normalised
    clean/<dataset>/run_id=<id>/data.<ext>               rows that passed data-quality rules
    rejected/<dataset>/run_id=<id>/data.<ext>            rows that failed, with reasons
    events/dt=YYYY-MM-DD/hr=HH/*.jsonl.gz                real-time events (written by Lambda)
    events_rejected/dt=.../hr=.../*.jsonl.gz             malformed events (dead-letter)
    dq/...                                               schema snapshots and ingest reports
"""
from __future__ import annotations

import gzip
import io
import json
import shutil
from pathlib import Path

import pandas as pd

from .config import Settings


class Storage:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.is_s3 = settings.mode == "aws"
        if self.is_s3:
            import boto3  # imported lazily so local mode needs no AWS libraries

            if not settings.s3_bucket:
                raise ValueError("DRE_S3_BUCKET must be set when DRE_MODE=aws")
            self._s3 = boto3.client("s3", region_name=settings.aws_region)
            self.bucket = settings.s3_bucket
            self.prefix = settings.s3_prefix.strip("/")
        else:
            self.root = settings.data_dir / "lake"
            self.root.mkdir(parents=True, exist_ok=True)

    # ---- addressing ------------------------------------------------------
    def _s3_key(self, key: str) -> str:
        return f"{self.prefix}/{key}" if self.prefix else key

    def uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{self._s3_key(key)}" if self.is_s3 else str(self.root / key)

    # ---- primitives ------------------------------------------------------
    def put_bytes(self, key: str, data: bytes) -> str:
        if self.is_s3:
            self._s3.put_object(Bucket=self.bucket, Key=self._s3_key(key), Body=data)
        else:
            path = self.root / key
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        return key

    def put_file(self, key: str, local_path: str | Path) -> str:
        if self.is_s3:
            self._s3.upload_file(str(local_path), self.bucket, self._s3_key(key))
        else:
            dest = self.root / key
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(local_path, dest)
        return key

    def get_bytes(self, key: str) -> bytes:
        if self.is_s3:
            return self._s3.get_object(Bucket=self.bucket, Key=self._s3_key(key))["Body"].read()
        return (self.root / key).read_bytes()

    def exists(self, key: str) -> bool:
        if self.is_s3:
            try:
                self._s3.head_object(Bucket=self.bucket, Key=self._s3_key(key))
                return True
            except Exception:  # noqa: BLE001 - botocore ClientError for 404
                return False
        return (self.root / key).exists()

    def list_keys(self, prefix: str) -> list[str]:
        if self.is_s3:
            keys, token = [], None
            full = self._s3_key(prefix)
            while True:
                kwargs = {"Bucket": self.bucket, "Prefix": full}
                if token:
                    kwargs["ContinuationToken"] = token
                resp = self._s3.list_objects_v2(**kwargs)
                keys += [o["Key"][len(self.prefix) + 1:] if self.prefix else o["Key"] for o in resp.get("Contents", [])]
                if not resp.get("IsTruncated"):
                    return sorted(keys)
                token = resp["NextContinuationToken"]
        base = self.root / prefix
        if not base.exists():
            return []
        return sorted(str(p.relative_to(self.root)).replace("\\", "/") for p in base.rglob("*") if p.is_file())

    # ---- json ------------------------------------------------------------
    def put_json(self, key: str, obj) -> str:
        return self.put_bytes(key, json.dumps(obj, indent=2, default=str).encode())

    def get_json(self, key: str, default=None):
        if not self.exists(key):
            return default
        return json.loads(self.get_bytes(key))

    # ---- dataframes ------------------------------------------------------
    def put_frame(self, key_base: str, df: pd.DataFrame, fmt: str = "parquet") -> str:
        """Write a DataFrame as parquet or csv.gz and return the key (with extension)."""
        if fmt == "parquet":
            buf = io.BytesIO()
            df.to_parquet(buf, index=False)  # needs pyarrow
            key = f"{key_base}.parquet"
            self.put_bytes(key, buf.getvalue())
        elif fmt == "csv":
            key = f"{key_base}.csv.gz"
            self.put_bytes(key, gzip.compress(df.to_csv(index=False).encode("utf-8")))
        else:
            raise ValueError(f"Unknown format {fmt!r}")
        return key

    def get_frame(self, key: str) -> pd.DataFrame:
        data = self.get_bytes(key)
        if key.endswith(".parquet"):
            return pd.read_parquet(io.BytesIO(data))
        if key.endswith(".csv.gz"):
            return pd.read_csv(io.BytesIO(data), compression="gzip", dtype=str, keep_default_na=True)
        raise ValueError(f"Unsupported file type: {key}")

    def latest_partition_file(self, prefix: str) -> str | None:
        """Newest data file under e.g. 'standardized/transactions/' (partitions sort lexicographically)."""
        files = [k for k in self.list_keys(prefix) if "/data." in k]
        return files[-1] if files else None
