"""File-level validation: readable, non-empty, required columns present, schema drift vs last run."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from ..schemas import DatasetSpec
from .standardize import normalize_col_name


class IngestError(Exception):
    """Raised when a file cannot be safely ingested (it is left in place, nothing is loaded)."""


@dataclass
class FileValidation:
    encoding: str
    source_columns: list[str]
    missing_required: list[str] = field(default_factory=list)
    added_columns: list[str] = field(default_factory=list)
    removed_columns: list[str] = field(default_factory=list)


def detect_encoding(path: Path) -> str:
    for enc in ("utf-8-sig", "latin-1"):
        try:
            pd.read_csv(path, nrows=50, dtype=str, encoding=enc)
            return enc
        except UnicodeDecodeError:
            continue
    raise IngestError(f"Cannot decode {path} as UTF-8 or Latin-1")


def validate_file(path: Path, spec: DatasetSpec, previous_columns: list[str] | None) -> FileValidation:
    if not path.exists():
        raise IngestError(f"File not found: {path}")
    if path.stat().st_size == 0:
        raise IngestError(f"File is empty: {path}")
    enc = detect_encoding(path)
    try:
        header = pd.read_csv(path, nrows=0, dtype=str, encoding=enc)
    except Exception as err:  # noqa: BLE001
        raise IngestError(f"Not a readable CSV: {path} ({err})") from err
    cols = [normalize_col_name(c) for c in header.columns]
    if not cols:
        raise IngestError("File has no header row")

    missing = [c.name for c in spec.required_columns if not any(s in cols for s in c.sources)]
    result = FileValidation(encoding=enc, source_columns=cols, missing_required=missing)
    if previous_columns is not None:
        result.added_columns = sorted(set(cols) - set(previous_columns))
        result.removed_columns = sorted(set(previous_columns) - set(cols))
    if missing:
        raise IngestError(
            f"{spec.name}: required columns not found: {missing}. "
            f"Columns present: {cols[:25]}{'...' if len(cols) > 25 else ''}"
        )
    return result
