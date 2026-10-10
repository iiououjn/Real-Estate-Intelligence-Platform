"""Column standardisation: rename, trim, null-normalise, type-coerce, derive bedrooms."""
from __future__ import annotations

import re
from dataclasses import dataclass, field

import pandas as pd

from ..schemas import DatasetSpec

NULL_TOKENS = {"", "na", "n/a", "null", "none", "nan", "nil", "-", "--"}
DATE_FORMATS = (
    "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d",
    "%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f",
)


@dataclass
class StandardizeReport:
    rows_in: int = 0
    mapped: dict[str, str] = field(default_factory=dict)          # standard col -> source col
    missing_required: list[str] = field(default_factory=list)
    unmapped_source_columns: list[str] = field(default_factory=list)
    coerced_to_null: dict[str, int] = field(default_factory=dict)  # values that failed to parse

    def merge(self, other: "StandardizeReport") -> None:
        self.rows_in += other.rows_in
        self.mapped.update(other.mapped)
        self.missing_required = sorted(set(self.missing_required) | set(other.missing_required))
        self.unmapped_source_columns = sorted(set(self.unmapped_source_columns) | set(other.unmapped_source_columns))
        for k, v in other.coerced_to_null.items():
            self.coerced_to_null[k] = self.coerced_to_null.get(k, 0) + v


def normalize_col_name(name: str) -> str:
    return re.sub(r"[^0-9a-z]+", "_", str(name).strip().lower()).strip("_")


def parse_dates(s: pd.Series) -> pd.Series:
    """Try several explicit formats (DLD exports dd-mm-yyyy; APIs give ISO). Unparseable -> NaT."""
    s = s.astype("string")
    out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    remaining = s.notna()
    for fmt in DATE_FORMATS:
        if not remaining.any():
            break
        parsed = pd.to_datetime(s[remaining], format=fmt, errors="coerce")
        ok = parsed.notna()
        if ok.any():
            out.loc[parsed.index[ok]] = parsed[ok].astype("datetime64[ns]")
            remaining.loc[parsed.index[ok]] = False
    return out.dt.normalize()


def parse_bedrooms(rooms: pd.Series) -> pd.Series:
    """'2 B/R' -> 2, 'Studio' -> 0, anything else ('PENTHOUSE', 'Office'...) -> <NA>."""
    txt = rooms.astype("string").str.lower()
    num = pd.to_numeric(txt.str.extract(r"(\d+)\s*(?:b/?r|bed)")[0], errors="coerce")
    num = num.mask(txt.str.contains("studio", na=False), 0)
    return num.astype("Int64")


def _clean_text(s: pd.Series) -> pd.Series:
    s = s.astype("string").str.strip().str.replace(r"\s+", " ", regex=True)
    return s.mask(s.str.lower().isin(NULL_TOKENS))


def standardize_frame(raw: pd.DataFrame, spec: DatasetSpec) -> tuple[pd.DataFrame, StandardizeReport]:
    raw = raw.copy()
    raw.columns = [normalize_col_name(c) for c in raw.columns]
    raw = raw.reset_index(drop=True)
    report = StandardizeReport(rows_in=len(raw))
    out = pd.DataFrame(index=raw.index)
    used: set[str] = set()

    for col in spec.columns:
        if col.name == "bedrooms":
            continue  # derived below
        src = next((c for c in col.sources if c in raw.columns), None)
        if src is None:
            if col.required:
                report.missing_required.append(col.name)
            out[col.name] = pd.Series(pd.NA, index=raw.index, dtype="string" if col.kind == "str" else object)
            continue
        used.add(src)
        report.mapped[col.name] = src
        text = _clean_text(raw[src])
        if col.kind == "str":
            out[col.name] = text
        elif col.kind == "float":
            numeric = pd.to_numeric(text.str.replace(",", "", regex=False), errors="coerce").astype("float64")
            bad = int(text.notna().sum() - numeric.notna().sum())
            if bad:
                report.coerced_to_null[col.name] = bad
            out[col.name] = numeric
        elif col.kind == "date":
            parsed = parse_dates(text)
            bad = int(text.notna().sum() - parsed.notna().sum())
            if bad:
                report.coerced_to_null[col.name] = bad
            out[col.name] = parsed
        else:
            out[col.name] = pd.to_numeric(text, errors="coerce").astype("Int64")

    out["bedrooms"] = parse_bedrooms(out["rooms"]) if "rooms" in out else pd.Series(pd.NA, index=out.index, dtype="Int64")
    report.unmapped_source_columns = sorted(set(raw.columns) - used)
    return out[spec.column_names], report
