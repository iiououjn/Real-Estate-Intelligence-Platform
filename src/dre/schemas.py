"""Data contracts for the three DLD / Dubai Pulse datasets.

Each standardized column lists the *candidate* source column names, in priority order. DLD occasionally
renames columns between exports, so the first candidate present in the file wins. Run
`python -m dre.cli profile <file>` on a fresh download to see which candidates matched. Column names below
follow the public DLD / Dubai Pulse schema as documented, but verify them against your actual download.
"""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class Col:
    name: str
    kind: str                       # str | float | int | date
    sources: tuple[str, ...] = ()
    required: bool = False


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    key: str                        # business key used for duplicate detection
    date_col: str                   # column used for freshness checks
    columns: tuple[Col, ...]

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def required_columns(self) -> list[Col]:
        return [c for c in self.columns if c.required]


TRANSACTIONS = DatasetSpec(
    name="transactions",
    key="transaction_id",
    date_col="transaction_date",
    columns=(
        Col("transaction_id", "str", ("transaction_id", "trans_id"), True),
        Col("trans_group", "str", ("trans_group_en", "trans_group"), True),
        Col("procedure_name", "str", ("procedure_name_en", "procedure_name")),
        Col("transaction_date", "date", ("instance_date", "transaction_date", "trans_date"), True),
        Col("property_id", "str", ("property_id", "prop_id")),
        Col("property_type", "str", ("property_type_en", "property_type"), True),
        Col("property_sub_type", "str", ("property_sub_type_en", "property_sub_type")),
        Col("property_usage", "str", ("property_usage_en", "property_usage")),
        Col("reg_type", "str", ("reg_type_en", "reg_type")),
        Col("area_name", "str", ("area_name_en", "area_en", "area_name"), True),
        Col("building_name", "str", ("building_name_en", "building_name")),
        Col("project_name", "str", ("project_name_en", "project_name")),
        Col("master_project", "str", ("master_project_en", "master_project")),
        Col("nearest_landmark", "str", ("nearest_landmark_en", "nearest_landmark")),
        Col("nearest_metro", "str", ("nearest_metro_en", "nearest_metro")),
        Col("nearest_mall", "str", ("nearest_mall_en", "nearest_mall")),
        Col("rooms", "str", ("rooms_en", "rooms")),
        Col("bedrooms", "int"),  # derived from rooms
        Col("has_parking", "float", ("has_parking",)),
        Col("area_sqm", "float", ("procedure_area", "actual_area", "area_sqm"), True),
        Col("amount_aed", "float", ("actual_worth", "trans_value", "amount"), True),
        Col("price_per_sqm", "float", ("meter_sale_price", "price_per_sqm")),
    ),
)

RENTALS = DatasetSpec(
    name="rentals",
    key="contract_id",
    date_col="contract_start_date",
    columns=(
        Col("contract_id", "str", ("contract_id", "contract_number", "ejari_contract_number"), True),
        Col("contract_reg_type", "str", ("contract_reg_type_en", "reg_type_en", "contract_reg_type")),
        Col("contract_start_date", "date", ("contract_start_date", "start_date"), True),
        Col("contract_end_date", "date", ("contract_end_date", "end_date")),
        Col("property_id", "str", ("property_id",)),
        Col("property_type", "str", ("ejari_property_type_en", "property_type_en", "property_type"), True),
        Col("property_sub_type", "str", ("ejari_property_sub_type_en", "property_sub_type_en")),
        Col("property_usage", "str", ("property_usage_en", "property_usage")),
        Col("area_name", "str", ("area_name_en", "area_en", "area_name"), True),
        Col("project_name", "str", ("project_name_en", "project_name")),
        Col("master_project", "str", ("master_project_en", "master_project")),
        Col("nearest_landmark", "str", ("nearest_landmark_en", "nearest_landmark")),
        Col("nearest_metro", "str", ("nearest_metro_en", "nearest_metro")),
        Col("nearest_mall", "str", ("nearest_mall_en", "nearest_mall")),
        Col("tenant_type", "str", ("tenant_type_en", "tenant_type")),
        Col("rooms", "str", ("rooms_en", "rooms")),
        Col("bedrooms", "int"),  # derived from rooms when present
        Col("area_sqm", "float", ("actual_area", "area_sqm", "procedure_area"), True),
        Col("annual_amount", "float", ("annual_amount", "annual_rent"), True),
        Col("contract_amount", "float", ("contract_amount",)),
    ),
)

PROJECTS = DatasetSpec(
    name="projects",
    key="project_name",
    date_col="start_date",
    columns=(
        Col("project_id", "str", ("project_id", "project_number")),
        Col("project_name", "str", ("project_name_en", "project_name"), True),
        Col("master_project", "str", ("master_project_en", "master_project")),
        Col("area_name", "str", ("area_name_en", "area_en", "area_name")),
        Col("project_status", "str", ("project_status", "project_status_en", "status_en")),
        Col("percent_completed", "float", ("percent_completed", "completion_percentage")),
        Col("start_date", "date", ("project_start_date", "start_date")),
        Col("completion_date", "date", ("completion_date", "end_date", "project_end_date")),
    ),
)

DATASETS: dict[str, DatasetSpec] = {s.name: s for s in (TRANSACTIONS, RENTALS, PROJECTS)}


def cast_frame(df: pd.DataFrame, spec: DatasetSpec) -> pd.DataFrame:
    """Re-apply the spec's dtypes. Needed after a CSV round-trip; harmless after parquet."""
    out = pd.DataFrame(index=df.index)
    for col in spec.columns:
        s = df[col.name] if col.name in df.columns else pd.Series(pd.NA, index=df.index, dtype=object)
        if col.kind == "str":
            out[col.name] = s.astype("string")
        elif col.kind == "float":
            out[col.name] = pd.to_numeric(s, errors="coerce").astype("float64")
        elif col.kind == "int":
            out[col.name] = pd.to_numeric(s, errors="coerce").astype("Int64")
        elif col.kind == "date":
            out[col.name] = pd.to_datetime(s, errors="coerce")
    return out
