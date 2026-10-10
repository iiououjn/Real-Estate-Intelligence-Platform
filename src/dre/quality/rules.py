"""Row-level data-quality rules.

Every rule returns a boolean Series where True = the row VIOLATES the rule.
  severity 'reject' -> the row is quarantined into the rejected layer (never silently dropped)
  severity 'warn'   -> the row is kept but the violation is counted and reported

Thresholds live in Thresholds so they can be tuned without touching rule code.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Callable

import pandas as pd

from ..config import SQM_TO_SQFT


@dataclass(frozen=True)
class Thresholds:
    min_sale_price_aed: float = 10_000
    max_sale_price_aed: float = 10_000_000_000
    min_psf: float = 100        # AED / sq.ft. -- below this a unit/villa sale is not credible
    max_psf: float = 30_000     # the most expensive Dubai addresses are well under this
    max_area_sqm: float = 1_000_000
    min_annual_rent_aed: float = 1_000
    max_annual_rent_aed: float = 100_000_000
    min_rent_psf: float = 5
    max_rent_psf: float = 1_500
    earliest_date: str = "1990-01-01"
    price_mismatch_tolerance: float = 0.25


@dataclass(frozen=True)
class Rule:
    name: str
    description: str
    severity: str  # reject | warn
    fn: Callable[[pd.DataFrame, "RuleContext"], pd.Series]


@dataclass
class RuleContext:
    source_columns: list[str]
    today: date
    thresholds: Thresholds = Thresholds()


def _isna(df, col):
    return df[col].isna()


def _is_dwelling(df):
    return df["property_type"].isin(["Unit", "Villa"])


# ----------------------------------------------------------------------------- transactions
def _tx_price_impossible(df, ctx):
    t = ctx.thresholds
    sales = df["trans_group"].eq("Sales")
    return sales & (df["amount_aed"].isna() | (df["amount_aed"] < t.min_sale_price_aed) | (df["amount_aed"] > t.max_sale_price_aed))


def _tx_psf_implausible(df, ctx):
    t = ctx.thresholds
    psf = df["amount_aed"] / (df["area_sqm"].where(df["area_sqm"] > 0) * SQM_TO_SQFT)
    return df["trans_group"].eq("Sales") & _is_dwelling(df) & ((psf < t.min_psf) | (psf > t.max_psf))


def _tx_price_mismatch(df, ctx):
    calc = df["amount_aed"] / df["area_sqm"].where(df["area_sqm"] > 0)
    rel = (calc - df["price_per_sqm"]).abs() / df["price_per_sqm"].where(df["price_per_sqm"] > 0)
    return df["trans_group"].eq("Sales") & (rel > ctx.thresholds.price_mismatch_tolerance)


def _missing_property_id(df, ctx):
    # only meaningful if the source file actually carries a property id column
    if "property_id" not in ctx.source_columns and "prop_id" not in ctx.source_columns:
        return pd.Series(False, index=df.index)
    return _isna(df, "property_id")


def _future_or_ancient(date_col):
    def rule(df, ctx):
        d = df[date_col]
        return d.notna() & ((d > pd.Timestamp(ctx.today)) | (d < pd.Timestamp(ctx.thresholds.earliest_date)))
    return rule


def _invalid_area(df, ctx):
    a = df["area_sqm"]
    return a.isna() | (a <= 0) | (a > ctx.thresholds.max_area_sqm)


TRANSACTION_RULES = [
    Rule("missing_transaction_id", "transaction_id is null", "reject", lambda df, c: _isna(df, "transaction_id")),
    Rule("duplicate_transaction_id", "transaction_id already seen earlier in the file", "reject",
         lambda df, c: df["transaction_id"].notna() & df["transaction_id"].duplicated(keep="first")),
    Rule("missing_property_id", "property_id is null (checked only if the column exists in source)", "reject", _missing_property_id),
    Rule("missing_area", "area_name is null", "reject", lambda df, c: _isna(df, "area_name")),
    Rule("missing_transaction_date", "transaction_date missing or unparseable", "reject", lambda df, c: _isna(df, "transaction_date")),
    Rule("date_out_of_range", "transaction_date in the future or before 1990", "reject", _future_or_ancient("transaction_date")),
    Rule("invalid_area", "area_sqm null, zero, negative or absurdly large", "reject", _invalid_area),
    Rule("impossible_sale_price", "Sales row with null, tiny, negative or absurd amount", "reject", _tx_price_impossible),
    Rule("implausible_price_per_sqft", "unit/villa sale outside the plausible AED/sqft band", "reject", _tx_psf_implausible),
    Rule("missing_property_type", "property_type is null", "warn", lambda df, c: _isna(df, "property_type")),
    Rule("price_vs_meter_price_mismatch", "amount/area disagrees with meter_sale_price by >25%", "warn", _tx_price_mismatch),
]

# ----------------------------------------------------------------------------- rentals
def _rent_psf(df):
    return df["annual_amount"] / (df["area_sqm"].where(df["area_sqm"] > 0) * SQM_TO_SQFT)


RENTAL_RULES = [
    Rule("missing_contract_id", "contract_id is null", "reject", lambda df, c: _isna(df, "contract_id")),
    Rule("duplicate_contract_id", "contract_id already seen earlier in the file", "reject",
         lambda df, c: df["contract_id"].notna() & df["contract_id"].duplicated(keep="first")),
    Rule("missing_area", "area_name is null", "reject", lambda df, c: _isna(df, "area_name")),
    Rule("missing_start_date", "contract_start_date missing or unparseable", "reject", lambda df, c: _isna(df, "contract_start_date")),
    Rule("date_out_of_range", "contract_start_date in the future (>1y) or before 1990", "reject",
         lambda df, c: df["contract_start_date"].notna() & ((df["contract_start_date"] > pd.Timestamp(c.today) + pd.Timedelta(days=365))
                                                          | (df["contract_start_date"] < pd.Timestamp(c.thresholds.earliest_date)))),
    Rule("end_before_start", "contract_end_date earlier than contract_start_date", "reject",
         lambda df, c: df["contract_end_date"].notna() & df["contract_start_date"].notna() & (df["contract_end_date"] < df["contract_start_date"])),
    Rule("invalid_area", "area_sqm null, zero, negative or absurdly large", "reject", _invalid_area),
    Rule("impossible_annual_rent", "annual_amount null, tiny, negative or absurd", "reject",
         lambda df, c: df["annual_amount"].isna() | (df["annual_amount"] < c.thresholds.min_annual_rent_aed) | (df["annual_amount"] > c.thresholds.max_annual_rent_aed)),
    Rule("implausible_rent_per_sqft", "unit/villa rent outside the plausible AED/sqft/year band", "reject",
         lambda df, c: _is_dwelling(df) & ((_rent_psf(df) < c.thresholds.min_rent_psf) | (_rent_psf(df) > c.thresholds.max_rent_psf))),
    Rule("missing_property_type", "property_type is null", "warn", lambda df, c: _isna(df, "property_type")),
]

# ----------------------------------------------------------------------------- projects
PROJECT_RULES = [
    Rule("missing_project_name", "project_name is null", "reject", lambda df, c: _isna(df, "project_name")),
    Rule("duplicate_project_name", "project_name already seen earlier in the file", "reject",
         lambda df, c: df["project_name"].notna() & df["project_name"].duplicated(keep="first")),
    Rule("percent_completed_out_of_range", "percent_completed outside 0-100", "reject",
         lambda df, c: df["percent_completed"].notna() & ((df["percent_completed"] < 0) | (df["percent_completed"] > 100))),
    Rule("missing_area", "area_name is null", "warn", lambda df, c: _isna(df, "area_name")),
]

RULES = {"transactions": TRANSACTION_RULES, "rentals": RENTAL_RULES, "projects": PROJECT_RULES}
