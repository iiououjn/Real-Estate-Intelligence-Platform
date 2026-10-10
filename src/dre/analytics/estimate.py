"""Market-statistics lookup behind the API: "Dubai Marina, 2-bedroom, 1,200 sq ft".

This is deliberately NOT machine learning. It returns robust statistics (medians, quartiles) over registered
DLD transactions, a gross rental yield, and the closest comparable transactions. `indicative_value_aed` is
median AED/sqft x size: a market-statistics figure, not a valuation.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd

from ..config import SQM_TO_SQFT
from ..warehouse import Warehouse

MIN_SAMPLE = 10
DISCLAIMER = ("Statistical summary of registered Dubai Land Department transactions. Not a valuation, "
              "not financial advice, and not a machine-learning prediction.")


@dataclass
class ParsedQuery:
    area: str | None
    bedrooms: int | None
    size_sqft: float | None


_SQFT = re.compile(r"([\d][\d,]*\.?\d*)\s*(?:sq\.?\s*ft\.?|sqft|square\s*(?:feet|foot)|sf)\b", re.I)
_SQM = re.compile(r"([\d][\d,]*\.?\d*)\s*(?:sq\.?\s*m\.?|sqm|square\s*met(?:er|re)s?|m2)\b", re.I)
_BED = re.compile(r"(\d+)\s*[- ]?\s*(?:bed(?:room)?s?|br|b/r|bhk)\b", re.I)


def parse_query(text: str) -> ParsedQuery:
    """'Dubai Marina, 2-bedroom, 1,200 sq ft' -> area='Dubai Marina', bedrooms=2, size_sqft=1200."""
    size = None
    if m := _SQFT.search(text):
        size = float(m.group(1).replace(",", ""))
    elif m := _SQM.search(text):
        size = float(m.group(1).replace(",", "")) * SQM_TO_SQFT
    bedrooms = 0 if re.search(r"\bstudio\b", text, re.I) else None
    if m := _BED.search(text):
        bedrooms = int(m.group(1))
    parts = [p.strip() for p in re.split(r"[,;|]", text) if p.strip()]
    area = None
    for p in parts:
        if not (_SQFT.search(p) or _SQM.search(p) or _BED.search(p) or re.fullmatch(r"studio", p, re.I)):
            area = p
            break
    return ParsedQuery(area=area, bedrooms=bedrooms, size_sqft=size)


def resolve_area(wh: Warehouse, name: str) -> tuple[str | None, list[str]]:
    """Alias table -> exact (case-insensitive) match on dim_area -> contains match. Returns (area, suggestions)."""
    key = name.strip().lower()
    hit = wh.df("SELECT area_name FROM area_alias WHERE alias = ?", [key])
    if len(hit):
        return str(hit.iloc[0, 0]), []
    hit = wh.df("SELECT area_name FROM dim_area WHERE LOWER(area_name) = ?", [key])
    if len(hit):
        return str(hit.iloc[0, 0]), []
    like = wh.df("SELECT area_name FROM dim_area WHERE LOWER(area_name) LIKE ? ORDER BY area_name", [f"%{key}%"])
    if len(like) == 1:
        return str(like.iloc[0, 0]), []
    return None, [str(x) for x in like["area_name"].head(8)]


def _round(x, n=0):
    return None if x is None or pd.isna(x) else round(float(x), n)


def estimate(wh: Warehouse, area: str, bedrooms: int | None, size_sqft: float | None, months: int = 12, comps: int = 10) -> dict:
    resolved, suggestions = resolve_area(wh, area)
    if resolved is None:
        return {"error": "area_not_found", "input_area": area, "suggestions": suggestions}

    max_day = wh.scalar("SELECT MAX(d.day_num) FROM fact_transactions f JOIN dim_date d ON d.date_key = f.date_key")
    if max_day is None:
        return {"error": "no_data", "detail": "fact_transactions is empty; run the pipeline first"}
    cutoff = int(max_day) - int(months * 30.4375)

    def fetch(with_bedrooms: bool) -> pd.DataFrame:
        sql = ("SELECT f.transaction_id, d.full_date AS transaction_date, a.area_name, pr.property_type, pr.bedrooms, "
               "p.project_name, f.area_sqft, f.amount_aed, f.price_per_sqft, pr.reg_type "
               "FROM fact_transactions f JOIN dim_date d ON d.date_key = f.date_key "
               "JOIN dim_area a ON a.area_key = f.area_key JOIN dim_project p ON p.project_key = f.project_key "
               "JOIN dim_property pr ON pr.property_key = f.property_key "
               "WHERE f.trans_group = 'Sales' AND f.price_per_sqft IS NOT NULL AND a.area_name = ? AND d.day_num > ?")
        params: list = [resolved, cutoff]
        if with_bedrooms:
            sql += " AND pr.bedrooms = ?"
            params.append(int(bedrooms))
        return wh.df(sql, params)

    basis = "area_and_bedrooms"
    df = fetch(True) if bedrooms is not None else pd.DataFrame()
    if bedrooms is None or len(df) < MIN_SAMPLE:
        basis = "area_only" if bedrooms is not None else "area_all_bedrooms"
        df = fetch(False)
    if df.empty:
        return {"error": "no_recent_transactions", "resolved_area": resolved, "window_months": months}

    psf = df["price_per_sqft"].astype(float)
    q1, med, q3 = np.percentile(psf, [25, 50, 75])
    sales = {"transactions": int(len(df)), "median_price_per_sqft_aed": _round(med), "p25_price_per_sqft_aed": _round(q1),
             "p75_price_per_sqft_aed": _round(q3), "median_price_aed": _round(df["amount_aed"].median()),
             "sample_is_small": bool(len(df) < MIN_SAMPLE)}
    indicative = None
    if size_sqft:
        indicative = {"median_aed": _round(med * size_sqft), "interquartile_range_aed": [_round(q1 * size_sqft), _round(q3 * size_sqft)],
                      "method": "median AED/sqft x size; not a valuation"}

    y = wh.df("SELECT property_type, sales_count, median_sale_psf, rental_count, median_annual_rent_psf, gross_yield_pct, as_of_date "
              "FROM mart_rental_yield WHERE area_name = ? ORDER BY sales_count DESC", [resolved])
    rental = None
    if len(y):
        row = y[y["property_type"] == "Unit"].iloc[0] if (y["property_type"] == "Unit").any() else y.iloc[0]
        rental = {"property_type": row["property_type"], "median_annual_rent_per_sqft_aed": _round(row["median_annual_rent_psf"], 1),
                  "gross_yield_pct": _round(row["gross_yield_pct"], 2), "rental_contracts": None if pd.isna(row["rental_count"]) else int(row["rental_count"]),
                  "basis": "area + property type, trailing 12 months; rentals carry no bedroom count"}

    comp_df = df.copy()
    if size_sqft:
        comp_df["size_gap"] = (comp_df["area_sqft"] - size_sqft).abs()
        comp_df = comp_df.sort_values(["size_gap", "transaction_date"], ascending=[True, False])
    else:
        comp_df = comp_df.sort_values("transaction_date", ascending=False)
    comparables = [{"transaction_id": r.transaction_id, "date": str(r.transaction_date)[:10], "project": r.project_name,
                    "bedrooms": None if pd.isna(r.bedrooms) or r.bedrooms < 0 else int(r.bedrooms), "size_sqft": _round(r.area_sqft),
                    "price_aed": _round(r.amount_aed), "price_per_sqft_aed": _round(r.price_per_sqft), "registration": r.reg_type}
                   for r in comp_df.head(comps).itertuples()]

    return {"input": {"area": area, "bedrooms": bedrooms, "size_sqft": size_sqft}, "resolved_area": resolved,
            "window": {"months": months, "basis": basis}, "sales": sales, "indicative_value": indicative,
            "rental": rental, "comparables": comparables, "disclaimer": DISCLAIMER,
            "generated_on": date.today().isoformat()}
