"""Static calendar dimension. Also carries day_num and prior-period month starts so SQL models never need
vendor-specific date arithmetic."""
from __future__ import annotations

import pandas as pd


def build_dim_date(start: str = "2000-01-01", end: str = "2035-12-31") -> pd.DataFrame:
    days = pd.date_range(start, end, freq="D")
    df = pd.DataFrame({"full_date": days})
    df["date_key"] = df.full_date.dt.year * 10000 + df.full_date.dt.month * 100 + df.full_date.dt.day
    df["day_num"] = (df.full_date - pd.Timestamp("1970-01-01")).dt.days
    df["year"] = df.full_date.dt.year
    df["quarter"] = df.full_date.dt.quarter
    df["month"] = df.full_date.dt.month
    df["month_name"] = df.full_date.dt.strftime("%B")
    df["month_start"] = df.full_date.dt.to_period("M").dt.to_timestamp()
    df["prev_month_start"] = df.month_start - pd.DateOffset(months=1)
    df["prev_year_month_start"] = df.month_start - pd.DateOffset(years=1)
    df["day_of_week"] = df.full_date.dt.dayofweek + 1          # 1 = Monday
    df["is_weekend"] = (df.day_of_week >= 6).astype(int)        # Sat/Sun
    return df[["date_key", "full_date", "day_num", "year", "quarter", "month", "month_name", "month_start",
               "prev_month_start", "prev_year_month_start", "day_of_week", "is_weekend"]]
