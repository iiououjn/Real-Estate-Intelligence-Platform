"""Synthetic data in the *shape* of the DLD open datasets, for offline development, tests and CI.

It is NOT real market data. Area names are real DLD area names, prices/volumes are invented.
Defects (duplicates, negative areas, impossible prices, missing ids, ...) are injected on purpose so the
data-quality layer has something to catch. Real data: see README -> "Using real DLD data".
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# (DLD area name, base AED/sqft in early 2021, sales weight, villa share, gross yield, [projects], master project)
AREAS = [
    ("Marsa Dubai", 1450, 0.16, 0.00, 0.066, ["Marina Gate", "Princess Tower", "Marina Promenade", "Al Sahab Tower"], "Dubai Marina"),
    ("Burj Khalifa", 2000, 0.10, 0.00, 0.055, ["Burj Vista", "The Address Residences", "Act One Act Two"], "Downtown Dubai"),
    ("Business Bay", 1250, 0.14, 0.00, 0.070, ["Executive Towers", "Damac Maison", "Peninsula Four", "Binghatti Canal"], "Business Bay"),
    ("Palm Jumeirah", 2300, 0.07, 0.30, 0.048, ["Shoreline Apartments", "Palm Tower", "Signature Villas"], "Palm Jumeirah"),
    ("Al Thanyah Fifth", 1000, 0.10, 0.00, 0.072, ["Lake View", "Jumeirah Bay X3", "Almas Tower"], "Jumeirah Lakes Towers"),
    ("Al Barsha South Fifth", 800, 0.14, 0.02, 0.078, ["Bloom Towers", "Sobha Verde", "Belgravia"], "Jumeirah Village Circle"),
    ("Hadaeq Sheikh Mohammed Bin Rashid", 1350, 0.09, 0.28, 0.058, ["Sidra Villas", "Park Heights", "Golf Place"], "Dubai Hills Estate"),
    ("Al Merkadh", 1400, 0.07, 0.15, 0.060, ["Sobha Hartland", "District One", "Meydan Avenue"], "Mohammed Bin Rashid City"),
    ("Dubai Investment Park Second", 600, 0.06, 0.05, 0.085, ["Green Community", "Mediterranean Cluster"], "Dubai Investments Park"),
    ("Wadi Al Safa 5", 800, 0.07, 0.40, 0.062, ["Akoya Oxygen", "Damac Hills"], "DAMAC Hills"),
]
BEDROOM_MIX_UNIT = (["Studio", "1 B/R", "2 B/R", "3 B/R", "4 B/R"], [0.20, 0.38, 0.28, 0.11, 0.03], [38, 68, 110, 165, 260])
BEDROOM_MIX_VILLA = (["3 B/R", "4 B/R", "5 B/R", "6 B/R"], [0.30, 0.35, 0.28, 0.07], [230, 330, 480, 650])
LANDMARKS = ["Sheikh Zayed Road", "Burj Khalifa", "Dubai Mall", "Palm Jumeirah", "Marina Mall", "Expo 2020 Site"]
METROS = ["Burj Khalifa Metro Station", "DMCC Metro Station", "Business Bay Metro Station", "Damac Properties Metro Station"]
MALLS = ["Dubai Mall", "Mall of the Emirates", "Marina Mall", "Ibn Battuta Mall"]


def _pick(rng, options, n, p=None):
    return np.array(options, dtype=object)[rng.choice(len(options), n, p=p)]


def _growth(days_since_start: np.ndarray) -> np.ndarray:
    """Synthetic market: ~+14%/yr compounding."""
    return 1.14 ** (days_since_start / 365.0)


def generate(out_dir: Path, n_transactions: int = 20000, n_rentals: int = 8000, seed: int = 42, end: date | None = None) -> dict[str, Path]:
    rng = np.random.default_rng(seed)
    end = end or date.today() - timedelta(days=2)
    start = date(2021, 1, 1)
    span = (end - start).days
    out_dir.mkdir(parents=True, exist_ok=True)
    weights = np.array([a[2] for a in AREAS]) / sum(a[2] for a in AREAS)

    def property_block(n):
        area_idx = rng.choice(len(AREAS), n, p=weights)
        is_villa = rng.random(n) < np.array([AREAS[i][3] for i in area_idx])
        rooms, sqm = np.empty(n, dtype=object), np.zeros(n)
        for villa, (labels, probs, sizes_sqm) in ((False, BEDROOM_MIX_UNIT), (True, BEDROOM_MIX_VILLA)):
            m = is_villa == villa
            k = int(m.sum())
            if k:
                idx = rng.choice(len(labels), k, p=probs)
                rooms[m] = np.array(labels, dtype=object)[idx]
                sqm[m] = np.array(sizes_sqm)[idx] * rng.lognormal(0, 0.18, k)
        proj = np.array([rng.choice(AREAS[i][5]) if rng.random() > 0.08 else None for i in area_idx], dtype=object)
        master = np.array([AREAS[i][6] for i in area_idx], dtype=object)
        return area_idx, is_villa, rooms, sqm, proj, master

    # ---------------- transactions ----------------
    n = n_transactions
    area_idx, is_villa, rooms, sqm, proj, master = property_block(n)
    days = (rng.beta(2.0, 1.0, n) * span).astype(int)
    tx_date = np.array([start + timedelta(days=int(d)) for d in days])
    base_psf = np.array([AREAS[i][1] for i in area_idx], dtype=float)
    psf = base_psf * _growth(days) * rng.lognormal(0, 0.14, n) * np.where(is_villa, 0.85, 1.0)
    amount = psf * sqm * 10.7639
    group = _pick(rng, ["Sales", "Mortgages", "Gifts"], n, [0.78, 0.17, 0.05])
    reg = _pick(rng, ["Off-Plan Properties", "Existing Properties"], n, [0.45, 0.55])
    procedure = np.where(group == "Sales", np.where(reg == "Off-Plan Properties", "Sell - Pre registration", "Sell"),
                         np.where(group == "Mortgages", "Mortgage Registration", "Gift"))
    amount = np.where(group == "Gifts", 0.0, amount)  # gifts carry no price in DLD data
    tx = pd.DataFrame({
        "transaction_id": [f"1-11-{d.year}-{i:07d}" for i, d in enumerate(tx_date)],
        "trans_group_en": group, "procedure_name_en": procedure,
        "instance_date": [d.strftime("%d-%m-%Y") for d in tx_date],   # DLD-style dd-mm-yyyy
        "property_id": rng.integers(10**9, 10**9 + n // 2, n).astype(str),  # resales share a property id
        "property_type_en": np.where(is_villa, "Villa", "Unit"),
        "property_sub_type_en": np.where(is_villa, "Villa", "Flat"),
        "property_usage_en": "Residential", "reg_type_en": reg,
        "area_name_en": [AREAS[i][0] for i in area_idx], "building_name_en": proj,
        "project_name_en": proj, "master_project_en": master,
        "nearest_landmark_en": np.where(rng.random(n) < 0.2, None, _pick(rng, LANDMARKS, n)),
        "nearest_metro_en": np.where(rng.random(n) < 0.3, None, _pick(rng, METROS, n)),
        "nearest_mall_en": np.where(rng.random(n) < 0.3, None, _pick(rng, MALLS, n)),
        "rooms_en": rooms, "has_parking": rng.integers(0, 2, n),
        "procedure_area": np.round(sqm, 2), "actual_worth": np.round(amount, 2),
        "meter_sale_price": np.round(np.where(sqm > 0, amount / np.maximum(sqm, 1e-9), 0), 2),
    })
    tx = _inject_transaction_defects(tx, rng)

    # ---------------- rentals (Ejari-like) ----------------
    m = n_rentals
    a2, v2, r2, sqm2, proj2, master2 = property_block(m)
    d2 = (rng.beta(2.0, 1.0, m) * span).astype(int)
    st = np.array([start + timedelta(days=int(d)) for d in d2])
    yld = np.array([AREAS[i][4] for i in a2]) * rng.lognormal(0, 0.12, m)
    price_psf = np.array([AREAS[i][1] for i in a2], dtype=float) * _growth(d2) * np.where(v2, 0.85, 1.0)
    rent = price_psf * sqm2 * 10.7639 * yld
    rentals = pd.DataFrame({
        "contract_id": [f"EJ-{s.year}-{i:07d}" for i, s in enumerate(st)],
        "contract_reg_type_en": _pick(rng, ["New", "Renew"], m, [0.45, 0.55]),
        "contract_start_date": [s.strftime("%d-%m-%Y") for s in st],
        "contract_end_date": [(s + timedelta(days=364)).strftime("%d-%m-%Y") for s in st],
        "property_id": rng.integers(10**9, 10**9 + m, m).astype(str),
        "ejari_property_type_en": np.where(v2, "Villa", "Unit"),
        "ejari_property_sub_type_en": np.where(v2, "Villa", "Flat"), "property_usage_en": "Residential",
        "area_name_en": [AREAS[i][0] for i in a2], "project_name_en": proj2, "master_project_en": master2,
        "nearest_landmark_en": _pick(rng, LANDMARKS, m), "nearest_metro_en": _pick(rng, METROS, m),
        "nearest_mall_en": _pick(rng, MALLS, m), "tenant_type_en": _pick(rng, ["Person", "Company"], m, [0.9, 0.1]),
        "actual_area": np.round(sqm2, 2), "annual_amount": np.round(rent, 2), "contract_amount": np.round(rent, 2),
    })
    rentals = _inject_rental_defects(rentals, rng)

    # ---------------- projects ----------------
    rows = []
    for area, _, _, _, _, projects, mp in AREAS:
        for p in projects:
            done = float(rng.choice([100, 100, 100, 85, 60, 35, 10]))
            s = start - timedelta(days=int(rng.integers(200, 1600)))
            rows.append({"project_id": f"P{len(rows) + 1:05d}", "project_name_en": p, "master_project_en": mp,
                         "area_name_en": area, "project_status": "FINISHED" if done == 100 else "ACTIVE",
                         "percent_completed": done, "project_start_date": s.isoformat(),
                         "completion_date": (s + timedelta(days=900)).isoformat()})
    projects_df = pd.DataFrame(rows)

    paths = {"transactions": out_dir / "transactions.csv", "rentals": out_dir / "rentals.csv", "projects": out_dir / "projects.csv"}
    for name, df in (("transactions", tx), ("rentals", rentals), ("projects", projects_df)):
        df.to_csv(paths[name], index=False)
    return paths


def _inject_transaction_defects(tx: pd.DataFrame, rng) -> pd.DataFrame:
    n = len(tx)
    tx["actual_worth"] = tx["actual_worth"].astype(object)
    tx["property_id"] = tx["property_id"].astype(object)
    pick = lambda frac: rng.choice(n, max(1, int(n * frac)), replace=False)  # noqa: E731
    tx.loc[pick(0.004), "transaction_id"] = None                                   # missing id
    tx.loc[pick(0.004), "procedure_area"] = -abs(tx["procedure_area"].iloc[0])     # negative area
    tx.loc[pick(0.003), "procedure_area"] = 0                                      # zero area
    bad_price = pick(0.004)
    tx.loc[bad_price, "actual_worth"] = rng.choice([0, 1, -500000, 9e12], len(bad_price))  # impossible prices
    tx.loc[pick(0.003), "area_name_en"] = None                                     # missing area
    tx.loc[pick(0.003), "property_id"] = None                                      # missing property id
    tx.loc[pick(0.002), "instance_date"] = (date.today() + timedelta(days=90)).strftime("%d-%m-%Y")  # future date
    tx.loc[pick(0.002), "actual_worth"] = "not-a-number"                           # type coercion failure
    dupes = tx.iloc[rng.choice(n, max(1, int(n * 0.006)), replace=False)]
    return pd.concat([tx, dupes], ignore_index=True)                               # duplicate transactions


def _inject_rental_defects(r: pd.DataFrame, rng) -> pd.DataFrame:
    n = len(r)
    pick = lambda frac: rng.choice(n, max(1, int(n * frac)), replace=False)  # noqa: E731
    r.loc[pick(0.004), "contract_id"] = None
    r.loc[pick(0.004), "annual_amount"] = -1000
    r.loc[pick(0.003), "actual_area"] = 0
    r.loc[pick(0.003), "area_name_en"] = None
    end_before = pick(0.003)
    r.loc[end_before, "contract_end_date"] = "01-01-2019"                          # ends before it starts
    dupes = r.iloc[rng.choice(n, max(1, int(n * 0.005)), replace=False)]
    return pd.concat([r, dupes], ignore_index=True)
