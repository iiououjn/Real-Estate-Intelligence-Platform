"""FastAPI service over the warehouse marts.   Run:  uvicorn dre.api.main:app --reload

    GET /health
    GET /estimate?q=Dubai Marina, 2-bedroom, 1,200 sq ft
    GET /estimate/structured?area=Dubai Marina&bedrooms=2&size_sqft=1200
    GET /areas
"""
from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query

from ..analytics.estimate import estimate, parse_query
from ..config import load_settings
from ..warehouse import connect_warehouse

app = FastAPI(title="Dubai Real Estate Market Statistics", version="0.1.0",
              description="Median prices, rental yield and comparable transactions from Dubai Land Department data. "
                          "Statistics only: no ML model is used.")
_settings = load_settings()


def _wh():
    return connect_warehouse(_settings)


@app.get("/health")
def health():
    wh = _wh()
    try:
        return {"status": "ok", "warehouse": wh.name, "transactions": int(wh.scalar("SELECT COUNT(*) FROM fact_transactions") or 0)}
    finally:
        wh.close()


@app.get("/areas")
def areas():
    wh = _wh()
    try:
        return {"areas": wh.df("SELECT area_name FROM dim_area ORDER BY area_name")["area_name"].tolist()}
    finally:
        wh.close()


def _run(area, bedrooms, size_sqft):
    wh = _wh()
    try:
        result = estimate(wh, area, bedrooms, size_sqft)
    finally:
        wh.close()
    if "error" in result:
        raise HTTPException(status_code=404, detail=result)
    return result


@app.get("/estimate")
def estimate_text(q: str = Query(..., description="e.g. 'Dubai Marina, 2-bedroom, 1,200 sq ft'")):
    parsed = parse_query(q)
    if not parsed.area:
        raise HTTPException(status_code=422, detail="Could not find an area name in the query")
    return _run(parsed.area, parsed.bedrooms, parsed.size_sqft)


@app.get("/estimate/structured")
def estimate_structured(area: str, bedrooms: int | None = Query(None, ge=0, le=10), size_sqft: float | None = Query(None, gt=0)):
    return _run(area, bedrooms, size_sqft)
