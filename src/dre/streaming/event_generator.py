"""Simulated marketplace traffic. Sessions follow a funnel: search -> views -> (save) -> (agent contact).

Community popularity is skewed on purpose (Marina/Downtown/Business Bay get most traffic) so the demand
mart has a signal to find. A small share of events is deliberately malformed to exercise the dead-letter path.
"""
from __future__ import annotations

import json
import random
import uuid
from datetime import datetime, timedelta, timezone

# marketplace community -> (traffic weight, base price AED/sqft, property types)
COMMUNITIES = {
    "Dubai Marina": (0.18, 2100), "Downtown Dubai": (0.15, 2900), "Business Bay": (0.14, 1900),
    "Palm Jumeirah": (0.09, 3300), "JLT": (0.08, 1500), "JVC": (0.13, 1100), "Dubai Hills Estate": (0.10, 2000),
    "MBR City": (0.06, 2000), "Dubai Investment Park": (0.03, 900), "Damac Hills": (0.04, 1300),
}
BEDROOM_WEIGHTS = {0: 0.12, 1: 0.32, 2: 0.30, 3: 0.14, 4: 0.08, 5: 0.04}
DEVICES = ["ios", "android", "web"]


class EventGenerator:
    def __init__(self, seed: int | None = 7, bad_event_rate: float = 0.01):
        self.rng = random.Random(seed)
        self.bad_rate = bad_event_rate
        self.names = list(COMMUNITIES)
        self.weights = [COMMUNITIES[n][0] for n in self.names]

    def _event(self, etype: str, session: dict, ts: datetime, **extra) -> dict:
        return {"event_id": str(uuid.UUID(int=self.rng.getrandbits(128), version=4)), "event_type": etype,
                "event_ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"), "session_id": session["session_id"],
                "user_id": session["user_id"], "device": session["device"], **extra}

    def session(self, ts: datetime) -> list[dict]:
        r = self.rng
        area = r.choices(self.names, self.weights)[0]
        beds = r.choices(list(BEDROOM_WEIGHTS), list(BEDROOM_WEIGHTS.values()))[0]
        ptype = "Villa" if beds >= 4 and r.random() < 0.6 else "Apartment"
        base_psf = COMMUNITIES[area][1]
        sess = {"session_id": f"s-{r.getrandbits(48):012x}", "user_id": f"u-{r.randint(1, 50000):06d}", "device": r.choice(DEVICES)}
        events = [self._event("property_searched", sess, ts, area_name=area, bedrooms=beds, property_type=ptype,
                              search_query=f"{beds if beds else 'studio'} bedroom {ptype.lower()} {area}")]
        t = ts
        listings = []
        for _ in range(r.choices([1, 2, 3, 5, 8], [0.25, 0.3, 0.25, 0.15, 0.05])[0]):
            t += timedelta(seconds=r.randint(5, 90))
            size = round((450 + 350 * beds) * r.uniform(0.8, 1.25))
            listing = {"listing_id": f"L{r.randint(1, 250000):07d}", "price_aed": round(base_psf * size * r.uniform(0.85, 1.2)),
                       "size_sqft": size}
            listings.append(listing)
            events.append(self._event("property_viewed", sess, t, area_name=area, bedrooms=beds, property_type=ptype, **listing))
        if listings and r.random() < 0.22:
            t += timedelta(seconds=r.randint(5, 60))
            events.append(self._event("listing_saved", sess, t, area_name=area, bedrooms=beds, property_type=ptype, **listings[-1]))
            if r.random() < 0.30:
                t += timedelta(seconds=r.randint(20, 300))
                events.append(self._event("agent_contacted", sess, t, area_name=area, bedrooms=beds, property_type=ptype,
                                          agent_id=f"A{r.randint(1, 4000):05d}", **listings[-1]))
        return events

    def _corrupt(self, event: dict) -> dict | str:
        kind = self.rng.choice(["bad_type", "no_session", "neg_price", "bad_ts", "not_json"])
        if kind == "not_json":
            return "{this is not json"
        e = dict(event)
        if kind == "bad_type":
            e["event_type"] = "property_teleported"
        elif kind == "no_session":
            e.pop("session_id", None)
        elif kind == "neg_price":
            e["price_aed"] = -1
        else:
            e["event_ts"] = "yesterday-ish"
        return e

    def stream(self, n_sessions: int, start: datetime | None = None, spread_minutes: float = 60.0, duplicate_rate: float = 0.005):
        """Yield encoded JSON strings (what a producer would put on Kinesis). `start` is where sessions begin;
        they are spread over `spread_minutes` so a run produces a realistic time series."""
        start = start or datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=spread_minutes)
        last = None
        for i in range(n_sessions):
            ts = start + timedelta(seconds=self.rng.uniform(0, spread_minutes * 60))
            for ev in self.session(ts):
                out = self._corrupt(ev) if self.rng.random() < self.bad_rate else ev
                yield out if isinstance(out, str) else json.dumps(out)
                if last is not None and self.rng.random() < duplicate_rate:   # at-least-once delivery
                    yield json.dumps(last)
                last = ev
