import base64
import gzip
import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from dre.streaming.lambda_handler import LocalSink, handler, validate_event

NOW = datetime(2026, 9, 25, 12, 0, 0)


def good(**kw):
    e = {"event_id": "e1", "event_type": "property_viewed", "event_ts": "2026-09-25T11:59:00Z", "session_id": "s1",
         "listing_id": "L1", "area_name": "Dubai Marina", "bedrooms": 2, "price_aed": 1_500_000}
    e.update(kw)
    return e


def kinesis(payloads):
    return {"Records": [{"kinesis": {"data": base64.b64encode((p if isinstance(p, str) else json.dumps(p)).encode()).decode(),
                                     "sequenceNumber": f"{i:04d}"}} for i, p in enumerate(payloads)]}


class BrokenSink:
    def write(self, key, records):
        raise RuntimeError("s3 down")


class HandlerTests(unittest.TestCase):
    def test_validation_rules(self):
        self.assertEqual(validate_event(good(), NOW), [])
        self.assertIn("unknown_event_type:x", validate_event(good(event_type="x"), NOW))
        self.assertIn("missing_session_id", validate_event(good(session_id=None), NOW))
        self.assertIn("invalid_price_aed", validate_event(good(price_aed=-1), NOW))
        self.assertIn("invalid_event_ts", validate_event(good(event_ts="soon"), NOW))
        self.assertIn("event_ts_in_future", validate_event(good(event_ts="2026-09-26T00:00:00Z"), NOW))
        self.assertIn("missing_listing_id", validate_event(good(listing_id=None), NOW))
        self.assertIn("missing_agent_id", validate_event(good(event_type="agent_contacted"), NOW))
        self.assertIn("invalid_bedrooms", validate_event(good(bedrooms=99), NOW))

    def test_valid_events_are_partitioned_and_invalid_go_to_dead_letter(self):
        with tempfile.TemporaryDirectory() as d:
            payloads = [good(event_id="a"), good(event_id="b", event_ts="2026-09-25T10:05:00+04:00"),
                        good(event_id="a"),                       # duplicate in batch
                        "{not json", good(event_id="c", event_type="nope")]
            out = handler(kinesis(payloads), sink=LocalSink(d), now=NOW)
            self.assertEqual(out["stats"], {"received": 5, "valid": 2, "rejected": 3})
            self.assertEqual(out["batchItemFailures"], [])
            files = sorted(str(p.relative_to(d)) for p in Path(d).rglob("*.jsonl.gz"))
            self.assertTrue(any(f.startswith("events/dt=2026-09-25/hr=11/") for f in files))
            self.assertTrue(any(f.startswith("events/dt=2026-09-25/hr=06/") for f in files))   # 10:05+04:00 -> 06:05 UTC
            self.assertTrue(any(f.startswith("events_rejected/") for f in files))
            rej = next(p for p in Path(d).rglob("*.jsonl.gz") if "events_rejected" in str(p))
            reasons = [json.loads(l)["reason"] for l in gzip.decompress(rej.read_bytes()).decode().splitlines()]
            self.assertIn("malformed_json", reasons)
            self.assertIn("duplicate_event_id_in_batch", reasons)

    def test_enrichment_fields(self):
        with tempfile.TemporaryDirectory() as d:
            handler(kinesis([good()]), sink=LocalSink(d), now=NOW)
            f = next(Path(d).rglob("*.jsonl.gz"))
            rec = json.loads(gzip.decompress(f.read_bytes()).decode().splitlines()[0])
            self.assertEqual((rec["event_date"], rec["event_hour"], rec["schema_version"]), ("2026-09-25", 11, 1))
            self.assertEqual(rec["event_ts"], "2026-09-25 11:59:00")

    def test_sink_failure_requests_retry_from_first_record(self):
        out = handler(kinesis([good(event_id="a"), good(event_id="b")]), sink=BrokenSink(), now=NOW)
        self.assertEqual(out["batchItemFailures"], [{"itemIdentifier": "0000"}])


if __name__ == "__main__":
    unittest.main()
