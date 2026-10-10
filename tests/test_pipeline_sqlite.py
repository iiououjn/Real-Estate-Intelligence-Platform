"""End-to-end: synthetic DLD files -> ingest -> quality -> star schema -> marts -> streaming -> API service.
Runs on the SQLite backend so CI needs nothing but pandas."""
import tempfile
import unittest
from pathlib import Path

from dre.analytics.estimate import estimate, resolve_area
from dre.etl.dim_date import build_dim_date
from dre.etl.pipeline import init_warehouse, run_transform
from dre.ingestion.pipeline import ingest_dataset
from dre.ingestion.sample_data import generate
from dre.ingestion.validate import IngestError
from dre.schemas import DATASETS
from dre.storage import Storage
from dre.streaming.event_generator import EventGenerator
from dre.streaming.lambda_handler import LocalSink
from dre.streaming.loader import load_events
from dre.streaming.producer import LocalProducer
from dre.warehouse import SQLiteWarehouse
from tests.helpers import make_settings


class PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.settings = make_settings(cls.tmp.name, max_reject_pct=20.0)
        cls.storage = Storage(cls.settings)
        cls.wh = SQLiteWarehouse(cls.settings, ":memory:")
        cls.files = generate(Path(cls.tmp.name) / "sample", 4000, 1800, seed=1)
        for ds, path in cls.files.items():
            cls.summary = ingest_dataset(cls.settings, cls.storage, ds, str(path), "run-1")
        cls.results = {r["dataset"]: r for r in run_transform(cls.settings, cls.storage, cls.wh, "run-1")}

    @classmethod
    def tearDownClass(cls):
        cls.wh.close()
        cls.tmp.cleanup()

    def test_raw_layer_is_untouched_copy_of_source(self):
        keys = self.storage.list_keys("raw/transactions/")
        self.assertEqual(len(keys), 1)
        self.assertEqual(self.storage.get_bytes(keys[0]), self.files["transactions"].read_bytes())

    def test_defects_are_quarantined_not_dropped(self):
        r = self.results["transactions"]
        self.assertGreater(r["rows_rejected"], 0)
        self.assertEqual(r["rows_in"], r["rows_clean"] + r["rows_rejected"])
        n_rej = self.wh.scalar("SELECT COUNT(*) FROM dq_rejected_records WHERE dataset = 'transactions'")
        self.assertEqual(n_rej, r["rows_rejected"])
        reasons = set(self.wh.df("SELECT reject_reasons FROM dq_rejected_records")["reject_reasons"])
        joined = ";".join(reasons)
        for expected in ("duplicate_transaction_id", "missing_transaction_id", "invalid_area", "impossible_sale_price", "missing_area"):
            self.assertIn(expected, joined)

    def test_star_schema_reconciles_and_has_no_orphans(self):
        self.assertEqual(self.wh.scalar("SELECT COUNT(*) FROM fact_transactions"), self.results["transactions"]["rows_clean"])
        self.assertEqual(self.wh.scalar("SELECT COUNT(*) FROM fact_rentals"), self.results["rentals"]["rows_clean"])
        bad = self.wh.df("SELECT check_name, failed_count FROM dq_run_log WHERE dataset = 'warehouse' AND status = 'FAIL'")
        self.assertTrue(bad.empty, bad.to_string())

    def test_marts_are_populated_and_plausible(self):
        y = self.wh.df("SELECT * FROM mart_rental_yield WHERE gross_yield_pct IS NOT NULL")
        self.assertFalse(y.empty)
        self.assertTrue(y["gross_yield_pct"].between(2, 15).all(), y["gross_yield_pct"].describe())
        t = self.wh.df("SELECT * FROM mart_price_trends_monthly")
        self.assertFalse(t.empty)
        self.assertTrue(t["yoy_change_pct"].notna().any())
        self.assertTrue((t["median_psf"] > 0).all())
        self.assertFalse(self.wh.df("SELECT * FROM mart_bedroom_popularity").empty)
        health = self.wh.df("SELECT * FROM mart_pipeline_health")
        self.assertIn("transactions", set(health["dataset"]))

    def test_ddl_columns_match_data_contract(self):
        for name, spec in DATASETS.items():
            cols = list(self.wh.df(f"SELECT * FROM stg_{name} LIMIT 0").columns)
            self.assertEqual(cols, spec.column_names, name)

    def test_schema_drift_and_bad_files_are_detected(self):
        bad = Path(self.tmp.name) / "bad.csv"
        bad.write_text("foo,bar\n1,2\n")
        with self.assertRaises(IngestError):
            ingest_dataset(self.settings, self.storage, "transactions", str(bad), "run-x")
        empty = Path(self.tmp.name) / "empty.csv"
        empty.write_text("")
        with self.assertRaises(IngestError):
            ingest_dataset(self.settings, self.storage, "transactions", str(empty), "run-x")

    def test_streaming_load_is_idempotent_and_feeds_demand_mart(self):
        sink = LocalSink(self.storage.root)
        stats = LocalProducer(sink).send(EventGenerator(seed=3, bad_event_rate=0.02).stream(300, spread_minutes=2000))
        self.assertGreater(stats["rejected"], 0)
        first = load_events(self.settings, self.storage, self.wh, full=True)
        self.assertEqual(first["new_events"], first["total_events"])
        second = load_events(self.settings, self.storage, self.wh, full=True)
        self.assertEqual(second["new_events"], 0)                       # replay adds nothing
        self.assertEqual(self.wh.scalar("SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM fact_events"), 0)
        d = self.wh.df("SELECT * FROM mart_area_demand WHERE views_30d > 0 ORDER BY demand_rank")
        self.assertFalse(d.empty)
        unresolved = self.wh.scalar("SELECT COUNT(*) FROM fact_events WHERE dld_area_name NOT IN (SELECT area_name FROM dim_area)")
        self.assertEqual(unresolved, 0)                                 # every community maps to a DLD area

    def test_estimate_service(self):
        area, _ = resolve_area(self.wh, "Dubai Marina")
        self.assertEqual(area, "Marsa Dubai")
        res = estimate(self.wh, "Dubai Marina", 2, 1200)
        self.assertEqual(res["resolved_area"], "Marsa Dubai")
        self.assertGreater(res["sales"]["median_price_per_sqft_aed"], 0)
        self.assertEqual(len(res["comparables"]), 10)
        self.assertIsNotNone(res["rental"]["gross_yield_pct"])
        self.assertIn("Not a valuation", res["disclaimer"])
        self.assertEqual(estimate(self.wh, "Atlantis", 2, 1000)["error"], "area_not_found")

    def test_dim_date_lookup_columns(self):
        d = build_dim_date("2024-03-15", "2024-03-15").iloc[0]
        self.assertEqual((d.date_key, d.month_start.strftime("%Y-%m-%d"), d.prev_month_start.strftime("%Y-%m-%d"),
                          d.prev_year_month_start.strftime("%Y-%m-%d")), (20240315, "2024-03-01", "2024-02-01", "2023-03-01"))


if __name__ == "__main__":
    unittest.main()
