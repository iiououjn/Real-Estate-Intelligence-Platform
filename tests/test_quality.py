import unittest
from datetime import date

import pandas as pd

from dre.quality.runner import QualityGateError, run_quality
from dre.schemas import RENTALS, TRANSACTIONS, cast_frame
from tests.helpers import make_settings


def tx_row(**kw):
    row = dict(transaction_id="T1", trans_group="Sales", procedure_name="Sell", transaction_date="2024-03-01", property_id="P1",
               property_type="Unit", property_sub_type="Flat", property_usage="Residential", reg_type="Existing Properties",
               area_name="Marsa Dubai", building_name="B", project_name="Proj", master_project="M", nearest_landmark=None,
               nearest_metro=None, nearest_mall=None, rooms="2 B/R", bedrooms=2, has_parking=1, area_sqm=100.0,
               amount_aed=2_500_000.0, price_per_sqm=25_000.0)
    row.update(kw)
    return row


class QualityTests(unittest.TestCase):
    def setUp(self):
        self.settings = make_settings("/tmp", max_reject_pct=100.0)

    def run_rows(self, rows, source_columns=None):
        df = cast_frame(pd.DataFrame(rows), TRANSACTIONS)
        return run_quality(df, TRANSACTIONS, self.settings, "run1", source_columns=source_columns or list(df.columns),
                           today=date(2024, 3, 10))

    def reasons(self, res):
        return dict(zip(res.rejected["record_key"], res.rejected["reject_reasons"]))

    def test_each_defect_is_rejected_with_its_reason(self):
        rows = [
            tx_row(transaction_id="ok"),
            tx_row(transaction_id="dup"), tx_row(transaction_id="dup"),
            tx_row(transaction_id=None),
            tx_row(transaction_id="noprop", property_id=None),
            tx_row(transaction_id="neg", area_sqm=-5.0),
            tx_row(transaction_id="zero", area_sqm=0.0),
            tx_row(transaction_id="price0", amount_aed=0.0),
            tx_row(transaction_id="huge", amount_aed=9e12),
            tx_row(transaction_id="psf", amount_aed=50_000.0, area_sqm=100.0),
            tx_row(transaction_id="future", transaction_date="2024-06-01"),
            tx_row(transaction_id="noarea", area_name=None),
        ]
        res = self.run_rows(rows)
        r = self.reasons(res)
        self.assertEqual(len(res.clean), 2)                         # 'ok' and the first 'dup'
        self.assertIn("duplicate_transaction_id", r["dup"])
        self.assertIn("missing_transaction_id", r["<null>"])
        self.assertIn("missing_property_id", r["noprop"])
        self.assertIn("invalid_area", r["neg"])
        self.assertIn("invalid_area", r["zero"])
        self.assertIn("impossible_sale_price", r["price0"])
        self.assertIn("impossible_sale_price", r["huge"])
        self.assertIn("implausible_price_per_sqft", r["psf"])
        self.assertIn("date_out_of_range", r["future"])
        self.assertIn("missing_area", r["noarea"])

    def test_rejected_rows_keep_full_payload(self):
        res = self.run_rows([tx_row(transaction_id="neg", area_sqm=-5.0)])
        self.assertIn('"area_sqm": "-5.0"', res.rejected.loc[0, "payload"])

    def test_mortgages_and_gifts_with_zero_amount_are_not_price_rejected(self):
        res = self.run_rows([tx_row(transaction_id="g", trans_group="Gifts", amount_aed=0.0, price_per_sqm=0.0)])
        self.assertEqual(len(res.rejected), 0)

    def test_property_id_check_skipped_when_source_has_no_such_column(self):
        cols = [c for c in TRANSACTIONS.column_names if c != "property_id"]
        res = self.run_rows([tx_row(property_id=None)], source_columns=cols)
        self.assertEqual(len(res.rejected), 0)

    def test_freshness_warns_on_stale_data(self):
        df = cast_frame(pd.DataFrame([tx_row(transaction_date="2023-01-01")]), TRANSACTIONS)
        res = run_quality(df, TRANSACTIONS, self.settings, "r", today=date(2024, 3, 10))
        row = res.checks[res.checks.check_name == "freshness"].iloc[0]
        self.assertEqual(row["status"], "WARN")

    def test_quality_gate_aborts_when_too_many_rows_rejected(self):
        strict = make_settings("/tmp", max_reject_pct=10.0)
        df = cast_frame(pd.DataFrame([tx_row(transaction_id=None)] * 5 + [tx_row(transaction_id="ok")]), TRANSACTIONS)
        with self.assertRaises(QualityGateError):
            run_quality(df, TRANSACTIONS, strict, "r", today=date(2024, 3, 10))

    def test_rental_end_before_start(self):
        row = dict(contract_id="c1", contract_reg_type="New", contract_start_date="2024-01-01", contract_end_date="2023-01-01",
                   property_id="p", property_type="Unit", area_name="Marsa Dubai", area_sqm=80.0, annual_amount=100000.0)
        df = cast_frame(pd.DataFrame([row]), RENTALS)
        res = run_quality(df, RENTALS, self.settings, "r", today=date(2024, 3, 10))
        self.assertIn("end_before_start", res.rejected.loc[0, "reject_reasons"])


if __name__ == "__main__":
    unittest.main()
