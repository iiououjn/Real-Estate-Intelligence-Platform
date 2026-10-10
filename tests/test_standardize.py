import unittest

import pandas as pd

from dre.ingestion.standardize import normalize_col_name, parse_bedrooms, parse_dates, standardize_frame
from dre.schemas import RENTALS, TRANSACTIONS


class StandardizeTests(unittest.TestCase):
    def test_column_name_normalisation(self):
        self.assertEqual(normalize_col_name(" Area Name-EN "), "area_name_en")

    def test_parse_dates_multiple_formats(self):
        s = pd.Series(["05-01-2023", "2023-01-06", "07/01/2023", "garbage", None])
        out = parse_dates(s)
        self.assertEqual(out.iloc[0], pd.Timestamp("2023-01-05"))
        self.assertEqual(out.iloc[1], pd.Timestamp("2023-01-06"))
        self.assertEqual(out.iloc[2], pd.Timestamp("2023-01-07"))
        self.assertTrue(out.iloc[3:].isna().all())

    def test_parse_bedrooms(self):
        out = parse_bedrooms(pd.Series(["2 B/R", "Studio", "1 B/R", "PENTHOUSE", None, "10 B/R"]))
        self.assertEqual(out.tolist()[:3], [2, 0, 1])
        self.assertTrue(out.iloc[3:5].isna().all())
        self.assertEqual(out.iloc[5], 10)

    def test_standardize_maps_aliases_nulls_and_types(self):
        raw = pd.DataFrame({
            "TRANSACTION_ID": ["a1", "a2"], "trans_group_en": ["Sales", "Sales"], "instance_date": ["01-02-2024", "bad"],
            "property_type_en": ["Unit", " Unit "], "area_name_en": ["Marsa  Dubai", "N/A"], "procedure_area": ["100.5", "1,200"],
            "actual_worth": ["1,000,000", "oops"], "rooms_en": ["2 B/R", "Studio"], "surprise_col": [1, 2],
        })
        out, rep = standardize_frame(raw, TRANSACTIONS)
        self.assertEqual(list(out.columns), TRANSACTIONS.column_names)
        self.assertEqual(out.loc[0, "area_name"], "Marsa Dubai")          # whitespace collapsed
        self.assertTrue(pd.isna(out.loc[1, "area_name"]))                  # 'N/A' -> null
        self.assertEqual(out.loc[1, "area_sqm"], 1200.0)                   # thousands separator
        self.assertTrue(pd.isna(out.loc[1, "amount_aed"]))                 # unparseable -> null, counted
        self.assertEqual(rep.coerced_to_null["amount_aed"], 1)
        self.assertEqual(rep.coerced_to_null["transaction_date"], 1)
        self.assertEqual(out["bedrooms"].tolist(), [2, 0])
        self.assertIn("surprise_col", rep.unmapped_source_columns)
        self.assertEqual(rep.missing_required, [])

    def test_missing_required_column_reported(self):
        out, rep = standardize_frame(pd.DataFrame({"contract_id": ["x"]}), RENTALS)
        self.assertIn("annual_amount", rep.missing_required)


if __name__ == "__main__":
    unittest.main()
