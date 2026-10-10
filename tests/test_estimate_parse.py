import unittest

from dre.analytics.estimate import parse_query


class ParseQueryTests(unittest.TestCase):
    def test_headline_example(self):
        p = parse_query("Dubai Marina, 2-bedroom, 1,200 sq ft")
        self.assertEqual((p.area, p.bedrooms, p.size_sqft), ("Dubai Marina", 2, 1200.0))

    def test_variants(self):
        self.assertEqual(parse_query("Business Bay 3 BR 1500 sqft").bedrooms, 3)
        p = parse_query("JVC, studio, 450 sqft")
        self.assertEqual((p.area, p.bedrooms, p.size_sqft), ("JVC", 0, 450.0))
        self.assertAlmostEqual(parse_query("Downtown Dubai, 1 bed, 100 sqm").size_sqft, 1076.39, places=1)
        p = parse_query("Palm Jumeirah")
        self.assertEqual((p.area, p.bedrooms, p.size_sqft), ("Palm Jumeirah", None, None))


if __name__ == "__main__":
    unittest.main()
