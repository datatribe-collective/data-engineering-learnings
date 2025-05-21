import unittest
import pandas as pd
from scripts.clean_data import clean_covid_data

class TestCleanCovidData(unittest.TestCase):
    def test_excludes_aggregates(self):
        test_df = pd.DataFrame({
            'country': ['World', 'Africa', 'South Africa'],
            'code': ['OWID_WRL', 'OWID_AFR', 'ZAF']
        })
        cleaned_df = clean_covid_data(test_df)
        self.assertEqual(len(cleaned_df), 1)
        self.assertEqual(cleaned_df.iloc[0]['country'], 'South Africa')

    def test_drops_missing_codes(self):
        test_df = pd.DataFrame({
            'country': ['South Africa', 'Namibia'],
            'code': ['ZAF', None]
        })
        cleaned_df = clean_covid_data(test_df)
        self.assertEqual(len(cleaned_df), 1)
        self.assertEqual(cleaned_df.iloc[0]['country'], 'South Africa')

if __name__ == '__main__':
    unittest.main()
