import unittest
import json
from unittest.mock import patch
from source_sendgrid import SourceSendgrid

class SendgridTestCase(unittest.TestCase):
    """Tests for extracting from sendgrid."""

    @patch.object(SourceSendgrid, '__init__')
    def test_construct_url(self, mock_init):
        """Does extract from sendgrid create expected output files?"""

        mock_init.return_value = None

        source = SourceSendgrid()
        start_date = '2018-07-29'
        end_date = '2018-07-30'

        expected_url = "https://api.sendgrid.com/v3/stats"
        expected_url += "?start_date=2018-07-29&end_date=2018-07-30"
        self.assertEqual(expected_url,
                            source.construct_url(start_date, end_date))


    @patch.object(SourceSendgrid, '__init__')
    def test_validate_config(self, mock_init):

        mock_init.return_value = None
        source = SourceSendgrid()

        source.source_config = {}
        with self.assertRaises(Exception) as ctx:
            source.validate_config()
        self.assertEqual("Key 'top-level-api' is missing", str(ctx.exception))

        source.source_config = {'top-level-api': 'some-invalid-value'}
        with self.assertRaises(Exception) as ctx:
            source.validate_config()
        self.assertEqual("Invalid value for key 'top-level-api'", str(ctx.exception))

        source.source_config = {'top-level-api': 'stats'}
        with self.assertRaises(Exception) as ctx:
            source.validate_config()
        self.assertEqual("Key 'start-date' is missing", str(ctx.exception))

        source.source_config = {'top-level-api': 'stats', 'start-date': 'abcd'}
        with self.assertRaises(Exception) as ctx:
            source.validate_config()
        self.assertEqual("Invalid date for 'start-date'", str(ctx.exception))

        source.source_config = {'top-level-api': 'stats',
                                'start-date': '2016-07-30'}
        with self.assertRaises(Exception) as ctx:
            source.validate_config()
        self.assertEqual("Key 'end-date' is missing", str(ctx.exception))

        source.source_config = {'top-level-api': 'stats',
                                'start-date': '2016-07-30',
                                'end-date': 'efgh'}
        with self.assertRaises(Exception) as ctx:
            source.validate_config()
        self.assertEqual("Invalid date for 'end-date'", str(ctx.exception))

        source.source_config = {'top-level-api': 'stats',
                                'start-date': '2016-07-30',
                                'end-date': '2018-08-10'}
        self.assertEqual(None, source.validate_config())

    @patch.object(SourceSendgrid, '__init__')
    def test_calculate_num_batches(self, mock_init):

        mock_init.return_value = None
        source = SourceSendgrid()

        source.source_config = {'top-level-api': 'stats',
                                'start-date': '2018-07-01',
                                'end-date': '2018-07-19'}
        self.assertEqual(1, source.calculate_num_batches())

        source.source_config = {'top-level-api': 'stats',
                                'start-date': '2018-07-01',
                                'end-date': '2018-07-20'}
        self.assertEqual(1, source.calculate_num_batches())

        source.source_config = {'top-level-api': 'stats',
                                'start-date': '2018-07-01',
                                'end-date': '2018-07-21'}
        self.assertEqual(2, source.calculate_num_batches())


    @patch.object(SourceSendgrid, '__init__')
    def test_calculate_date_range(self, mock_init):

        mock_init.return_value = None
        source = SourceSendgrid()

        source.batches_fetched = 0
        source.source_config = {'top-level-api': 'stats',
                                'start-date': '2018-07-01',
                                'end-date': '2018-08-15'}
        self.assertEqual(("2018-07-01", "2018-07-20"),
                                        source.calculate_date_range())

        source.batches_fetched = 1
        source.source_config = {'top-level-api': 'stats',
                                'start-date': '2018-07-01',
                                'end-date': '2018-08-15'}
        self.assertEqual(("2018-07-21", "2018-08-09"),
                                        source.calculate_date_range())

        source.batches_fetched = 2
        source.source_config = {'top-level-api': 'stats',
                                'start-date': '2018-07-01',
                                'end-date': '2018-08-15'}
        self.assertEqual(("2018-08-10", "2018-08-15"),
                                        source.calculate_date_range())

        source.batches_fetched = 0
        source.source_config = {'top-level-api': 'stats',
                                'start-date': '2017-03-01',
                                'end-date': '2017-03-10'}
        self.assertEqual(("2017-03-01", "2017-03-10"),
                                        source.calculate_date_range())


    @patch.object(SourceSendgrid, '__init__')
    def test_prepare_batch_rows(self, mock_init):

        mock_init.return_value = None
        source = SourceSendgrid()
        source.source_config = { 'fields' :
                                  ['requests', 'delivered', 'unique_opens']
                                }
        json_list = json.loads('''[
              {
                  "stats" : [
                    {
                        "metrics" : {
                          "delivered" : 10,
                          "processed" : 0,
                          "invalid_emails" : 0,
                          "unique_opens" : 5,
                          "requests" : 20,
                          "clicks" : 0
                        }
                    }
                  ],
                  "date" : "2016-07-30"
              },
              {
                  "stats" : [
                    {
                        "metrics" : {
                          "delivered" : 30,
                          "processed" : 0,
                          "unique_opens" : 25,
                          "requests" : 40,
                          "clicks" : 0
                        }
                    }
                  ],
                  "date" : "2016-07-31"
              },
              {
                  "stats" : [
                    {
                        "metrics" : {
                          "delivered" : 95,
                          "processed" : 0,
                          "unique_opens" : 91,
                          "requests" : 100,
                          "clicks" : 0
                        }
                    }
                  ],
                  "date" : "2016-08-01"
              }]''')

        expected_list = [ ("2016-07-30", 20, 10, 5),
                          ("2016-07-31", 40, 30, 25),
                          ("2016-08-01", 100, 95, 91)
                        ]
        self.assertEqual(expected_list, source.prepare_batch_rows(json_list))

if __name__ == "__main__":
    unittest.main()
