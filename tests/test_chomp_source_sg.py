import unittest
import json
import sys
from unittest.mock import patch
from source_sendgrid import SourceSendgrid

def get_file_contents(filename):
    try:
        with open(filename) as f:
            contents = f.read()
    except FileNotFoundError:
        print(f'Error, file "{filename}" does not exist.', file=sys.stderr)
        raise

    return contents

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


    def test_prepare_batch_rows(self):

        credentials = json.loads(get_file_contents('sg_credentials.json'))
        source_config = {'top-level-api': 'stats',
                         'start-date': '2018-07-21',
                         'end-date': '2018-08-20',
                         'fields' :
                                  ['requests', 'delivered', 'unique_opens']
                         }

        source = SourceSendgrid(credentials, source_config)

        expected_batch_1 = [
            ('2018-07-21', 0, 0, 0),
            ('2018-07-22', 0, 0, 0),
            ('2018-07-23', 0, 0, 0),
            ('2018-07-24', 0, 0, 0),
            ('2018-07-25', 0, 0, 0),
            ('2018-07-26', 0, 0, 0),
            ('2018-07-27', 0, 0, 0),
            ('2018-07-28', 0, 0, 0),
            ('2018-07-29', 0, 0, 0),
            ('2018-07-30', 3, 1, 1),
            ('2018-07-31', 0, 0, 0),
            ('2018-08-01', 0, 0, 0),
            ('2018-08-02', 0, 0, 0),
            ('2018-08-03', 0, 0, 0),
            ('2018-08-04', 0, 0, 0),
            ('2018-08-05', 0, 0, 0),
            ('2018-08-06', 0, 0, 0),
            ('2018-08-07', 0, 0, 0),
            ('2018-08-08', 0, 0, 0),
            ('2018-08-09', 0, 0, 0)
        ]
        self.assertEqual(expected_batch_1, source.get_batch())

        expected_batch_2 = [
            ('2018-08-10', 0, 0, 0),
            ('2018-08-11', 0, 0, 0),
            ('2018-08-12', 0, 0, 0),
            ('2018-08-13', 0, 0, 0),
            ('2018-08-14', 0, 0, 0),
            ('2018-08-15', 0, 0, 0),
            ('2018-08-16', 0, 0, 0),
            ('2018-08-17', 15, 15, 11),
            ('2018-08-18', 0, 0, 0),
            ('2018-08-19', 0, 0, 4),
            ('2018-08-20', 0, 0, 0)
        ]
        self.assertEqual(expected_batch_2, source.get_batch())

        expected_batch_3 = []
        self.assertEqual(expected_batch_3, source.get_batch())

if __name__ == "__main__":
    unittest.main()
