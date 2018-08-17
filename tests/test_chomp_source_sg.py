import unittest
from unittest.mock import patch
from source_sendgrid import SourceSendgrid

class SendgridTestCase(unittest.TestCase):
    """Tests for extracting from sendgrid."""

    @patch.object(SourceSendgrid, '__init__')
    def test_construct_url(self, mock_init):
        """Does extract from sendgrid create expected output files?"""

        mock_init.return_value = None

        source = SourceSendgrid()
        source.source_config = { 'start_date': '2018-07-29',
                                'end_date': '2018-07-30' }

        expected_url = f"https://api.sendgrid.com/v3/stats?start_date=2018-07-29"
        expected_url += f"&end_date=2018-07-30"
        self.assertEqual(expected_url, source.construct_url())


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
        self.assertEqual(None, source.validate_config())

if __name__ == "__main__":
    unittest.main()
