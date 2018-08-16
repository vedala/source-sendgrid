import unittest
import os
import sys
from chompetl import main, get_file_contents

class IntegrationSendgridExtractTestCase(unittest.TestCase):
    """Tests for extracting from sendgrid."""

    def count_lines(self, file_name):
        count = 0
        with open(file_name) as f:
            while True:
                row = f.readline()
                if not row:
                    break
                count = count + 1
        return count

    def test_sendgrid_extract_job_1003(self):
        """Does extract from sendgrid create expected output files?"""

        json_file = "job_1003.json"
        sys.argv[1:] = [json_file]

        main()
        expected_str_stats = '0,0,0\n3,1,1\n'
        test_file_1 = "~/misc/extract_out/stats.csv"
        self.assertEqual(expected_str_stats,
                            get_file_contents(os.path.expanduser(test_file_1)))
        os.remove(os.path.expanduser(test_file_1))

if __name__ == "__main__":
    unittest.main()
