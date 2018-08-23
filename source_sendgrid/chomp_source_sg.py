import os
import csv
import urllib.request
import json

ITEMS_PER_PAGE = 20

class SourceSendgrid(object):
    def __init__(self, credentials, source_config):
        self.credentials = credentials
        self.source_config = source_config
        self.validate_config()
        url_str = construct_url()
        self.items_per_page = ITEMS_PER_PAGE

    def validate_config(self):
        if 'top-level-api' not in self.source_config:
            raise Exception("Key 'top-level-api' is missing")
        elif self.source_config['top-level-api'] not in ['stats']:
            raise Exception("Invalid value for key 'top-level-api'")
        elif 'start-date' not in self.source_config:
            raise Exception("Key 'start-date' is missing")
        elif 'end-date' not in self.source_config:
            raise Exception("Key 'end-date' is missing")


    def construct_url(self):
        start_date = self.source_config['start_date']
        end_date = self.source_config['end_date']
        sg_url = f"https://api.sendgrid.com/v3/stats?start_date={start_date}"
        sg_url = sg_url + f"&end_date={end_date}"
        return sg_url

    def build_request(self):
        sg_request =urllib.request.Request(sg_url)
        sg_request.add_header('Authorization', f"Bearer {api_key}")

    def get_batch(self):
        sg_request.add_header('Authorization', f"Bearer {api_key}")
        sg_response = urllib.request.urlopen(sg_request).read()
        sg_response_decoded = sg_response.decode('utf-8').replace("'", '"')
        sg_response_json = json.loads(sg_response_decoded)
        for row in sg_response_json:
            # append to list instead of writing to file
            # write_row_to_file(extract_file, csv_writer, row, fields)
            pass

    def cleanup(self):
        pass
