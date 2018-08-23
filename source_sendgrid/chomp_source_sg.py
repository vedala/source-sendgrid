import os
import csv
import urllib.request
import json
import math
from datetime import datetime, date, timedelta

ITEMS_PER_PAGE = 20
SG_URL_BASE = "https://api.sendgrid.com/v3/stats"

class SourceSendgrid(object):
    def __init__(self, credentials, source_config):
        self.credentials = credentials
        self.source_config = source_config
        self.validate_config()
        self.batches_fetched = 0
        self.batches_to_fetch = self.calculate_num_batches()

    def calculate_num_batches(self):
        start_d = date.fromisoformat(self.source_config['start-date'])
        end_d   = date.fromisoformat(self.source_config['end-date'])
        date_delta = end_d - start_d
        num_days = date_delta.days + 1
        return math.ceil(num_days / ITEMS_PER_PAGE)

    def validate_date_format(self, date_str):
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            return False

        return True

    def validate_config(self):
        if 'top-level-api' not in self.source_config:
            raise Exception("Key 'top-level-api' is missing")
        elif self.source_config['top-level-api'] not in ['stats']:
            raise Exception("Invalid value for key 'top-level-api'")
        elif 'start-date' not in self.source_config:
            raise Exception("Key 'start-date' is missing")
        elif not self.validate_date_format(self.source_config['start-date']):
            raise Exception("Invalid date for 'start-date'")
        elif 'end-date' not in self.source_config:
            raise Exception("Key 'end-date' is missing")
        elif not self.validate_date_format(self.source_config['end-date']):
            raise Exception("Invalid date for 'end-date'")

    def construct_url(self, start_date_str, end_date_str):
        sg_url = SG_URL_BASE
        sg_url += f"?start_date={start_date_str}"
        sg_url += f"&end_date={end_date_str}"
        return sg_url

    def build_request(self, sg_url):
        sg_request = urllib.request.Request(sg_url)
        api_key = self.credentials['api-key']
        sg_request.add_header('Authorization', f"Bearer {api_key}")
        return sg_request

    def calculate_date_range(self):
        start_date = self.source_config['start-date']
        end_date = self.source_config['end-date']

        days_to_add = self.batches_fetched * ITEMS_PER_PAGE
        td_days = timedelta(days=days_to_add)

        start_date_obj = date.fromisoformat(start_date) + td_days
        calc_end_date_obj = start_date_obj + timedelta(days=(ITEMS_PER_PAGE - 1))
        end_date_obj = date.fromisoformat(end_date)

        if end_date_obj < calc_end_date_obj:
            calc_end_date_obj = end_date_obj

        start_date_str = start_date_obj.isoformat()
        end_date_str   = calc_end_date_obj.isoformat()

        return (start_date_str, end_date_str)

    def prepare_batch_rows(self, json_list):
        list_of_tuples = []
        for row in json_list:
            row_list = []
            row_list.append(row['date'])
            metrics = row['stats'][0]['metrics']
            for field in self.source_config['fields']:
                row_list.append(metrics[field])

            row_tuple = tuple(row_list)
            list_of_tuples.append(row_tuple)

        return list_of_tuples

    def get_batch(self):
        if self.batches_fetched == self.batches_to_fetch:
            return []
        else:
            date_range_start, date_range_end = self.calculate_date_range()

        # construct_url
        sg_url = self.construct_url(date_range_start, date_range_end)

        # build request
        sg_request = self.build_request(sg_url)

        # execute request
        sg_response = urllib.request.urlopen(sg_request).read()
        response_decoded = sg_response.decode('utf-8').replace("'", '"')
        response_json = json.loads(response_decoded)

        # convert json array to list of tuples
        response_list = self.prepare_batch_rows(response_json)

        self.batches_fetched += 1
        return response_list

    def cleanup(self):
        pass
