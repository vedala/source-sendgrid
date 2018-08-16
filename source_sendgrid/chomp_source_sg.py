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

    def get_batch(self):
        pass

    def cleanup(self):
        pass

    def construct_url(self):
        start_date = self.source_config['start_date']
        end_date = self.source_config['end_date']
        sg_url = f"https://api.sendgrid.com/v3/stats?start_date={start_date}"
        sg_url = sg_url + f"&end_date={end_date}"
        return sg_url

    def fetch_and_write_data(extract_name, api_key, fields, start_date,
                                                end_date, extract_location):
        filename_with_path = construct_filename(extract_location, extract_name)
        extract_file = open(filename_with_path, 'w+')
        csv_writer = csv.writer(extract_file, quotechar='"',
                                        quoting=csv.QUOTE_NONNUMERIC)
    
        sg_url = f"https://api.sendgrid.com/v3/stats?start_date={start_date}"
        sg_url = sg_url + f"&end_date={end_date}"
        sg_request =urllib.request.Request(sg_url)
        sg_request.add_header('Authorization', f"Bearer {api_key}")
        sg_response = urllib.request.urlopen(sg_request).read()
        sg_response_decoded = sg_response.decode('utf-8').replace("'", '"')
        sg_response_json = json.loads(sg_response_decoded)
        for row in sg_response_json:
          write_row_to_file(extract_file, csv_writer, row, fields)
        extract_file.close()
    
    def construct_filename(directory, base_filename):
        filename_with_path = os.path.join(os.path.expanduser(directory),
                                                   base_filename + ".csv")
        return filename_with_path 
    
    def write_row_to_file(file_obj, csv_writer, row, fields):
        field_values_list = []
        for field in fields:
            field_values_list.append(row['stats'][0]['metrics'][field])
        field_values_tuple = tuple(field_values_list)
        csv_writer.writerow(field_values_tuple)
