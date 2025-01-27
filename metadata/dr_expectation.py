from dataclasses import dataclass
from utils import http_io as ufh

import logging


@dataclass
class DRExpectation:
    exp_id: str
    exp_name: str
    ge_method: str

    def __init__(self, exp_id, exp_name, ge_method):
        self.exp_id = exp_id
        self.exp_name = exp_name
        self.ge_method = ge_method

    @classmethod
    def from_json(self, exp_id):
        json_file_url = "https://raw.githubusercontent.com/dexplorer/df-metadata/refs/heads/main/metadata/api_data/dr_expectations.json"
        json_key = "dr_expectations"

        response = ufh.get_http_response(url=json_file_url)
        try:
            dr_expectations = response.json()[json_key]
            if dr_expectations:
                for dr_expectation in dr_expectations:
                    # print(dr_expectations)
                    if dr_expectation["exp_id"] == exp_id:
                        return self(**dr_expectation)
            else:
                raise ValueError("Data reconciliation expectation data is invalid.")
        except ValueError as error:
            logging.error(error)
            raise
