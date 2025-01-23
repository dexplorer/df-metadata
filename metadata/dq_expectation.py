from dataclasses import dataclass
import json
from utils import file_io as uff
from utils import http_io as ufh 

import logging


@dataclass
class DQExpectation:
    exp_id: str
    exp_name: str
    ge_method: str

    def __init__(self, exp_id, exp_name, ge_method):
        self.exp_id = exp_id
        self.exp_name = exp_name
        self.ge_method = ge_method

    @classmethod
    def from_json(self, exp_id):
        json_file_url = "https://raw.githubusercontent.com/dexplorer/df-metadata/refs/heads/main/metadata/api_data/dq_expectations.json"
        json_key = "dq_expectations"

        response = ufh.get_http_response(url=json_file_url)
        try:
            dq_expectations = response.json()[json_key]
            if dq_expectations:
                for dq_expectation in dq_expectations:
                    # print(dq_expectations)
                    if dq_expectation["exp_id"] == exp_id:
                        return self(**dq_expectation)
            else:
                raise ValueError("DQ expectation data is invalid.")
        except ValueError as error:
            logging.error(error)
            raise
