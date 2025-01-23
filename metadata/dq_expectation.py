from dataclasses import dataclass
import json
from utils import file_io as uff

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
        json_file = "https://github.com/dexplorer/df-metadata/blob/ddf4fff0e85a4175fbd34406bedb9bbb14e0edb6/metadata/api_data/dq_expectations.json"
        json_key = "dq_expectations"
        # with open(json_file, 'r') as f:
        with uff.uf_open_file(file_path=json_file, open_mode="r") as f:
            dq_expectations = json.load(f)[json_key]

        try:
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
