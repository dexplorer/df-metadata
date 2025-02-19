from dataclasses import dataclass
from utils import http_io as ufh

import logging


@dataclass
class DatasetSchema:
    dataset_id: str
    schema: list[dict]

    def __init__(self, dataset_id: str, schema: list[dict]):
        self.dataset_id = dataset_id
        self.schema = schema

    @classmethod
    def from_json(cls, dataset_id: str):
        json_file_url = "https://raw.githubusercontent.com/dexplorer/df-metadata/refs/heads/main/api_data/dataset_schemas.json"
        json_key = "dataset_schemas"

        response = ufh.get_http_response(url=json_file_url)
        try:
            dataset_schemas = response.json()[json_key]
            if dataset_schemas:
                for dataset_schema in dataset_schemas:
                    # print(dataset_schemas)
                    if dataset_schema["dataset_id"] == dataset_id:
                        return cls(**dataset_schema)
            else:
                raise ValueError("Dataset schema data is invalid.")
        except ValueError as error:
            logging.error(error)
            raise
