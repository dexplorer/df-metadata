from dataclasses import dataclass
from utils import http_io as ufh

import logging


@dataclass
class DictionaryItem:
    column_name: str
    column_description: str
    system_data_element_name: str


@dataclass
class DatasetDictionary:
    dataset_id: str
    column_attributes: list[DictionaryItem]

    def __init__(
        self,
        dataset_id: str,
        column_attributes: list[DictionaryItem] | list[dict],
    ):
        self.dataset_id = dataset_id

        if isinstance(column_attributes, list) and all(
            isinstance(column_attribute, dict) for column_attribute in column_attributes
        ):
            self.column_attributes = [
                DictionaryItem(**column_attribute)
                for column_attribute in column_attributes
            ]
        else:
            self.column_attributes = column_attributes

    @classmethod
    def from_json(cls, dataset_id: str):
        json_file_url = "https://raw.githubusercontent.com/dexplorer/df-metadata/refs/heads/main/api_data/dataset_dictionary.json"
        json_key = "dictionaries"

        response = ufh.get_http_response(url=json_file_url)
        try:
            dictionaries = response.json()[json_key]
            if dictionaries:
                for dictionary in dictionaries:
                    # print(dictionaries)
                    if dictionary["dataset_id"] == dataset_id:
                        return cls(**dictionary)
            else:
                raise ValueError("Dataset schema data is invalid.")
        except ValueError as error:
            logging.error(error)
            raise
