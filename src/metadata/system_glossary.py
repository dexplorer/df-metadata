from dataclasses import dataclass
from utils import http_io as ufh

import logging


@dataclass
class SystemGlossaryItem:
    system_data_element_name: str
    system_data_element_description: str
    business_data_element_name: str

    def __init__(self, system_data_element_name, system_data_element_description, business_data_element_name):
        self.system_data_element_name = system_data_element_name
        self.system_data_element_description = system_data_element_description
        self.business_data_element_name = business_data_element_name

    @classmethod
    def from_json(cls, system_data_element_name):
        json_file_url = "https://raw.githubusercontent.com/dexplorer/df-metadata/refs/heads/main/api_data/system_glossary.json"
        json_key = "glossary_items"

        response = ufh.get_http_response(url=json_file_url)
        try:
            glossary_items = response.json()[json_key]
            if glossary_items:
                for glossary_item in glossary_items:
                    # print(glossary_items)
                    if glossary_item["system_data_element_name"] == system_data_element_name:
                        return cls(**glossary_item)
            else:
                raise ValueError("System glossary data is invalid.")
        except ValueError as error:
            logging.error(error)
            raise

def get_all_sys_glossary_items_from_json() -> list[SystemGlossaryItem]:
    json_file_url = "https://raw.githubusercontent.com/dexplorer/df-metadata/refs/heads/main/api_data/system_glossary.json"
    json_key = "glossary_items"

    response = ufh.get_http_response(url=json_file_url)
    try:
        glossary_items = response.json()[json_key]
        if glossary_items:
            # print(glossary_items)
            glossary_item_objects = []
            for glossary_item in glossary_items:
                glossary_item_objects.append(SystemGlossaryItem(**glossary_item))
            return glossary_item_objects
        else:
            raise ValueError("System glossary rules data is invalid.")
    except ValueError as error:
        logging.error(error)
        raise
