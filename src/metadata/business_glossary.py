from dataclasses import dataclass
from utils import http_io as ufh

import logging


@dataclass
class BusinessGlossaryItem:
    business_data_element_name: str
    business_data_element_description: str
    data_classification: str

    def __init__(
        self,
        business_data_element_name,
        business_data_element_description,
        data_classification,
    ):
        self.business_data_element_name = business_data_element_name
        self.business_data_element_description = business_data_element_description
        self.data_classification = data_classification

    @classmethod
    def from_json(cls, business_data_element_name):
        json_file_url = "https://raw.githubusercontent.com/dexplorer/df-metadata/refs/heads/main/api_data/business_glossary.json"
        json_key = "glossary_items"

        response = ufh.get_http_response(url=json_file_url)
        try:
            glossary_items = response.json()[json_key]
            if glossary_items:
                for glossary_item in glossary_items:
                    # print(glossary_items)
                    if (
                        glossary_item["business_data_element_name"]
                        == business_data_element_name
                    ):
                        return cls(**glossary_item)
            else:
                raise ValueError("Business glossary data is invalid.")
        except ValueError as error:
            logging.error(error)
            raise


def get_all_bus_glossary_items_from_json() -> list[BusinessGlossaryItem]:
    json_file_url = "https://raw.githubusercontent.com/dexplorer/df-metadata/refs/heads/main/api_data/business_glossary.json"
    json_key = "glossary_items"

    response = ufh.get_http_response(url=json_file_url)
    try:
        glossary_items = response.json()[json_key]
        if glossary_items:
            # print(glossary_items)
            glossary_item_objects = []
            for glossary_item in glossary_items:
                glossary_item_objects.append(BusinessGlossaryItem(**glossary_item))
            return glossary_item_objects
        else:
            raise ValueError("Business glossary rules data is invalid.")
    except ValueError as error:
        logging.error(error)
        raise
