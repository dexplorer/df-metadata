from dataclasses import dataclass
from utils import http_io as ufh

import logging


@dataclass
class DataSource:
    data_source_id: str
    data_source_name: str
    data_source_user: str

    def __init__(
        self,
        data_source_id,
        data_source_name,
        data_source_user,
    ):
        self.data_source_id = data_source_id
        self.data_source_name = data_source_name
        self.data_source_user = data_source_user

    @classmethod
    def from_json(cls, data_source_id):
        json_file_url = "https://raw.githubusercontent.com/dexplorer/df-metadata/refs/heads/main/api_data/data_sources.json"
        json_key = "data_sources"

        response = ufh.get_http_response(url=json_file_url)
        try:
            data_sources = response.json()[json_key]
            # print(assets)
            if data_sources:
                for data_source in data_sources:
                    # print(asset)
                    if data_source["data_source_id"] == data_source_id:
                        return cls(**data_source)
            else:
                raise ValueError("Data source data is invalid.")
        except ValueError as error:
            logging.error(error)
            raise


def get_data_source_by_data_source_id(
    data_source_id: str, data_sources: list[DataSource]
) -> DataSource:

    for data_source in data_sources:
        if data_source_id == data_source.data_source_id:
            return data_source

    return None


def get_data_source_from_json(data_source_id: str):
    json_file_url = "https://raw.githubusercontent.com/dexplorer/df-metadata/refs/heads/main/api_data/data_sources.json"
    # json_file_url = f"file:///workspaces/df-metadata/api_data/datasets.json"
    json_key = "data_sources"

    response = ufh.get_http_response(url=json_file_url)
    try:
        data_sources = response.json()[json_key]
        # print(data_sources)
        if data_sources:
            for data_source in data_sources:
                # print(data_source)
                if data_source["data_source_id"] == data_source_id:
                    return DataSource(**data_source)
        else:
            raise ValueError("Data source data is invalid.")
    except ValueError as error:
        logging.error(error)
        raise
