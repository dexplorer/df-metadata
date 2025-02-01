from dataclasses import dataclass
from utils import http_io as ufh

import logging


@dataclass
class Service:
    name: str
    endpoint: str

    def __init__(self, name, endpoint):
        self.name = name
        self.endpoint = endpoint

    @classmethod
    def from_json(self, name):
        json_file_url = "https://raw.githubusercontent.com/dexplorer/df-metadata/refs/heads/main/metadata/api_data/services.json"
        json_key = "services"

        response = ufh.get_http_response(url=json_file_url)
        try:
            services = response.json()[json_key]
            if services:
                for service in services:
                    # print(service)
                    if service["name"] == name:
                        return self(**service)
            else:
                raise ValueError("Service data is invalid.")
        except ValueError as error:
            logging.error(error)
            raise
