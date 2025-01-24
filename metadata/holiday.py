# from typing_extensions import TypedDict
from dataclasses import dataclass
from utils import http_io as ufh

import logging


@dataclass
class Holiday:
    holiday_date: str
    holiday_desc: str
    holiday_groups: list[str]

    def __init__(self, holiday_date, holiday_desc, holiday_groups):
        self.holiday_date = holiday_date
        self.holiday_desc = holiday_desc
        self.holiday_groups = holiday_groups

    @classmethod
    def from_json(self, holiday_date):
        json_file_url = "https://raw.githubusercontent.com/dexplorer/df-metadata/refs/heads/main/metadata/api_data/holidays.json"
        json_key = "holidays"

        response = ufh.get_http_response(url=json_file_url)
        try:
            holidays = response.json()[json_key]
            if holidays:
                for holiday in holidays:
                    # print(holidays)
                    if holiday["holiday_date"] == holiday_date:
                        return self(**holiday)
            else:
                raise ValueError("Holiday data is invalid.")
        except ValueError as error:
            logging.error(error)
            raise


def get_all_holidays_from_json() -> list[Holiday]:
    json_file_url = "https://raw.githubusercontent.com/dexplorer/df-metadata/refs/heads/main/metadata/api_data/holidays.json"
    json_key = "holidays"

    response = ufh.get_http_response(url=json_file_url)
    try:
        holidays = response.json()[json_key]
        if holidays:
            # print(holidays)
            holiday_objects = []
            for holiday in holidays:
                holiday_objects.append(Holiday(**holiday))
            return holiday_objects
        else:
            raise ValueError("Holiday data is invalid.")
    except ValueError as error:
        logging.error(error)
        raise
