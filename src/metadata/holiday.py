# from typing_extensions import TypedDict
from dataclasses import dataclass
from utils import http_io as ufh
from datetime import datetime
from dateutil import rrule

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
    def from_json(cls, holiday_date):
        json_file_url = "https://raw.githubusercontent.com/dexplorer/df-metadata/refs/heads/main/metadata/api_data/holidays.json"
        json_key = "holidays"

        response = ufh.get_http_response(url=json_file_url)
        try:
            holidays = response.json()[json_key]
            if holidays:
                for holiday in holidays:
                    if holiday["holiday_date"] == holiday_date:
                        return cls(**holiday)
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
        # Get holidays from metadata
        holidays = response.json()[json_key]
        if holidays:
            holiday_objects = []
            for holiday in holidays:
                holiday_objects.append(Holiday(**holiday))
            return holiday_objects
        else:
            raise ValueError("Holiday data is invalid.")

    except ValueError as error:
        logging.error(error)
        raise


def get_weekend_holidays(start_date: str, end_date: str) -> list[Holiday]:
    date_format = "%Y-%m-%d"
    iso_weekend_days = [6, 7]
    weekend_holiday_objects = []
    for dt in rrule.rrule(
        rrule.DAILY,
        dtstart=datetime.strptime(start_date, date_format),
        until=datetime.strptime(end_date, date_format),
    ):
        if dt.isoweekday() in iso_weekend_days:
            holiday = {
                "holiday_date": dt.strftime(date_format),
                "holiday_desc": "Weekend Day",
                "holiday_groups": ["Weekend"],
            }
            weekend_holiday_objects.append(Holiday(**holiday))
    return weekend_holiday_objects
