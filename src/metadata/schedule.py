from dataclasses import dataclass
from utils import http_io as ufh

import logging


@dataclass
class Schedule:
    schedule_id: str
    application_id: str
    schedule_desc: str
    schedule_frequency: str
    run_calendar_offset: int
    holiday_groups: list[str]

    def __init__(
        self,
        schedule_id,
        application_id,
        schedule_desc,
        schedule_frequency,
        run_calendar_offset,
        holiday_groups,
    ):
        self.schedule_id = schedule_id
        self.application_id = application_id
        self.schedule_desc = schedule_desc
        self.schedule_frequency = schedule_frequency
        self.run_calendar_offset = run_calendar_offset
        self.holiday_groups = holiday_groups

    @classmethod
    def from_json(cls, schedule_id):
        json_file_url = "https://raw.githubusercontent.com/dexplorer/df-metadata/refs/heads/main/api_data/schedules.json"
        json_key = "schedules"

        response = ufh.get_request(url=json_file_url)
        try:
            schedules = response.json()[json_key]
            if schedules:
                for schedule in schedules:
                    # print(schedules)
                    if schedule["schedule_id"] == schedule_id:
                        return cls(**schedule)
            else:
                raise ValueError("Schedule data is invalid.")
        except ValueError as error:
            logging.error(error)
            raise
