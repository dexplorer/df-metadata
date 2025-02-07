from dataclasses import dataclass

from utils import http_io as ufh

import logging


@dataclass
class DistributionPattern:
    extracter: str
    source_type: str
    target_type: str

    def __init__(
        self,
        extracter: str,
        source_type: str,
        target_type: str,
    ):
        self.extracter = extracter
        self.source_type = source_type
        self.target_type = target_type


@dataclass
class DistributionTask:
    distribution_task_id: str
    source_dataset_id: str
    target_dataset_id: str
    distribution_pattern: DistributionPattern

    def __init__(
        self,
        distribution_task_id: str,
        source_dataset_id: str,
        target_dataset_id: str,
        distribution_pattern: DistributionPattern | dict,
    ):
        self.distribution_task_id = distribution_task_id
        self.source_dataset_id = source_dataset_id
        self.target_dataset_id = target_dataset_id
        if isinstance(distribution_pattern, dict):
            self.distribution_pattern = DistributionPattern(**distribution_pattern)
        else:
            self.distribution_pattern = distribution_pattern

    @classmethod
    def from_json(cls, distribution_task_id):
        json_file_url = "https://raw.githubusercontent.com/dexplorer/df-metadata/refs/heads/main/metadata/api_data/distribution_tasks.json"
        json_key = "distribution_tasks"

        response = ufh.get_http_response(url=json_file_url)
        try:
            distribution_tasks = response.json()[json_key]
            # print(distribution_tasks)
            if distribution_tasks:
                for distribution_task in distribution_tasks:
                    # print(distribution_task)
                    if distribution_task["distribution_task_id"] == distribution_task_id:
                        return cls(**distribution_task)
            else:
                raise ValueError("Distribution task data is invalid.")
        except ValueError as error:
            logging.error(error)
            raise
