from dataclasses import dataclass
from enum import StrEnum

from utils import http_io as ufh

import logging


class IntegrationTaskType(StrEnum):
    INGESTION = "ingestion"
    DISTRIBUTION = "distribution"


@dataclass
class IngestionPattern:
    loader: str
    source_type: str
    target_type: str
    load_type: str
    idempotent: bool

    def __init__(
        self,
        loader: str,
        source_type: str,
        target_type: str,
        load_type: str,
        idempotent: bool,
    ):
        self.loader = loader
        self.source_type = source_type
        self.target_type = target_type
        self.load_type = load_type
        self.idempotent = idempotent


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
class IntegrationTask:
    task_id: str
    task_type: str
    source_dataset_id: str
    target_dataset_id: str

    def __init__(
        self,
        task_id: str,
        task_type: str,
        source_dataset_id: str,
        target_dataset_id: str,
    ):
        self.task_id = task_id
        self.task_type = task_type
        self.source_dataset_id = source_dataset_id
        self.target_dataset_id = target_dataset_id

    @classmethod
    def from_json(cls, task_id):
        json_file_url = "https://raw.githubusercontent.com/dexplorer/df-metadata/refs/heads/main/api_data/integration_tasks.json"
        json_key = "integration_tasks"

        response = ufh.get_http_response(url=json_file_url)
        try:
            integration_tasks = response.json()[json_key]
            # print(integration_tasks)
            if integration_tasks:
                for integration_task in integration_tasks:
                    # print(integration_task)
                    if integration_task["task_id"] == task_id:
                        return cls(**integration_task)
            else:
                raise ValueError("Integration task data is invalid.")
        except ValueError as error:
            logging.error(error)
            raise


@dataclass(kw_only=True)
class IngestionTask(IntegrationTask):
    ingestion_pattern: IngestionPattern

    def __init__(
        self,
        task_id: str,
        task_type: str,
        source_dataset_id: str,
        target_dataset_id: str,
        ingestion_pattern: IngestionPattern | dict,
    ):
        super().__init__(
            task_id,
            task_type,
            source_dataset_id,
            target_dataset_id,
        )
        self.ingestion_pattern = ingestion_pattern
        if isinstance(ingestion_pattern, dict):
            self.ingestion_pattern = IngestionPattern(**ingestion_pattern)
        else:
            self.ingestion_pattern = ingestion_pattern


@dataclass(kw_only=True)
class DistributionTask(IntegrationTask):
    distribution_pattern: DistributionPattern

    def __init__(
        self,
        task_id: str,
        task_type: str,
        source_dataset_id: str,
        target_dataset_id: str,
        distribution_pattern: DistributionPattern | dict,
    ):
        super().__init__(
            task_id,
            task_type,
            source_dataset_id,
            target_dataset_id,
        )
        self.distribution_pattern = distribution_pattern
        if isinstance(distribution_pattern, dict):
            self.distribution_pattern = DistributionPattern(**distribution_pattern)
        else:
            self.distribution_pattern = distribution_pattern


def get_integration_task_from_json(task_id: str):
    json_file_url = "https://raw.githubusercontent.com/dexplorer/df-metadata/refs/heads/main/api_data/integration_tasks.json"
    json_key = "integration_tasks"

    response = ufh.get_http_response(url=json_file_url)
    try:
        integration_tasks = response.json()[json_key]
        # print(integration_tasks)
        if integration_tasks:
            for integration_task in integration_tasks:
                # print(integration_task)
                if integration_task["task_id"] == task_id:
                    if integration_task["task_type"] == IntegrationTaskType.INGESTION:
                        return IngestionTask(**integration_task)
                    elif (
                        integration_task["task_type"]
                        == IntegrationTaskType.DISTRIBUTION
                    ):
                        return DistributionTask(**integration_task)
        else:
            raise ValueError("Integration task data is invalid.")
    except ValueError as error:
        logging.error(error)
        raise
