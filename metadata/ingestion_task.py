from dataclasses import dataclass

from utils import http_io as ufh

import logging


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
        idempotent: bool
    ):
        self.loader = loader
        self.source_type = source_type
        self.target_type = target_type
        self.load_type = load_type
        self.idempotent = idempotent

@dataclass
class IngestionTask:
    ingestion_task_id: str
    source_dataset_id: str
    target_dataset_id: str
    ingestion_pattern: IngestionPattern

    def __init__(
        self,
        ingestion_task_id: str,
        source_dataset_id: str,
        target_dataset_id: str,
        ingestion_pattern: IngestionPattern | dict,
    ):
        self.kind = "generic"
        self.ingestion_task_id = ingestion_task_id
        self.source_dataset_id = source_dataset_id
        self.target_dataset_id = target_dataset_id
        if isinstance(ingestion_pattern, dict):
            self.ingestion_pattern = IngestionPattern(**ingestion_pattern)
        else:
            self.ingestion_pattern = ingestion_pattern

    @classmethod
    def from_json(self, ingestion_task_id):
        json_file_url = "https://raw.githubusercontent.com/dexplorer/df-metadata/refs/heads/main/metadata/api_data/ingestion_tasks.json"
        json_key = "ingestion_tasks"

        response = ufh.get_http_response(url=json_file_url)
        try:
            ingestion_tasks = response.json()[json_key]
            # print(ingestion_tasks)
            if ingestion_tasks:
                for ingestion_task in ingestion_tasks:
                    # print(dataset)
                    if ingestion_task["ingestion_task_id"] == ingestion_task_id:
                        return self(**ingestion_task)
            else:
                raise ValueError("Ingestion task data is invalid.")
        except ValueError as error:
            logging.error(error)
            raise
