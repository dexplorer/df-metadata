from dataclasses import dataclass

from utils import http_io as ufh

import logging


@dataclass
class ManagementTask:
    task: str
    required_parameters: dict

    def __init__(
        self,
        task: str, 
        required_parameters: dict 
    ):
        self.task = task
        self.required_parameters = required_parameters

@dataclass
class IngestionWorkflow:
    ingestion_workflow_id: str 
    ingestion_task_id: str
    pre_ingestion_tasks: list[ManagementTask]
    post_ingestion_tasks: list[ManagementTask]

    def __init__(
        self,
        ingestion_workflow_id: str,
        ingestion_task_id: str,
        pre_ingestion_tasks: list[ManagementTask | dict],
        post_ingestion_tasks: list[ManagementTask | dict]
    ):
        self.ingestion_workflow_id = ingestion_workflow_id
        self.ingestion_task_id = ingestion_task_id
        if isinstance(pre_ingestion_tasks, list) and all([isinstance(task, dict) for task in pre_ingestion_tasks]):
            self.pre_ingestion_tasks = [ManagementTask(**task) for task in pre_ingestion_tasks]
        else:
            self.pre_ingestion_tasks = pre_ingestion_tasks
        if isinstance(post_ingestion_tasks, list) and all([isinstance(task, dict) for task in post_ingestion_tasks]):
            self.post_ingestion_tasks = [ManagementTask(**task) for task in post_ingestion_tasks]
        else:
            self.post_ingestion_tasks = post_ingestion_tasks

    @classmethod
    def from_json(self, ingestion_workflow_id):
        json_file_url = "https://raw.githubusercontent.com/dexplorer/df-metadata/refs/heads/main/metadata/api_data/ingestion_workflows.json"
        json_key = "ingestion_workflows"

        response = ufh.get_http_response(url=json_file_url)
        try:
            ingestion_workflows = response.json()[json_key]
            # print(ingestion_workflows)
            if ingestion_workflows:
                for ingestion_workflow in ingestion_workflows:
                    # print(ingestion_workflows)
                    if ingestion_workflow["ingestion_workflow_id"] == ingestion_workflow_id:
                        return self(**ingestion_workflow)
            else:
                raise ValueError("Ingestion workflow data is invalid.")
        except ValueError as error:
            logging.error(error)
            raise
