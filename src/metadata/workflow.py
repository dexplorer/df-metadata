from dataclasses import dataclass
from enum import StrEnum

from utils import http_io as ufh

import logging


class WorkflowType(StrEnum):
    INGESTION = "ingestion"
    DISTRIBUTION = "distribution"


@dataclass
class ManagementTask:
    name: str
    required_parameters: dict

    def __init__(self, name: str, required_parameters: dict):
        self.name = name
        self.required_parameters = required_parameters


@dataclass
class Workflow:
    workflow_id: str
    workflow_type: str
    pre_tasks: list[ManagementTask]
    post_tasks: list[ManagementTask]

    def __init__(
        self,
        workflow_id: str,
        workflow_type: str,
        pre_tasks: list[ManagementTask | dict],
        post_tasks: list[ManagementTask | dict],
    ):
        self.workflow_id = workflow_id
        self.workflow_type = workflow_type
        if isinstance(pre_tasks, list) and all(
            [isinstance(task, dict) for task in pre_tasks]
        ):
            self.pre_tasks = [ManagementTask(**task) for task in pre_tasks]
        else:
            self.pre_tasks = pre_tasks
        if isinstance(post_tasks, list) and all(
            [isinstance(task, dict) for task in post_tasks]
        ):
            self.post_tasks = [ManagementTask(**task) for task in post_tasks]
        else:
            self.post_tasks = post_tasks

    @classmethod
    def from_json(cls, workflow_id):
        json_file_url = "https://raw.githubusercontent.com/dexplorer/df-metadata/refs/heads/main/api_data/workflows.json"
        json_key = "workflows"

        response = ufh.get_http_response(url=json_file_url)
        try:
            workflows = response.json()[json_key]
            # print(workflows)
            if workflows:
                for workflow in workflows:
                    # print(workflows)
                    if workflow["workflow_id"] == workflow_id:
                        return cls(**workflow)
            else:
                raise ValueError("Workflow data is invalid.")
        except ValueError as error:
            logging.error(error)
            raise


@dataclass(kw_only=True)
class IngestionWorkflow(Workflow):
    ingestion_task_id: str

    def __init__(
        self,
        workflow_id: str,
        workflow_type: str,
        pre_tasks: list[ManagementTask | dict],
        post_tasks: list[ManagementTask | dict],
        ingestion_task_id: str,
    ):
        super().__init__(
            workflow_id,
            workflow_type,
            pre_tasks,
            post_tasks,
        )
        self.ingestion_task_id = ingestion_task_id


@dataclass(kw_only=True)
class DistributionWorkflow(Workflow):
    distribution_task_id: str

    def __init__(
        self,
        workflow_id: str,
        workflow_type: str,
        pre_tasks: list[ManagementTask | dict],
        post_tasks: list[ManagementTask | dict],
        distribution_task_id: str,
    ):
        super().__init__(
            workflow_id,
            workflow_type,
            pre_tasks,
            post_tasks,
        )
        self.distribution_task_id = distribution_task_id


def get_workflow_from_json(workflow_id: str):
    json_file_url = "https://raw.githubusercontent.com/dexplorer/df-metadata/refs/heads/main/api_data/workflows.json"
    json_key = "workflows"

    response = ufh.get_http_response(url=json_file_url)
    try:
        workflows = response.json()[json_key]
        # print(workflows)
        if workflows:
            for workflow in workflows:
                # print(workflow)
                if workflow["workflow_id"] == workflow_id:
                    if workflow["workflow_type"] == WorkflowType.INGESTION:
                        return IngestionWorkflow(**workflow)
                    elif workflow["workflow_type"] == WorkflowType.DISTRIBUTION:
                        return DistributionWorkflow(**workflow)
        else:
            raise ValueError("Workflow data is invalid.")
    except ValueError as error:
        logging.error(error)
        raise
