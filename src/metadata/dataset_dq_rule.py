# from typing_extensions import TypedDict
from dataclasses import dataclass
from utils import http_io as ufh

import logging


@dataclass
class DatasetDQRule:
    dataset_id: str
    rule_id: str
    exp_id: str
    rule_fail_action: str

    def __init__(self, dataset_id, rule_id, exp_id, rule_fail_action, **kwargs):
        self.dataset_id = dataset_id
        self.rule_id = rule_id
        self.exp_id = exp_id
        self.rule_fail_action = rule_fail_action
        self.kwargs = kwargs


def get_dq_rules_by_dataset_id(
    dataset_id: str, dq_rules: list[DatasetDQRule]
) -> list[DatasetDQRule]:
    dq_rules_for_dataset = []

    for dq_rule in dq_rules:
        if dataset_id == dq_rule.dataset_id:
            dq_rules_for_dataset.append(dq_rule)

    # print(dq_rules_for_dataset)
    return dq_rules_for_dataset


def get_all_dq_rules_from_json() -> list[DatasetDQRule]:
    json_file_url = "https://raw.githubusercontent.com/dexplorer/df-metadata/refs/heads/main/api_data/dataset_dq_rules.json"
    json_key = "dq_rules"

    response = ufh.get_http_response(url=json_file_url)
    try:
        dq_rules = response.json()[json_key]
        if dq_rules:
            # print(dq_rules)
            dq_rule_objects = []
            for dq_rule in dq_rules:
                dq_rule_objects.append(DatasetDQRule(**dq_rule))
            return dq_rule_objects
        else:
            raise ValueError("Data quality rules data is invalid.")
    except ValueError as error:
        logging.error(error)
        raise
