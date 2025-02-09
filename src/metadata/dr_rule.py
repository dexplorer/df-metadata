# from typing_extensions import TypedDict
from dataclasses import dataclass
from utils import http_io as ufh

import logging


@dataclass
class DRRule:
    rule_id: str
    dataset_id: str
    exp_id: str
    rule_fail_action: str

    def __init__(self, rule_id, dataset_id, exp_id, rule_fail_action, **kwargs):
        self.rule_id = rule_id
        self.dataset_id = dataset_id
        self.exp_id = exp_id
        self.rule_fail_action = rule_fail_action
        self.kwargs = kwargs


def get_dr_rules_by_dataset_id(dataset_id: str, dr_rules: list[DRRule]) -> list[DRRule]:
    dr_rules_for_dataset = []

    for dr_rule in dr_rules:
        if dataset_id == dr_rule.dataset_id:
            dr_rules_for_dataset.append(dr_rule)

    # print(dr_rules_for_dataset)
    return dr_rules_for_dataset


def get_all_dr_rules_from_json() -> list[DRRule]:
    json_file_url = "https://raw.githubusercontent.com/dexplorer/df-metadata/refs/heads/main/api_data/dr_rules.json"
    json_key = "dr_rules"

    response = ufh.get_http_response(url=json_file_url)
    try:
        dr_rules = response.json()[json_key]
        if dr_rules:
            # print(dr_rules)
            dr_rule_objects = []
            for dr_rule in dr_rules:
                dr_rule_objects.append(DRRule(**dr_rule))
            return dr_rule_objects
        else:
            raise ValueError("Data reconciliation rules data is invalid.")
    except ValueError as error:
        logging.error(error)
        raise
