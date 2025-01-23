# from typing_extensions import TypedDict
from dataclasses import dataclass
from utils import http_io as ufh 

import logging


@dataclass
class DQRule:
    rule_id: str
    dataset_id: str
    exp_id: str
    rule_fail_action: str

    def __init__(self, rule_id, dataset_id, exp_id, rule_fail_action, **kwargs):
        self.rule_id = rule_id
        self.dataset_id = dataset_id
        # self.column_name = column_name
        self.exp_id = exp_id
        self.rule_fail_action = rule_fail_action
        self.kwargs = kwargs


def get_dq_rules_by_dataset_id(dataset_id: str, dq_rules: list[DQRule]) -> list[DQRule]:
    dq_rules_for_dataset = []

    for dq_rule in dq_rules:
        if dataset_id == dq_rule.dataset_id:
            dq_rules_for_dataset.append(dq_rule)

    # print(dq_rules_for_dataset)
    return dq_rules_for_dataset


def get_all_dq_rules_from_json() -> list[DQRule]:
    json_file_url = "https://raw.githubusercontent.com/dexplorer/df-metadata/refs/heads/main/metadata/api_data/dq_rules.json"
    json_key = "dq_rules"

    response = ufh.get_http_response(url=json_file_url)
    try:
        dq_rules = response.json()[json_key]
        if dq_rules:
            # print(dq_rules)
            dq_rule_objects = []
            for dq_rule in dq_rules:
                dq_rule_objects.append(DQRule(**dq_rule))
            return dq_rule_objects
        else:
            raise ValueError("DQ rules data is invalid.")
    except ValueError as error:
        logging.error(error)
        raise
