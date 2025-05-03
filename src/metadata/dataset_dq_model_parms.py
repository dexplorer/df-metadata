import logging
from dataclasses import dataclass

from utils import http_io as ufh


@dataclass
class Feature:
    column: str
    variable_type: str
    variable_sub_type: str
    encoding: str


@dataclass
class DataSnapshot:
    snapshot: str


@dataclass
class ModelParameters:
    features: list[Feature]
    hist_data_snapshots: list[DataSnapshot]
    sample_size: int

    def __init__(
        self,
        features: list[Feature] | list[dict],
        hist_data_snapshots: list[DataSnapshot] | list[dict],
        sample_size: int,
    ):
        if isinstance(features, list) and all(
            isinstance(feature, dict) for feature in features
        ):
            self.features = [Feature(**feature) for feature in features]
        else:
            self.features = features

        if isinstance(hist_data_snapshots, list) and all(
            isinstance(snapshot, dict) for snapshot in hist_data_snapshots
        ):
            self.hist_data_snapshots = [
                DataSnapshot(**snapshot) for snapshot in hist_data_snapshots
            ]
        else:
            self.hist_data_snapshots = hist_data_snapshots

        self.sample_size = sample_size


@dataclass
class DatasetDQModelParameters:
    dataset_id: str
    model_parameters: ModelParameters

    def __init__(
        self,
        dataset_id: str,
        model_parameters: ModelParameters | dict | None,
    ):
        self.dataset_id = dataset_id
        if isinstance(model_parameters, dict):
            self.model_parameters = ModelParameters(**model_parameters)
        else:
            self.model_parameters = model_parameters

    @classmethod
    def from_json(cls, dataset_id):
        json_file_url = "https://raw.githubusercontent.com/dexplorer/df-metadata/refs/heads/main/api_data/dataset_dq_model_parms.json"
        json_key = "dq_model_parms"

        response = ufh.get_request(url=json_file_url)
        try:
            datasets_dq_model_parms = response.json()[json_key]
            # print(datasets_dq_model_parms)
            if datasets_dq_model_parms:
                for dataset_dq_model_parms in datasets_dq_model_parms:
                    # print(dataset_dq_model_parms)
                    if dataset_dq_model_parms["dataset_id"] == dataset_id:
                        return cls(**dataset_dq_model_parms)
            else:
                raise ValueError("Dataset DQ model parms data is invalid.")
        except ValueError as error:
            logging.error(error)
            raise
