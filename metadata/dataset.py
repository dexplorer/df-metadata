from dataclasses import dataclass
import json
from utils import file_io as uff

import logging


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
class Dataset:
    kind: str
    dataset_id: str
    catalog_ind: str
    schedule_id: str
    dq_rule_ids: list[str]
    model_parameters: ModelParameters

    def __init__(
        self,
        dataset_id: str,
        catalog_ind: bool,
        schedule_id: str,
        dq_rule_ids: list[str] | None,
        model_parameters: ModelParameters | dict | None,
    ):
        self.kind = "generic"
        self.dataset_id = dataset_id
        self.catalog_ind = catalog_ind
        self.schedule_id = schedule_id
        self.dq_rule_ids = dq_rule_ids
        if isinstance(model_parameters, dict):
            self.model_parameters = ModelParameters(**model_parameters)
        else:
            self.model_parameters = model_parameters

    @classmethod
    def from_json(self, dataset_id):
        json_file = "https://github.com/dexplorer/df-metadata/blob/ddf4fff0e85a4175fbd34406bedb9bbb14e0edb6/metadata/api_data/datasets.json"
        json_key = "datasets"
        # with open(json_file, 'r') as f:
        with uff.uf_open_file(file_path=json_file, open_mode="r") as f:
            datasets = json.load(f)[json_key]
            # print(datasets)

        try:
            if datasets:
                for dataset in datasets:
                    # print(dataset)
                    if dataset["dataset_id"] == dataset_id:
                        return self(**dataset)
            else:
                raise ValueError("Dataset data is invalid.")
        except ValueError as error:
            logging.error(error)
            raise


@dataclass
class DelimFileDataset(Dataset):
    file_delim: str

    def __init__(
        self,
        dataset_id: str,
        catalog_ind: bool,
        schedule_id: str,
        dq_rule_ids: list[str] | None,
        model_parameters: ModelParameters | dict | None,
        file_delim: str,
    ):
        super().__init__(
            dataset_id, catalog_ind, schedule_id, dq_rule_ids, model_parameters
        )
        self.kind = "delim file"
        self.file_delim = file_delim


@dataclass
class LocalDelimFileDataset(DelimFileDataset):
    file_path: str

    def __init__(
        self,
        dataset_id: str,
        catalog_ind: bool,
        schedule_id: str,
        dq_rule_ids: list[str] | None,
        model_parameters: ModelParameters | dict | None,
        file_delim: str,
        file_path: str,
    ):
        super().__init__(
            dataset_id,
            catalog_ind,
            schedule_id,
            dq_rule_ids,
            model_parameters,
            file_delim,
        )
        self.kind = "local delim file"
        self.file_path = file_path

    def resolve_file_path(self, date_str):
        return self.file_path.replace("yyyymmdd", date_str)


@dataclass
class AWSS3DelimFileDataset(DelimFileDataset):
    s3_uri: str

    def __init__(
        self,
        dataset_id: str,
        catalog_ind: bool,
        schedule_id: str,
        dq_rule_ids: list[str] | None,
        model_parameters: ModelParameters | dict | None,
        file_delim: str,
        s3_uri: str,
    ):
        super().__init__(
            dataset_id,
            catalog_ind,
            schedule_id,
            dq_rule_ids,
            model_parameters,
            file_delim,
        )
        self.kind = "aws s3 delim file"
        self.s3_uri = s3_uri


@dataclass
class AzureADLSDelimFileDataset(DelimFileDataset):
    adls_uri: str

    def __init__(
        self,
        dataset_id: str,
        catalog_ind: bool,
        schedule_id: str,
        dq_rule_ids: list[str] | None,
        model_parameters: ModelParameters | dict | None,
        file_delim: str,
        adls_uri: str,
    ):
        super().__init__(
            dataset_id,
            catalog_ind,
            schedule_id,
            dq_rule_ids,
            model_parameters,
            file_delim,
        )
        self.kind = "azure adls delim file"
        self.adls_uri = adls_uri
