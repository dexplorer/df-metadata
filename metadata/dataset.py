from dataclasses import dataclass
from enum import StrEnum

# from typing import Optional
from utils import http_io as ufh

import logging


class DatasetKind(StrEnum):
    GENERIC = "generic"
    DELIM_FILE = "delim file"
    LOCAL_DELIM_FILE = "local delim file"
    AWS_S3_DELIM_FILE = "aws s3 delim file"
    AZURE_ADLS_DELIM_FILE = "azure adls delim file"
    SPARK_TABLE = "spark table"
    SAPRK_SQL_FILE = "spark sql file"

class FileDelimiter(StrEnum):
    CSV_FILE = ","
    PIPE_DELIM_FILE = "|"

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
    kind: DatasetKind
    dataset_id: str
    catalog_ind: str
    schedule_id: str
    dq_rule_ids: list[str]
    model_parameters: ModelParameters

    def __init__(
        self,
        dataset_id: str,
        dataset_kind: str,
        catalog_ind: bool,
        schedule_id: str | None,
        dq_rule_ids: list[str] | None,
        model_parameters: ModelParameters | dict | None,
    ):
        # self.kind = DatasetKind.GENERIC
        self.kind = DatasetKind(dataset_kind)
        self.dataset_id = dataset_id
        self.catalog_ind = catalog_ind
        self.schedule_id = schedule_id
        self.dq_rule_ids = dq_rule_ids
        if isinstance(model_parameters, dict):
            self.model_parameters = ModelParameters(**model_parameters)
        else:
            self.model_parameters = model_parameters

    @classmethod
    def from_json(cls, dataset_id):
        json_file_url = "https://raw.githubusercontent.com/dexplorer/df-metadata/refs/heads/main/metadata/api_data/datasets.json"
        # json_file_url = f"file:///workspaces/df-metadata/metadata/api_data/datasets.json"
        json_key = "datasets"

        response = ufh.get_http_response(url=json_file_url)
        try:
            datasets = response.json()[json_key]
            # print(datasets)
            if datasets:
                for dataset in datasets:
                    # print(dataset)
                    if dataset["dataset_id"] == dataset_id:
                        return cls(**dataset)
            else:
                raise ValueError("Dataset data is invalid.")
        except ValueError as error:
            logging.error(error)
            raise


@dataclass(kw_only=True)
class DelimFileDataset(Dataset):
    file_delim: str

    def __init__(
        self,
        dataset_id: str,
        dataset_kind: str,
        catalog_ind: bool,
        schedule_id: str | None,
        dq_rule_ids: list[str] | None,
        model_parameters: ModelParameters | dict | None,
        file_delim: str,
    ):
        super().__init__(
            dataset_id,
            dataset_kind,
            catalog_ind,
            schedule_id,
            dq_rule_ids,
            model_parameters,
        )
        # self.kind = DatasetKind.DELIM_FILE
        self.file_delim = file_delim


@dataclass(kw_only=True)
class LocalDelimFileDataset(DelimFileDataset):
    file_path: str
    recon_file_delim: str
    recon_file_path: str

    def __init__(
        self,
        dataset_id: str,
        dataset_kind: str,
        catalog_ind: bool,
        schedule_id: str | None,
        dq_rule_ids: list[str] | None,
        model_parameters: ModelParameters | dict | None,
        file_delim: str,
        file_path: str,
        recon_file_delim: str,
        recon_file_path: str,
    ):
        super().__init__(
            dataset_id,
            dataset_kind,
            catalog_ind,
            schedule_id,
            dq_rule_ids,
            model_parameters,
            file_delim,
        )
        # self.kind = DatasetKind.LOCAL_DELIM_FILE
        self.file_path = file_path
        self.recon_file_delim = recon_file_delim
        self.recon_file_path = recon_file_path

    def resolve_file_path(self, date_str):
        return self.file_path.replace("yyyymmdd", date_str)

    def resolve_recon_file_path(self, date_str):
        return self.recon_file_path.replace("yyyymmdd", date_str)


@dataclass(kw_only=True)
class AWSS3DelimFileDataset(DelimFileDataset):
    s3_uri: str

    def __init__(
        self,
        dataset_id: str,
        dataset_kind: str,
        catalog_ind: bool,
        schedule_id: str | None,
        dq_rule_ids: list[str] | None,
        model_parameters: ModelParameters | dict | None,
        file_delim: str,
        s3_uri: str,
    ):
        super().__init__(
            dataset_id,
            dataset_kind,
            catalog_ind,
            schedule_id,
            dq_rule_ids,
            model_parameters,
            file_delim,
        )
        # self.kind = DatasetKind.AWS_S3_DELIM_FILE
        self.s3_uri = s3_uri


@dataclass(kw_only=True)
class AzureADLSDelimFileDataset(DelimFileDataset):
    adls_uri: str

    def __init__(
        self,
        dataset_id: str,
        dataset_kind: str,
        catalog_ind: bool,
        schedule_id: str | None,
        dq_rule_ids: list[str] | None,
        model_parameters: ModelParameters | dict | None,
        file_delim: str,
        adls_uri: str,
    ):
        super().__init__(
            dataset_id,
            dataset_kind,
            catalog_ind,
            schedule_id,
            dq_rule_ids,
            model_parameters,
            file_delim,
        )
        # self.kind = DatasetKind.AZURE_ADLS_DELIM_FILE
        self.adls_uri = adls_uri


@dataclass(kw_only=True)
class SparkTableDataset(Dataset):
    database_name: str
    table_name: str
    partition_keys: list[str] | None
    recon_file_delim: str | None
    recon_file_path: str | None

    def __init__(
        self,
        dataset_id: str,
        dataset_kind: str,
        catalog_ind: bool,
        schedule_id: str | None,
        dq_rule_ids: list[str] | None,
        model_parameters: ModelParameters | dict | None,
        database_name: str,
        table_name: str,
        partition_keys: list[str] | None,
        recon_file_delim: str | None,
        recon_file_path: str | None,
    ):
        super().__init__(
            dataset_id,
            dataset_kind,
            catalog_ind,
            schedule_id,
            dq_rule_ids,
            model_parameters,
        )
        # self.kind = DatasetKind.SPARK_TABLE
        self.database_name = database_name
        self.table_name = table_name
        self.partition_keys = partition_keys
        self.recon_file_delim = recon_file_delim
        self.recon_file_path = recon_file_path

    def get_qualified_table_name(self):
        return f"{self.database_name}.{self.table_name}"

    def resolve_recon_file_path(self, date_str):
        return self.recon_file_path.replace("yyyymmdd", date_str)


@dataclass(kw_only=True)
class SparkSqlFileDataset(Dataset):
    sql_file_path: str

    def __init__(
        self,
        dataset_id: str,
        dataset_kind: str,
        catalog_ind: bool,
        schedule_id: str | None,
        dq_rule_ids: list[str] | None,
        model_parameters: ModelParameters | dict | None,
        sql_file_path: str,
    ):
        super().__init__(
            dataset_id,
            dataset_kind,
            catalog_ind,
            schedule_id,
            dq_rule_ids,
            model_parameters,
        )
        self.sql_file_path = sql_file_path


def get_dataset_from_json(dataset_id):
    json_file_url = "https://raw.githubusercontent.com/dexplorer/df-metadata/refs/heads/main/metadata/api_data/datasets.json"
    # json_file_url = f"file:///workspaces/df-metadata/metadata/api_data/datasets.json"
    json_key = "datasets"

    response = ufh.get_http_response(url=json_file_url)
    try:
        datasets = response.json()[json_key]
        # print(datasets)
        if datasets:
            for dataset in datasets:
                # print(dataset)
                if dataset["dataset_id"] == dataset_id:
                    if dataset["dataset_kind"] == DatasetKind.LOCAL_DELIM_FILE:
                        return LocalDelimFileDataset(**dataset)
                    elif dataset["dataset_kind"] == DatasetKind.SPARK_TABLE:
                        return SparkTableDataset(**dataset)
        else:
            raise ValueError("Dataset data is invalid.")
    except ValueError as error:
        logging.error(error)
        raise
