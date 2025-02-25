from dataclasses import dataclass
from enum import StrEnum

# from typing import Optional
from utils import http_io as ufh

import logging


class DatasetType(StrEnum):
    GENERIC = "generic"
    DELIM_FILE = "delim file"
    LOCAL_DELIM_FILE = "local delim file"
    AWS_S3_DELIM_FILE = "aws s3 delim file"
    AZURE_ADLS_DELIM_FILE = "azure adls delim file"
    SPARK_TABLE = "spark table"
    SPARK_SQL_FILE = "spark sql file"


class FileDelimiter(StrEnum):
    CSV_FILE = ","
    PIPE_DELIM_FILE = "|"


@dataclass
class Dataset:
    dataset_id: str
    dataset_type: DatasetType
    schedule_id: str

    def __init__(
        self,
        dataset_id: str,
        dataset_type: str,
        schedule_id: str | None,
    ):
        self.dataset_type = dataset_type
        self.dataset_id = dataset_id
        self.schedule_id = schedule_id

    @classmethod
    def from_json(cls, dataset_id):
        json_file_url = "https://raw.githubusercontent.com/dexplorer/df-metadata/refs/heads/main/api_data/datasets.json"
        # json_file_url = f"file:///workspaces/df-metadata/api_data/datasets.json"
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
        dataset_type: str,
        schedule_id: str | None,
        file_delim: str,
    ):
        super().__init__(
            dataset_id,
            dataset_type,
            schedule_id,
        )
        self.file_delim = file_delim


@dataclass(kw_only=True)
class LocalDelimFileDataset(DelimFileDataset):
    file_path: str
    recon_file_delim: str
    recon_file_path: str

    def __init__(
        self,
        dataset_id: str,
        dataset_type: str,
        schedule_id: str | None,
        file_delim: str,
        file_path: str,
        recon_file_delim: str,
        recon_file_path: str,
    ):
        super().__init__(
            dataset_id,
            dataset_type,
            schedule_id,
            file_delim,
        )
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
        dataset_type: str,
        schedule_id: str | None,
        file_delim: str,
        s3_uri: str,
    ):
        super().__init__(
            dataset_id,
            dataset_type,
            schedule_id,
            file_delim,
        )
        self.s3_uri = s3_uri


@dataclass(kw_only=True)
class AzureADLSDelimFileDataset(DelimFileDataset):
    adls_uri: str

    def __init__(
        self,
        dataset_id: str,
        dataset_type: str,
        schedule_id: str | None,
        file_delim: str,
        adls_uri: str,
    ):
        super().__init__(
            dataset_id,
            dataset_type,
            schedule_id,
            file_delim,
        )
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
        dataset_type: str,
        schedule_id: str | None,
        database_name: str,
        table_name: str,
        partition_keys: list[str] | None,
        recon_file_delim: str | None,
        recon_file_path: str | None,
    ):
        super().__init__(
            dataset_id,
            dataset_type,
            schedule_id,
        )
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
        dataset_type: str,
        schedule_id: str | None,
        sql_file_path: str,
    ):
        super().__init__(
            dataset_id,
            dataset_type,
            schedule_id,
        )
        self.sql_file_path = sql_file_path


def get_dataset_from_json(dataset_id):
    json_file_url = "https://raw.githubusercontent.com/dexplorer/df-metadata/refs/heads/main/api_data/datasets.json"
    # json_file_url = f"file:///workspaces/df-metadata/api_data/datasets.json"
    json_key = "datasets"

    response = ufh.get_http_response(url=json_file_url)
    try:
        datasets = response.json()[json_key]
        # print(datasets)
        if datasets:
            for dataset in datasets:
                # print(dataset)
                if dataset["dataset_id"] == dataset_id:
                    if dataset["dataset_type"] == DatasetType.LOCAL_DELIM_FILE:
                        return LocalDelimFileDataset(**dataset)
                    elif dataset["dataset_type"] == DatasetType.SPARK_TABLE:
                        return SparkTableDataset(**dataset)
                    elif dataset["dataset_type"] == DatasetType.SPARK_SQL_FILE:
                        return SparkSqlFileDataset(**dataset)
        else:
            raise ValueError("Dataset data is invalid.")
    except ValueError as error:
        logging.error(error)
        raise
