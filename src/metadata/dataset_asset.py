from dataclasses import dataclass
from utils import http_io as ufh

import logging


@dataclass
class DatasetAsset:
    dataset_id: str
    catalog_asset_name: str
    catalog_asset_domain: str
    business_owners: list[str]
    technology_owners: list[str]
    data_stewards: list[str]

    def __init__(
        self,
        dataset_id,
        catalog_asset_name,
        catalog_asset_domain,
        business_owners,
        technology_owners,
        data_stewards,
    ):
        self.dataset_id = dataset_id
        self.catalog_asset_name = catalog_asset_name
        self.catalog_asset_domain = catalog_asset_domain
        self.business_owners = business_owners
        self.technology_owners = technology_owners
        self.data_stewards = data_stewards

    @classmethod
    def from_json(cls, dataset_id):
        json_file_url = "https://raw.githubusercontent.com/dexplorer/df-metadata/refs/heads/main/api_data/dataset_asset.json"
        json_key = "assets"

        response = ufh.get_http_response(url=json_file_url)
        try:
            assets = response.json()[json_key]
            # print(assets)
            if assets:
                for asset in assets:
                    # print(asset)
                    if asset["dataset_id"] == dataset_id:
                        return cls(**asset)
            else:
                raise ValueError("Dataset asset data is invalid.")
        except ValueError as error:
            logging.error(error)
            raise
