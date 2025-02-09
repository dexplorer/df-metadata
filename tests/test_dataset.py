import pytest
from metadata import dataset as ds


@pytest.fixture(scope="module", name="dataset")
def local_delim_file_dataset():
    dataset = ds.LocalDelimFileDataset(
        dataset_id="1",
        dataset_kind="local delim file",
        catalog_ind=True,
        schedule_id="1",
        dq_rule_ids=None,
        model_parameters=None,
        file_delim=",",
        file_path="APP_DATA_IN_DIR/acct_positions_yyyymmdd.csv",
        recon_file_delim="|",
        recon_file_path="APP_DATA_IN_DIR/acct_positions_yyyymmdd.recon",
    )
    print(dataset.file_path)
    return dataset


# class TestLocalDelimFileDataset:
#     def setup(self):
#         print("this is setup, runs once per test case")

#     def teardown(self):
#         print("this is teardown, runs once per test case")

#     def setup_class(cls):
#         print("this is setup class, runs once")

#     def teardown_class(cls):
#         print("this is teardown class, runs once")

#     def test_resolve_file_path(self, dataset, date_str='20240101'):
#         expected_output = "APP_DATA_IN_DIR/acct_positions_20240101.csv"
#         assert dataset.resolve_file_path(date_str) == expected_output

test_data1 = [
    ("20240101", "APP_DATA_IN_DIR/acct_positions_20240101.csv"),
]


@pytest.mark.parametrize("date_str, expected_output", test_data1)
def test_resolve_file_path(
    dataset: ds.LocalDelimFileDataset, date_str: str, expected_output: str
):
    assert dataset.resolve_file_path(date_str) == expected_output
