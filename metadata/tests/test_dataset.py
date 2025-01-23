import pytest 
from metadata import dataset as ds 

@pytest.fixture(scope='module') 
def local_delim_file_dataset():
    dataset = ds.LocalDelimFileDataset(
        dataset_id = '1',
        catalog_ind =  True,
        schedule_id = '1',
        dq_rule_ids = None, 
        model_parameters = None,
        file_delim = ',',
        file_path = "APP_ROOT_DIR/data/acct_positions_yyyymmdd.csv",
        )
    print(dataset.file_path)
    return dataset 

class TestLocalDelimFileDataset:
    def setup(self):
        print("this is setup, runs once per test case")
    
    def teardown(self):
        print("this is teardown, runs once per test case")
    
    def setup_class(cls):
        print("this is setup class, runs once")

    def teardown_class(cls):
        print("this is teardown class, runs once")

    def test_resolve_file_path(self, local_delim_file_dataset, date_str='20240101'):
        dataset = local_delim_file_dataset
        print(type(dataset))
        expected_output = "APP_ROOT_DIR/data/acct_positions_20240101.csv"
        assert dataset.resolve_file_path(date_str) == expected_output
