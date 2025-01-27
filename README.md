[![metadata CI pipeline with Github Actions](https://github.com/dexplorer/df-metadata/actions/workflows/ci.yml/badge.svg)](https://github.com/dexplorer/df-metadata/actions/workflows/ci.yml)

# df-metadata

This is a metadata management application. It implements modules to serve out the following metadata and associated functionalities.

* Datasets (includes Data Quality ML model parameters)
* Data Quality Expectations
* Data Quality Rules
* Schedules
* Holidays
* Data Reconciliation Expectations
* Data Reconciliation Rules


### Install

- **Install via setuptools**
  ```sh
    python setup.py install
  ```

### Imports

- **Datasets**
  ```
  from metadata import datasets
  ```  

- **Data Quality Expectations**
  ```
  from metadata import dq_expectations
  ```  

- **Data Quality Rules**
  ```
  from metadata import dq_rules
  ```  

- **Schedules**
  ```
  from metadata import schedules
  ```  

- **Holidays**
  ```
  from metadata import holidays
  ```  

- **Data Reconciliation Expectations**
  ```
  from metadata import dr_expectations
  ```  

- **Data Reconciliation Rules**
  ```
  from metadata import dr_rules
  ```  

