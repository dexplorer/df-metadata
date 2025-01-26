[![metadata CI pipeline with Github Actions](https://github.com/dexplorer/df-metadata/actions/workflows/ci.yml/badge.svg)](https://github.com/dexplorer/df-metadata/actions/workflows/ci.yml)

# df-metadata

This is a metadata management application. It implements modules to serve out the following metadata and associated functionalities.

* Datasets (includes DQ ML model parameters)
* DQ expectations
* DQ rules
* Schedules
* Holidays


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

- **DQ expectations**
  ```
  from metadata import dq_expectations
  ```  

- **DQ rules**
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
