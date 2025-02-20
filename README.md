[![metadata CI pipeline with Github Actions](https://github.com/dexplorer/df-metadata/actions/workflows/ci.yml/badge.svg)](https://github.com/dexplorer/df-metadata/actions/workflows/ci.yml)

# df-metadata

This is a metadata management application. It implements modules to serve out the following metadata and associated functionalities.

* Datasets 
  - Dataset Schemas
  - Dataset Dictionaries
* System Glossary
* Business Glossary
* Data Quality Expectations
* Data Quality Rules
* Data Quality ML model parameters
* Schedules
* Holidays
* Data Reconciliation Expectations
* Data Reconciliation Rules
* Data Ingestion Workflows
  - Ingestion Maps
* Data Distribution Workflows
* Data Sources
* Data Consumers


### Install

- **Install via Makefile and pip**
  ```sh
    make install
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

- **Data Ingestion and Distribution Workflows**
  ```
  from metadata import workflows
  ```  

- **Data Ingestion and Distribution Tasks**
  ```
  from metadata import integration_tasks
  ```  

