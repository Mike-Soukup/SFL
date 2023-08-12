# Overview

This module will demonstrate how an ETL Pipeline can be created by ingesting a flat file (Excel file provided), conduct ETL Operations on PII data, and ingest this data into a local instance of PostgreSQL. This pipeline will be managed via Airflow

## Run Instructions:

1. Source the virtual environment located in `.airflow_venv`
2. Set up environemnt by executing: `make install`
3. Start the Airflow Webserver by executing: `./airflow_start.sh`
    Enter any password as desired
4. Open another terminal and activate the virtual environment
5. Start the Airflow Scheduler by executing: `./airflow_scheduler.sh`