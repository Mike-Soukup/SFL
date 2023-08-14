# Overview

This module will demonstrate how an ETL Pipeline can be created with the objective to read a flat file (Excel file provided), conduct ETL Operations on PII data, and ingest this data into a local instance of PostgreSQL. This pipeline will be managed via a local Airflow deployment.

## Database Selection:

For this task, I chose to create a relational database with on a local PostgreSQL deployment. After initial EDA, I found that a relational data store suited the data provided as the data already had a well defined schema. Also, with little understanding of the application requirements for this data, a relational data store is a safe choice until further information is provided by the client and other stakeholders. 

One database was created to support the storage of this data. A MD5 hashed string was chosen as the primary key for this dataset with the aim of protecting PII if this table were to serve as a dimension for other tables. Protecting PII, such as that information contained within this dataset, by minimizing the spread of the data is paramount to proper data governance and privacy. 

The schema for the created `persons` table is as follows:<br>
PK - hash (varchar(50)) <br>
id (smallint) <br>
first_name (varchar(50)) <br>
last_name (varchar(50)) <br>
email (varchar(50)) <br>
gender (varchar(2)) <br>
ip_address (varchar(20)) <br>
timestamp (numeric) <br>

## ETL Tasks:

The ETL tasks for this pipeline will create the target `persons` database if one does not exist already. Transform the provided data by creating an MD5 hashed primary key, mapping the gender column to one or two letter acronyms, and applying a timestamp to the data. Once the data has been transformed sufficiently, it will be loaded into the `persons` table. Finally, there will be a routine check to validate if the data in the table has gone stale (i.e. becoming old and potentially irrelevant)

### Run Instructions:
In order to run this DAG in Airflow, there are two pre-requisites. First, this repo must be cloned and second there must be an active instance of Airflow.

1. Be sure to enter your own credentials for accessing the PostgreSQL database. For instance, look for the `YOUR_CREDENTIALS` string in the dag file and enter your password. 
2. Source the virtual environment located in `.airflow_venv`
3. Set up environemnt by executing: `make install`
4. Start the Airflow Webserver by executing: `./airflow_start.sh` -- or call `make webserver`
    Enter any password as desired
5. Open another terminal and source the virtual environment
6. Start the Airflow Scheduler by executing: `./airflow_scheduler.sh` -- or call `make scheduler`
7. Open webpage to localhost port 8080
    - Open a web browser and in the address bar type: `localhost:8080`
Here you should see the Airflow GUI and the SFL_Airflow_ETL DAG in the DAGs tab
8. Turn the DAG On to run the ETL Pipeline

[Demonstration Video](https://youtu.be/fBfXb-7X1mY)

### ETL Pipeline in GCP:

In addition to the ETL Pipeline I wrote in Airflow, I also created a quick cloud based solution using GCP Cloud Storage, BigQuery, and DataPrep. I wanted to share this example since this is a cloud based, low code solution. I think this type of implementaiton would be great to integrate with teams that want to manage their data, but don't know how to code. 

[Cloud Storage -> DataPrep -> BigQuery](https://youtu.be/rQxzZBW_iRU)

### Opportunities for Future Improvement:
- Modularize source code
- Use hosted deployments of Airflow and PostgreSQL
- Better understanding of data and business objective;
    - Where is the data coming from?
    - What will stakeholders be using the data for?
    - What industry regulations and requirements must data storage conform to?
