#!/bin/bash
airflow db init

airflow users create \
    --username mtsoukup \
    --firstname Mike \
    --lastname Soukup \
    --role Admin \
    --email michaeltsoukup@gmail.com

airflow webserver --port 8080