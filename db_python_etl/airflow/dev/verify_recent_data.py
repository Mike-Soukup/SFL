"""Demo script for verifying data in the DB is recent."""
### Import Libraries
import logging
import psycopg2
import pandas as pd
from datetime import datetime


def pull_data():
    """Driver function to pull data from person table"""

    ### Estbalish connection to the Database
    conn = psycopg2.connect(
        host="localhost",
        database="SFL",
        user="postgres",
        password="MounT@inM@n1992",
        port="5433",
    )
    cur = conn.cursor()

    sql = """
    SELECT DISTINCT
    timestamp
    FROM public.persons
    ORDER BY timestamp DESC
    """

    ### Pull data timestamps
    cur.execute(sql)
    data = cur.fetchall()
    return data


def determine_freshness(d):
    """Function to log how fresh the data is"""
    ### Get most recent entry timestamp:
    most_recent_entry = float(d[0][0])

    ### Get current timestamp:
    now = datetime.timestamp(datetime.now())

    ### Get delta in seconds
    delta = now - most_recent_entry

    if delta < 300:
        logging.info("Data is Fresh!")
    else:
        logging.warning("Data is Stale!")


if __name__ == "__main__":
    d = pull_data()
    determine_freshness(d)
