###Import packages
import logging
from datetime import datetime
import hashlib
import pandas as pd
import psycopg2  ## Used to programmatically make connections with PostgreSQL
import numpy as np
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator


@task(task_id="create_data_table")
def create_data_table(**kwargs):
    def connect_to_postgres(
        database: str = "postgres",
        user: str = "postgres",
        host: str = "localhost",
        password: str = None,
        post: str = "5433",
        **kwargs,
    ):
        """'This function will return a connection cursor
        to a locally hosted PostgreSQL Database."""

        conn = psycopg2.connect(
            database=database,
            user="postgres",
            password=password,
            host="localhost",
            port="5433",
        )
        conn.autocommit = True
        cur = conn.cursor()
        logging.info("Connection to PostgreSQL Established.")
        return cur, conn

    def create_db(cur, db_name: str = None):
        """Function to create a new database in PostgreSQL."""
        sql = f"""
        CREATE database {db_name}
        """
        try:
            cur.execute(sql)
            logging.info(f"{db_name} Created Successfull!")
            cur.close()
        except psycopg2.errors.DuplicateDatabase:
            logging.warning("Database already exists!")
            cur.close()

    def create_destination_table(db_name: str = None):
        """Create the persons destination table in PostgreSQL"""
        cur, _ = connect_to_postgres(database=db_name, password="MounT@inM@n1992")

        sql = """
            CREATE TABLE IF NOT EXISTS public.persons
            (
                id smallint NOT NULL,
                hash character varying(50) NOT NULL,
                first_name character varying(50) NOT NULL,
                last_name character varying(50) NOT NULL,
                email character varying(50) NOT NULL,
                gender character varying(2) NOT NULL,
                ip_address character varying(20) NOT NULL,
                timestamp numeric NOT NULL,
                CONSTRAINT persons_pkey PRIMARY KEY (hash)
            )

            TABLESPACE pg_default;

            ALTER TABLE public.persons
                OWNER to postgres;
            """
        cur.execute(sql)
        cur.close()
        logging.info("Table Created Successfully!")

    password = "MounT@inM@n1992"
    db_name = "SFL"
    cur, conn = connect_to_postgres(password=password)
    create_db(cur, db_name=db_name)
    create_destination_table(db_name=db_name)
    conn.commit()
    conn.close()


@task(task_id="etl")
def etl(**kwargs):
    GENDER_MAP = {
        "Genderfluid": "GF",
        "Female": "F",
        "Genderqueer": "GQ",
        "Male": "M",
        "Agender": "A",
        "Bigender": "B",
        "Polygender": "P",
        "Non-binary": "NB",
    }

    def read_data(path: str = None) -> pd.DataFrame:
        """Return pandas data frame provided a .csv file path"""
        ### Make sure path is relative to DAG location:
        return pd.read_csv(path)

    def apply_gender_map(df: pd.DataFrame, map: dict = None) -> pd.DataFrame:
        """Map gender to 1 or 2 letter acronyms provided mapping"""
        df["gender"] = df["gender"].apply(lambda x: GENDER_MAP.get(x, "UNKWN"))
        return df

    def create_pk(df: pd.DataFrame) -> pd.DataFrame:
        """Driver function to hash pii to create primary key"""
        df["concat"] = (
            df["first_name"] + df["last_name"] + df["email"] + df["ip_address"]
        )
        df["hash"] = df["concat"].apply(lambda x: hashlib.md5(x.encode()).hexdigest())
        df.drop(columns=["concat"], axis=1, inplace=True)
        return df

    def create_timestamp(df: pd.DataFrame) -> pd.DataFrame:
        """Create timestamp for data"""
        df["timestamp"] = datetime.timestamp(datetime.now())
        return df

    def load_data(df: pd.DataFrame):
        """Driver function to enter data into PostgreSQL"""
        ### Establish DB Connection
        conn = psycopg2.connect(
            host="localhost",
            database="SFL",
            user="postgres",
            password="MounT@inM@n1992",
            port="5433",
        )

        sql = f"""
        INSERT INTO persons (id, 
                            hash,
                            first_name,
                            last_name,
                            email,
                            gender,
                            ip_address,
                            timestamp)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (hash)
        DO UPDATE
        SET id = EXCLUDED.id,
            gender = EXCLUDED.gender,
            ip_address = EXCLUDED.ip_address,
            timestamp = EXCLUDED.timestamp;

        """
        cur = conn.cursor()
        for index, row in df.iterrows():
            values = (
                int(row["id"]),
                str(row["hash"]),
                str(row["first_name"]),
                str(row["last_name"]),
                str(row["email"]),
                str(row["gender"]),
                str(row["ip_address"]),
                float(row["timestamp"]),
            )
            cur.execute(sql, values)
            conn.commit()
        cur.close()
        conn.close()

    def etl_driver(path):
        """Driver function for executing ETL operations"""
        ### Read data from CSV file:
        df = read_data(path)

        logging.info("Data read from csv")
        ### Apply gender mapping:
        df = apply_gender_map(df)

        logging.info("Gender mapping applied")
        ### Hash PII to make PK:
        df = create_pk(df)

        logging.info("Primary Key created")
        ### Apply timestamp to data:
        df = create_timestamp(df)

        logging.info("Timestamps applied to dataframe")
        ### Load data into PostgreSQL
        load_data(df)
        logging.info("Data loaded into PostgreSQL!")

    path = "/Users/mikesoukup/Desktop/NextLevel/Deloitte/SFL/db_python_etl/airflow/SRDataEngineerChallenge_DATASET.csv"
    etl_driver(path)


@task(task_id="data_check")
def verify_recent_data(**kwargs):
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

    d = pull_data()
    determine_freshness(d)


args = {
    "owner": "Mike Soukup",
    "start_date": days_ago(1),
    "retries": 2,
}

with DAG(
    dag_id="SFL_Airflow_ETL",
    schedule="@hourly",
    default_args=args,
    catchup = False,
) as dag:
    create_data_table() >> etl() >> verify_recent_data()
