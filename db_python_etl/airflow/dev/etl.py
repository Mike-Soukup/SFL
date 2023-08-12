"""Demo script for reading, transforming, and ingesting our data"""
### Import Libraries
import logging
import psycopg2
import pandas as pd
import hashlib
from datetime import datetime

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
    df["concat"] = df["first_name"] + df["last_name"] + df["email"] + df["ip_address"]
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


if __name__ == "__main__":
    path = "/Users/mikesoukup/Desktop/NextLevel/Deloitte/SFL/db_python_etl/airflow/SRDataEngineerChallenge_DATASET.csv"
    etl_driver(path)
