"""Demo script for creating our target table in PostgreSQL"""
### Import Libraries
import logging
import psycopg2


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


if __name__ == "__main__":
    password = "MounT@inM@n1992"
    db_name = "SFL"
    cur, conn = connect_to_postgres(password=password)
    create_db(cur, db_name=db_name)
    create_destination_table(db_name=db_name)
    conn.commit()
    conn.close()
