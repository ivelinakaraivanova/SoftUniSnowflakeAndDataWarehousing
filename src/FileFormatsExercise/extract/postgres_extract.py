import logging
import pandas as pd
import psycopg2

from sqlalchemy import create_engine


def extract_from_database(sql_query: str, db_params: dict) -> pd.DataFrame:
    """
    Extracts data from a PostgreSQL database using the provided connection parameters and SQL query.

    """
    connection_string = (f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}"
                         f"@{db_params['host']}:{db_params['port']}/{db_params['database']}")
    
    logging.info(f"Connecting to database {connection_string}")

    try:
        engine = create_engine(connection_string)
    except Exception as e:
        logging.error(f"Error extracting data from database {connection_string}: {e}")
        raise

    logging.info(f"Executing query: {sql_query}")

    try:
        with engine.connect() as connection:   
            df = pd.read_sql_query(sql_query, connection)
    except Exception as e:
        logging.error(f"Error executing query on database {connection_string}: {e}")
        raise

    logging.info(f"Successfully extracted data from database. Shape: {df.shape}")
    return df