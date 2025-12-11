import pandas as pd


from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook  # type: ignore
from ..logger import setup_logger


logging = setup_logger("etl.load_data")


def load_data_to_snowflake(df: pd.DataFrame, database: str, schema: str, table: str, snowflake_conn_id: str):
    """
    Loads the provided DataFrame into a Snowflake table.

    """
    logging.info(f"Loading data into Snowflake table {table}")

    if df.empty:
        raise ValueError("The provided DataFrame is empty")

    try:
        hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
        engine = hook.get_sqlalchemy_engine()

        df.to_sql(
            name=table,
            con=engine,
            schema=schema,
            index=False,
            if_exists="replace",
            method="multi",
        )
    except Exception as e:
        logging.error(f"Error loading data into Snowflake table {table}: {e}")
        raise

    logging.info(f"Data loaded successfully into Snowflake table {table}")