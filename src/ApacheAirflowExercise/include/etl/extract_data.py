import pandas as pd

from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # type: ignore
from ..logger import setup_logger


logging = setup_logger("etl.extract_data")


def get_storage_options(aws_conn_id: str):
    """
    Retrieves storage options for accessing S3 using the provided S3Hook.

    """
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    credentials = s3_hook.get_credentials()

    storage_options = {
        "key": credentials.access_key,
        "secret": credentials.secret_key,
    }

    return s3_hook, storage_options


def extract_data_from_s3(bucket: str, folder: str, aws_conn_id: str) -> dict:
    """
    Extracts data from CSV files stored in an S3 bucket and folder.

    """
    s3_hook, storage_options = get_storage_options(aws_conn_id)
    keys = s3_hook.list_keys(bucket_name=bucket, prefix=folder)

    if not keys:
        raise ValueError(f"No files found in bucket '{bucket}' with prefix '{folder}'")

    dfs = {}

    for key in keys:
        if not key.lower().endswith(".csv"):
            logging.info(f"Skipping non-csv file {key}")
            continue

        s3_path = f"s3://{bucket}/{key}"
        logging.info(f"Extracting data from {s3_path}")

        try:
                df = pd.read_csv(s3_path, storage_options=storage_options)
                if df.empty:
                    logging.warning(f"No data found in {s3_path}")
                    continue
        except Exception as e:
            logging.error(f"Error reading {s3_path}: {e}")
            raise

        dfs[key] = df
    
    return dfs

    