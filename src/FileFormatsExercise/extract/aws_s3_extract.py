import logging
import pandas as pd
import pyarrow

from config.settings import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME, FILE_PATH_CSV, FILE_PATH_PARQUET
from config.s3_utils import get_s3_client_and_storage_options


def extract_csv_from_s3(bucket_name: str, file_key: str) -> pd.DataFrame:
    """
    Extracts CSV data from an S3 bucket and returns it as a pandas DataFrame.

    """
    _, storage_options = get_s3_client_and_storage_options()
    
    s3_path = f"s3://{bucket_name}/{file_key}"
    
    logging.info(f"Extracting CSV data from {s3_path}")

    try:
        df = pd.read_csv(s3_path, storage_options=storage_options)
    except Exception as e:
        logging.error(f"Error reading CSV file from S3: {e}")
        raise

    logging.info(f"Successfully extracted data from {s3_path}. Shape: {df.shape}")
    return df


def extract_json_from_s3(bucket_name: str, file_key: str) -> pd.DataFrame:
    """
    Extracts JSON data from an S3 bucket and returns it as a pandas DataFrame.

    """
    _, storage_options = get_s3_client_and_storage_options()
    
    s3_path = f"s3://{bucket_name}/{file_key}"
    
    logging.info(f"Extracting JSON data from {s3_path}")

    try:
        df = pd.read_json(s3_path, storage_options=storage_options)
    except Exception as e:
        logging.error(f"Error reading JSON file from S3: {e}")
        raise

    logging.info(f"Successfully extracted data from {s3_path}. Shape: {df.shape}")
    return df


def extract_parquet_from_s3(bucket_name: str, file_key: str) -> pd.DataFrame:
    """
    Extracts Parquet data from an S3 bucket and returns it as a pandas DataFrame.

    """
    _, storage_options = get_s3_client_and_storage_options()
    
    s3_path = f"s3://{bucket_name}/{file_key}"
    
    logging.info(f"Extracting Parquet data from {s3_path}")

    try:
        df = pd.read_parquet(s3_path, storage_options=storage_options)
    except Exception as e:
        logging.error(f"Error reading Parquet file from S3: {e}")
        raise

    logging.info(f"Successfully extracted data from {s3_path}. Shape: {df.shape}")
    return df

# repeatable code, can be done with one function!!!