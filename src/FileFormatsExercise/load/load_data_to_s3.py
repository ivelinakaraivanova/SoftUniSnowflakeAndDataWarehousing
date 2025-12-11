import logging
import pandas as pd

from config.s3_utils import get_s3_client_and_storage_options


def load_data_to_s3(df: pd.DataFrame, bucket_name: str, file_key: str, file_format: str) -> None:
    """
    Loads the given DataFrame to an S3 bucket in the specified format (CSV, JSON, or Parquet).

    """
    _, storage_options = get_s3_client_and_storage_options()
    
    s3_path = f"s3://{bucket_name}/{file_key}"
    
    logging.info(f"Starting to upload DataFrame to {s3_path} in {file_format} format")

    try:
        if file_format == "csv":
            df.to_csv(s3_path, index=False, storage_options=storage_options)
        elif file_format == "json":
            df.to_json(s3_path, orient="records", storage_options=storage_options)
        elif file_format == "parquet":
            df.to_parquet(s3_path, index=False, storage_options=storage_options)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
    except Exception as e:
        logging.error(f"Error uploading DataFrame to {s3_path}: {e}")
        raise

    logging.info(f"Successfully uploaded DataFrame to {s3_path}")
    