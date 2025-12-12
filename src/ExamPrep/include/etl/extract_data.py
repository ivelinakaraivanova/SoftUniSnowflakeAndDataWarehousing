from airflow.exceptions import AirflowException
from include.s3_utils import get_storage_options

from ..logger import setup_logger


logging = setup_logger("etl.extract_data")

def extract_data_from_s3(bucket: str, folder: str, aws_conn_id: str, file_type: str = "csv") -> list:
    """
    Extracts data from different types of files on an S3.

    """

    logging.info(f"Extracting {file_type} files from s3://{bucket}/{folder}")

    s3_hook, storage_options = get_storage_options(aws_conn_id)
    keys = s3_hook.list_keys(bucket_name=bucket, prefix=folder)

    if not keys:
        raise AirflowException(f"No files found in s3://{bucket}/{folder}")

    matches_paths = [f"s3://{bucket}/{key}" for key in keys if key.endswith(f".{file_type}")]

    if not matches_paths:
        raise AirflowException(f"No {file_type} files found in s3://{bucket}/{folder}")

    return matches_paths
