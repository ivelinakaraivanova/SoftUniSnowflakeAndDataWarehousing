import pandas as pd

from include.s3_utils import get_storage_options

from ..logger import setup_logger


logging = setup_logger("etl.load_data")


def load_df_to_s3_csv(df: pd.DataFrame, s3_path: str, aws_conn_id: str) -> None:
    """
    Loads a DataFrame to an S3 path in CSV format.

    """
    s3_hook, storage_options = get_storage_options(aws_conn_id)

    try:
        df.to_csv(s3_path, index=False, storage_options=storage_options)
        logging.info(f"DataFrame successfully loaded to {s3_path}")
    except Exception as e:
        logging.error(f"Failed to load DataFrame to {s3_path}: {e}")
        raise

    