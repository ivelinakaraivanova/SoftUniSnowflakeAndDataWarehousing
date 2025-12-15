from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # type: ignore


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
