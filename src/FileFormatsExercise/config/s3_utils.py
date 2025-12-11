import boto3
import s3fs
import fsspec

from config.settings import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY


def get_s3_client_and_storage_options() -> tuple[boto3.client, dict]:
    """
    Create an S3 client and corresponding storage options for fsspec.

    Returns:
        A tuple containing the S3 client and a dictionary of storage options.
    """
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
    
    storage_options = {
        'key': s3_client._request_signer._credentials.access_key,
        'secret': s3_client._request_signer._credentials.secret_key,
    }

    return s3_client, storage_options

