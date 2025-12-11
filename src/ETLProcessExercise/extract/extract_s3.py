import pandas as pd
import fsspec
import s3fs

from botocore.exceptions import BotoCoreError

from config.s3_utils import get_s3_client_and_storage_options


def s3_extract(bucket_name: str, folder_name: str) -> list:
    s3, storage_options = get_s3_client_and_storage_options()
    mapping = {"sales": None, "product": None, "customer": None, "shipping": None}

    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
        contents = response.get('Contents', [])
        if not contents:
            raise FileNotFoundError(f"No objects found in bucket {bucket_name} with prefix {folder_name}")
    except BotoCoreError as e:
        print(f"Error connecting to S3 for {bucket_name}/{folder_name}: {e}")
        raise
    
    for obj in contents:
        key = obj["Key"]

        if not key.lower().endswith('.csv'):
            continue

        s3_path = f"s3://{bucket_name}/{key}"

        try:
            df = pd.read_csv(s3_path, storage_options=storage_options)
        except Exception as e:  
            print(f"Error reading {s3_path}: {e}")
            continue
        for name in mapping.keys():
            if name in key.lower():
                mapping[name] = df
                break

    return list(mapping.values())
