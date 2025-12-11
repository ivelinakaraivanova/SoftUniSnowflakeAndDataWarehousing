import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import io
import pandas as pd
import requests 

from sqlalchemy import create_engine
from config.settings import S3


def extract_csv(bucket_name: str, folder_name: str, file_name: str) -> pd.DataFrame:
    csv_obj = S3.get_object(Bucket=bucket_name, Key=f"{folder_name}/{file_name}")
    df = pd.read_csv(csv_obj['Body'])
    return df


def extract_json(bucket_name: str, folder_name: str, file_name: str) -> pd.DataFrame:
    json_obj = S3.get_object(Bucket=bucket_name, Key=f"{folder_name}/{file_name}")
    json_bytes = json_obj['Body'].read()
    json_str = json_bytes.decode('utf-8')
    df = pd.read_json(io.StringIO(json_str))
    return df


def extract_parquet(bucket_name: str, folder_name: str, file_name: str) -> pd.DataFrame:
    parquet_obj = S3.get_object(Bucket=bucket_name, Key=f"{folder_name}/{file_name}")
    parquet_bytes = parquet_obj['Body'].read()
    df = pd.read_parquet(io.BytesIO(parquet_bytes))
    return df


def extract_api_data(api_url: str) -> pd.DataFrame:
    response = requests.get(api_url)

    if response.status_code == 200:
        data = response.json()
        df = pd.json_normalize(data)
        return df
    else:
        raise Exception(f"API request failed with status code {response.status_code}")


def extract_db_data(db_url: str) -> pd.DataFrame:
    engine = create_engine(db_url)
    query = "SELECT * FROM sales_data"
    db_data = pd.read_sql(query, engine)
    return db_data
