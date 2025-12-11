import boto3


S3 = boto3.client(
    "s3",
    aws_access_key_id="",
    aws_secret_access_key="",
)

BUCKET_NAME = "iva-data-warehouse-10"
FOLDER_NAME = "FileFormats"
CSV_FILE_NAME = "sales_data.csv"
PARQUET_FILE_NAME = "sales_data.parquet"
JSON_FILE_NAME = "sales_data.json"

USER = "postgres"
PASSWORD = "postgres"
HOST = "localhost"
PORT = "5432"
DATABASE = "sales_data"