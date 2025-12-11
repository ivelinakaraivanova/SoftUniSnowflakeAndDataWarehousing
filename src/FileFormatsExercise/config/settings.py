import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''

BUCKET_NAME = 'iva-data-warehouse-10'

FILE_PATH_CSV = 'FileFormats/sales_data.csv'
FILE_PATH_PARQUET = 'FileFormats/sales_data.parquet'

USER = 'postgres'
PASSWORD = 'postgres'
DATABASE = 'sales_data'
HOST = 'localhost'
PORT = 5432
