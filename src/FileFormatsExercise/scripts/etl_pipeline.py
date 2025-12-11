import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import BUCKET_NAME, FILE_PATH_CSV, FILE_PATH_PARQUET, DATABASE, USER, PASSWORD, HOST, PORT
from extract.aws_s3_extract import extract_csv_from_s3, extract_parquet_from_s3
from extract.local_extract import extract_customers_from_json, extract_and_flatten_orders_from_json
from extract.api_extract import extract_weather_from_sinoptik
from extract.postgres_extract import extract_from_database

from load.load_local_data import load_local_data_file
from load.load_data_to_s3 import load_data_to_s3

from validations.customers_validations import validate_customers_data
from validations.orders_validations import validate_orders_data
from validations.sales_validations import validate_sales_data
from validations.weather_validations import validate_weather_data


if __name__ == "__main__":

    # Local extraction
    customers_df = extract_customers_from_json("E:/Iva/SoftUni/Introduction to Snowflake and Data Warehousing/Materials/Projects/FileFormatsExercise/data/customers.json")
    orders_df = extract_and_flatten_orders_from_json("E:/Iva/SoftUni/Introduction to Snowflake and Data Warehousing/Materials/Projects/FileFormatsExercise/data/orders.json")

    orders_df["order_id"] = orders_df["order_id"].astype(int)
    orders_df["customer_id"] = orders_df["customer_id"].astype(int)

    merged_df = pd.merge(orders_df, customers_df, on="customer_id", how="left")

    # S3 extraction
    sales_df_csv = extract_csv_from_s3(bucket_name=BUCKET_NAME, file_key=FILE_PATH_CSV)
    sales_df_parquet = extract_parquet_from_s3(bucket_name=BUCKET_NAME, file_key=FILE_PATH_PARQUET)

    # Database extraction
    sql_query = "SELECT * FROM sales_data;"
    db_params = {
        "database": DATABASE,
        "user": USER,
        "password": PASSWORD,
        "host": HOST,
        "port": PORT
    }

    sales_df_db = extract_from_database(sql_query=sql_query, db_params=db_params)

    # API extraction
    weather_df = extract_weather_from_sinoptik("sofia")

    # Data Validation
    validated_customers_df = validate_customers_data(customers_df)
    validated_orders_df = validate_orders_data(orders_df)
    validated_sales_df = validate_sales_data(sales_df_csv)
    validated_weather_df = validate_weather_data(weather_df)

    # Data Loading
    load_local_data_file(sales_df_db, file_name="sales_data_db", file_format="json")
    load_local_data_file(validated_customers_df, file_name="customers_data", file_format="json")
    load_local_data_file(validated_orders_df, file_name="orders_data", file_format="json")

    load_local_data_file(validated_sales_df, file_name="sales_data", file_format="csv")
    load_local_data_file(validated_weather_df, file_name="weather_data", file_format="csv")
    load_data_to_s3(validated_sales_df, bucket_name=BUCKET_NAME, file_key="test_load_data/sales_data.csv", file_format="csv")
    load_data_to_s3(validated_orders_df, bucket_name=BUCKET_NAME, file_key="test_load_data/orders_data.csv", file_format="csv")
    