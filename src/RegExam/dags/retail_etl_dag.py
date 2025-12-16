import pandas as pd

from airflow.sdk import dag, task, task_group
from airflow.sdk.exceptions import AirflowException
from airflow.utils import yaml
from pendulum import datetime

from pathlib import Path

from include.etl.extract_s3 import extract_data_from_s3
from include.etl.load_s3_csv import load_df_to_s3_csv
from include.etl.transform import transform_products_data, transform_sales_data
from include.s3_utils import get_storage_options


# Get the absolute path to config file
config_path = Path(__file__).parent.parent / "include" / "config.yaml"
with open(config_path, "r") as file:
    config = yaml.safe_load(file)


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["retail_etl_dag"],
)
def retail_etl_dag():
    s3_hook, storage_options = get_storage_options(config["aws_conn_id"])
    
    @task_group(group_id="extract_s3")
    def extract_group():

        @task
        def extract_csv_from_s3(bucket: str, folder: str, aws_conn_id: str) -> list:
            return extract_data_from_s3(
                bucket=bucket, folder=folder, aws_conn_id=aws_conn_id, file_type="csv"
            )

        @task
        def extract_json_from_s3(bucket: str, folder: str, aws_conn_id: str) -> list:
            return extract_data_from_s3(
                bucket=bucket, folder=folder, aws_conn_id=aws_conn_id, file_type="json"
            )

        @task
        def get_sales_path(paths: list) -> str:
            for path in paths:
                if "sales" in path.lower():
                    return path
            raise AirflowException("No sales file found in the provided paths.")
        
        @task
        def get_products_path(paths: list) -> str:
            for path in paths:
                if "product" in path.lower():
                    return path
            raise AirflowException("No products file found in the provided paths.")
        
        csv_paths = extract_csv_from_s3(
            bucket=config["s3"]["bucket"], folder=config["s3"]["input_folder"], aws_conn_id=config["aws_conn_id"])
        json_paths = extract_json_from_s3(
            bucket=config["s3"]["bucket"], folder=config["s3"]["input_folder"], aws_conn_id=config["aws_conn_id"])
        

        sales_path = get_sales_path(csv_paths)
        products_path = get_products_path(json_paths)

        return {
            "sales_path": sales_path,
            "products_path": products_path
        }


    @task_group(group_id="transform_data")
    def transform_group(sales_path: str, products_path: str):
        
        @task()
        def transform_sales(sales_path: str):
            sales_df = pd.read_csv(sales_path, storage_options=storage_options)
            sales_df = transform_sales_data(sales_df)

            output_path = f"s3://{config['s3']['bucket']}/{config['s3']['output_folder']}/cleaned_sales.csv"
            load_df_to_s3_csv(sales_df, output_path, config["aws_conn_id"])
            
            return output_path
        
        @task()
        def transform_products(products_path: str):
            products_df = pd.read_json(products_path, storage_options=storage_options)
            products_df = transform_products_data(products_df)

            output_path = f"s3://{config['s3']['bucket']}/{config['s3']['output_folder']}/cleaned_products.csv"
            load_df_to_s3_csv(products_df, output_path, config["aws_conn_id"])
            
            return output_path

        cleaned_sales = transform_sales(sales_path)
        cleaned_products = transform_products(products_path)
        
        return {
            "cleaned_sales": cleaned_sales,
            "cleaned_products": cleaned_products,
        }        

        

    extract_output = extract_group()
    sales_path = extract_output["sales_path"]
    products_path = extract_output["products_path"]

    transform_group(sales_path, products_path)

retail_etl_dag()
