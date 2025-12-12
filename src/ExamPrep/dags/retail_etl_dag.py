import pandas as pd

from airflow.exceptions import AirflowException
from airflow.sdk import dag, task, task_group
from airflow.utils import yaml
from pendulum import datetime

from include.etl.extract_data import extract_data_from_s3
from include.etl.load_data import load_df_to_s3_csv
from include.etl.transform import enrich_merged_data, hourly_sales_trend, merge_sales_and_products, product_sales_ranking_with_brand, product_sales_ranking_with_brand, revenue_concentration, seasonal_sales_pattern, transform_products_data, transform_sales_data
from include.s3_utils import get_storage_options


with open("include/config.yaml", "r") as file:
    config = yaml.safe_load(file)


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["retail_etl_dag"],
)

def retail_etl_dag():
    s3_hook, storage_options = get_storage_options(config["aws_conn_id"])

    @task_group(group_id="extract_data")
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
            bucket=config["s3"]["bucket"], folder=config["s3"]["folder"], aws_conn_id=config["aws_conn_id"])
        json_paths = extract_json_from_s3(
            bucket=config["s3"]["bucket"], folder=config["s3"]["folder"], aws_conn_id=config["aws_conn_id"])
        

        sales_path = get_sales_path(csv_paths)
        products_path = get_products_path(json_paths)

        return {
            "sales_path": sales_path,
            "products_path": products_path
        }
    

    @task_group(group_id="transform_group")
    def transform_group(sales_path: str, products_path: str):
        
        @task()
        def transform_sales(sales_path: str) -> pd.DataFrame:
            sales_df = pd.read_csv(sales_path, storage_options=storage_options)
            sales_df = transform_sales_data(sales_df)

            output_path = f"s3://{config['s3']['bucket']}/{config['s3']['output_folder']}/cleaned_sales.csv"
            load_df_to_s3_csv(sales_df, output_path, config["aws_conn_id"])
            
            return output_path
        
        @task()
        def transform_products(products_path: str) -> pd.DataFrame:
            products_df = pd.read_json(products_path, storage_options=storage_options)
            products_df = transform_products_data(products_df)

            output_path = f"s3://{config['s3']['bucket']}/{config['s3']['output_folder']}/cleaned_products.json"
            load_df_to_s3_csv(products_df, output_path, config["aws_conn_id"])
            
            return output_path

        @task()
        def merge_data(sales_path: str, products_path: str) -> None:
            sales_df = pd.read_csv(sales_path, storage_options=storage_options)
            products_df = pd.read_json(products_path, storage_options=storage_options)

            merged_df = merge_sales_and_products(sales_df, products_df)

            output_path = f"s3://{config['s3']['bucket']}/{config['s3']['output_folder']}/merged_data.csv"
            load_df_to_s3_csv(merged_df, output_path, config["aws_conn_id"])
            
            return output_path
        
        @task()
        def enrich_data(merged_path: str) -> None:
            merged_df = pd.read_csv(merged_path, storage_options=storage_options)
          
            enriched_df = enrich_merged_data(merged_df)

            output_path = f"s3://{config['s3']['bucket']}/{config['s3']['output_folder']}/enriched_data.csv"
            load_df_to_s3_csv(enriched_df, output_path, config["aws_conn_id"])
            
            return output_path

        cleaned_sales = transform_sales(sales_path)
        cleaned_products = transform_products(products_path)
        merged_data = merge_data(cleaned_sales, cleaned_products)
        enriched_data = enrich_data(merged_data)

        return {
            "cleaned_sales": cleaned_sales,
            "cleaned_products": cleaned_products,
            "merged_data": merged_data,
            "enriched_data": enriched_data
        }        


    @task_group(group_id="analytics_group")
    def analytics_group(enriched_path: str):

        @task()
        def run_hourly_sales_trend(enriched_path: str) -> None:
            enriched_df = pd.read_csv(enriched_path, storage_options=storage_options)
            result = hourly_sales_trend(enriched_df)

            output_path = f"s3://{config['s3']['bucket']}/{config['s3']['analytics_folder']}/hourly_sales_trend.csv"
            load_df_to_s3_csv(result, output_path, config["aws_conn_id"])
            return output_path
        
        @task()
        def run_product_sales_ranking(enriched_path: str) -> None:
            enriched_df = pd.read_csv(enriched_path, storage_options=storage_options)
            result = product_sales_ranking_with_brand(enriched_df)

            output_path = f"s3://{config['s3']['bucket']}/{config['s3']['analytics_folder']}/product_sales_ranking.csv"
            load_df_to_s3_csv(result, output_path, config["aws_conn_id"])
            return output_path
        
        @task()
        def run_seasonal_sales_pattern(enriched_path: str) -> None:
            enriched_df = pd.read_csv(enriched_path, storage_options=storage_options)
            result = seasonal_sales_pattern(enriched_df)

            output_path = f"s3://{config['s3']['bucket']}/{config['s3']['analytics_folder']}/seasonal_sales_pattern.csv"
            load_df_to_s3_csv(result, output_path, config["aws_conn_id"])
            return output_path
        
        @task()
        def run_revenue_concentration(enriched_path: str) -> None:
            enriched_df = pd.read_csv(enriched_path, storage_options=storage_options)
            result = revenue_concentration(enriched_df)

            output_path = f"s3://{config['s3']['bucket']}/{config['s3']['analytics_folder']}/revenue_concentration.csv"
            load_df_to_s3_csv(result, output_path, config["aws_conn_id"])
            return output_path
        
        hourly_trend = run_hourly_sales_trend(enriched_path)
        product_ranking = run_product_sales_ranking(enriched_path)
        seasonal_pattern = run_seasonal_sales_pattern(enriched_path)
        revenue_concentration = run_revenue_concentration(enriched_path)

        return {
            "hourly_trend": hourly_trend,
            "product_ranking": product_ranking,
            "seasonal_pattern": seasonal_pattern,
            "revenue_concentration": revenue_concentration
        }
    

    @task_group(group_id="load_analytics_group")
    def load_analytics_group(hourly_trend, product_ranking, seasonal_pattern, revenue_concentration):
        
        @task()
        def copy_csv(input_path: str, output_file: str) -> None:
            df = pd.read_csv(input_path, storage_options=storage_options)

            bucket = config["s3"]["bucket"]
            folder = config["s3"]["analytics_folder"]
            
            output_path = f"s3://{bucket}/{folder}/{output_file}"
            load_df_to_s3_csv(df, output_path, config["aws_conn_id"])
            return output_path
        
        copy_csv.override(task_id="load_hourly_trend")(hourly_trend, "hourly_sales_trend.csv")
        copy_csv.override(task_id="load_product_ranking")(product_ranking, "product_sales_ranking.csv")
        copy_csv.override(task_id="load_seasonal_pattern")(seasonal_pattern, "seasonal_sales_pattern.csv")
        copy_csv.override(task_id="load_revenue_concentration")(revenue_concentration, "revenue_concentration.csv")

         
    extract_output = extract_group()

    sales_path = extract_output["sales_path"]
    products_path = extract_output["products_path"]

    transform_output = transform_group(sales_path, products_path)
    
    enriched_path = transform_output["enriched_data"]
    
    analytics_output = analytics_group(enriched_path)
    
    load_analytics_group(
        hourly_trend=analytics_output["hourly_trend"],
        product_ranking=analytics_output["product_ranking"],
        seasonal_pattern=analytics_output["seasonal_pattern"],
        revenue_concentration=analytics_output["revenue_concentration"]
    )

retail_etl_dag()