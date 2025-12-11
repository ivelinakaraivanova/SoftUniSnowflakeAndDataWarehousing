import pandas as pd

from airflow.sdk import dag, task, TaskGroup
from airflow.utils import yaml
from pendulum import datetime

from include.etl.extract_data import extract_data_from_s3
from include.etl.transform import clean_sales_data, clean_customers_data, clean_products_data, compute_monthly_aggregates, forecast_sales, merge_data, segment_customers, detect_sales_anomalies
from include.etl.load_data import load_data_to_snowflake


with open("include/config.yaml", "r") as file:
    config = yaml.safe_load(file)


@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["exercise"],
)

def etl_pipeline_dag():
    @task()
    def extract_data(bucket: str, folder: str, aws_conn_id: str)-> dict:
        return extract_data_from_s3(bucket, folder, aws_conn_id)
        
    @task()
    def get_sales_file(files: dict):
        for key, df in files.items():
            if "sales" in key.lower():
                return df.to_json(orient="split")   # Here it's better to save data into parquet format - df.to_parquet(path to save - to cloud storage or base)
        raise ValueError("No sales file found")

    @task()
    def get_customers_file(files: dict):
        for key, df in files.items():
            if "customer" in key.lower():
                return df.to_json(orient="split")
        raise ValueError("No customer file found")

    @task()
    def get_products_file(files: dict):
        for key, df in files.items():
            if "product" in key.lower():
                return df.to_json(orient="split")
        raise ValueError("No product file found")
    
    @task()
    def transform_sales_data(sales_file: str) -> str:

        sales_df = pd.read_json(sales_file, orient="split")   # Here read from S3 with storage options
        sales_df = clean_sales_data(sales_df)
        return sales_df.to_json(orient="split", date_format='iso')
    
    @task()
    def transform_customers_data(customers_file: str) -> str:

        customers_df = pd.read_json(customers_file, orient="split")   
        customers_df = clean_customers_data(customers_df)
        return customers_df.to_json(orient="split", date_format='iso')

    @task()
    def transform_products_data(products_file: str) -> str:

        products_df = pd.read_json(products_file, orient="split")   
        products_df = clean_products_data(products_df)
        return products_df.to_json(orient="split")
    
    @task()
    def merged_data_task(transformed_sales: str, transformed_customers: str, transformed_products: str) -> str:
        sales_df = pd.read_json(transformed_sales, orient="split")
        customers_df = pd.read_json(transformed_customers, orient="split")
        products_df = pd.read_json(transformed_products, orient="split")

        merged_df = merge_data(sales_df, customers_df, products_df)
        return merged_df.to_json(orient="split")
    
    @task()
    def aggregated_data_task(merged_data: str) -> str:
        merged_df = pd.read_json(merged_data, orient="split")
        aggregated_df = compute_monthly_aggregates(merged_df)
        return aggregated_df.to_json(orient="split", date_format='iso')
    
    @task()
    def segmented_customers_task(sales_data: str, customers_data: str) -> str:
        sales_df = pd.read_json(sales_data, orient="split")
        customers_df = pd.read_json(customers_data, orient="split")
        segmented_df = segment_customers(sales_df, customers_df) 
        return segmented_df.to_json(orient="split", date_format='iso')
    
    @task()
    def anomalies_sales_task(sales_data: str) -> str:
        sales_df = pd.read_json(sales_data, orient="split")
        sales_df = detect_sales_anomalies(sales_df)
        return sales_df.to_json(orient="split", date_format='iso')
    
    @task()
    def forecasted_sales_task(sales_data: str) -> str:
        sales_df = pd.read_json(sales_data, orient="split")
        sales_df = forecast_sales(sales_df)
        return sales_df.to_json(orient="split", date_format='iso')
    
    @task()
    def load_to_snowflake_task(final_json: str, database: str, schema: str, table: str, snowflake_conn_id: str):
        final_df = pd.read_json(final_json, orient="split")
        load_data_to_snowflake(
            df=final_df,
            database=database,
            schema=schema,
            table=table,
            snowflake_conn_id=snowflake_conn_id
        )


    with TaskGroup("extraction") as extraction:
        files = extract_data(
            bucket = config["s3"]["bucket"],
            folder = config["s3"]["folder"],
            aws_conn_id = config["aws_conn_id"]
        )

        sales_file = get_sales_file(files)
        customers_file = get_customers_file(files)
        products_file = get_products_file(files)


    with TaskGroup("transformation") as transformation:
        transformed_sales = transform_sales_data(sales_file=sales_file)
        transformed_customers = transform_customers_data(customers_file=customers_file)
        transformed_products = transform_products_data(products_file=products_file)

        merged_output = merged_data_task(
            transformed_sales=transformed_sales,
            transformed_customers=transformed_customers,
            transformed_products=transformed_products
        )


    with TaskGroup("analysis") as analysis:
        aggregated_output = aggregated_data_task(merged_output)

        segmented_customers_output = segmented_customers_task(
            sales_data=transformed_sales,
            customers_data=transformed_customers
        )

        anomalies_sales_output = anomalies_sales_task(sales_data=transformed_sales)

        forecast_sales_output = forecasted_sales_task(sales_data=transformed_sales)


    with TaskGroup("loading") as loading:
        load_to_snowflake_task.override(task_id="load_cleaned_sales")(
            final_json=transformed_sales,
            database=config["snowflake"]["database"],
            schema=config["snowflake"]["targets"]["sales"]["schema"],
            table=config["snowflake"]["targets"]["sales"]["table"],
            snowflake_conn_id=config["snowflake"]["conn_id"]
        )

        load_to_snowflake_task.override(task_id="load_cleaned_customers")(
            final_json=transformed_customers,
            database=config["snowflake"]["database"],
            schema=config["snowflake"]["targets"]["customers"]["schema"],
            table=config["snowflake"]["targets"]["customers"]["table"],
            snowflake_conn_id=config["snowflake"]["conn_id"]
        )

        load_to_snowflake_task.override(task_id="load_cleaned_products")(
            final_json=transformed_products,
            database=config["snowflake"]["database"],
            schema=config["snowflake"]["targets"]["products"]["schema"],
            table=config["snowflake"]["targets"]["products"]["table"],
            snowflake_conn_id=config["snowflake"]["conn_id"]
        )
        
        load_to_snowflake_task.override(task_id="load_monthly_sales")(
            final_json=aggregated_output,
            database=config["snowflake"]["database"],
            schema=config["snowflake"]["targets"]["monthly_sales"]["schema"],
            table=config["snowflake"]["targets"]["monthly_sales"]["table"],
            snowflake_conn_id=config["snowflake"]["conn_id"]
        )

        load_to_snowflake_task.override(task_id="load_segmented_customers")(
            final_json=segmented_customers_output,
            database=config["snowflake"]["database"],
            schema=config["snowflake"]["targets"]["segmented_customers"]["schema"],
            table=config["snowflake"]["targets"]["segmented_customers"]["table"],
            snowflake_conn_id=config["snowflake"]["conn_id"]
        )

        load_to_snowflake_task.override(task_id="load_detect_sales_anomalies")(
            final_json=anomalies_sales_output,
            database=config["snowflake"]["database"],
            schema=config["snowflake"]["targets"]["detect_sales_anomalies"]["schema"],
            table=config["snowflake"]["targets"]["detect_sales_anomalies"]["table"],
            snowflake_conn_id=config["snowflake"]["conn_id"]
        )

        load_to_snowflake_task.override(task_id="load_forecast_sales")(
            final_json=forecast_sales_output,
            database=config["snowflake"]["database"],
            schema=config["snowflake"]["targets"]["forecast_sales"]["schema"],
            table=config["snowflake"]["targets"]["forecast_sales"]["table"],
            snowflake_conn_id=config["snowflake"]["conn_id"]
        )

etl_pipeline_dag()
