import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from extract.extract_s3 import s3_extract
from config.settings import BUCKET_NAME, FOLDER_NAME
from transform.transform import categorize_products, clean_data, compute_derived_columns, merge_data, remove_duplicates, segment_deliveries
from load.load_to_postgres import load_raw_to_postgres, load_transformed_to_postgres


def run_etl_pipeline():
    sales_df, product_df, customer_df, shipping_df = s3_extract(
        bucket_name=BUCKET_NAME, folder_name=FOLDER_NAME)
    
    cleaned_dfs = clean_data([sales_df, product_df, customer_df, shipping_df], 
                             old_column_name="diskount", new_column_name="discount")

    deduped_dfs = remove_duplicates(dfs=cleaned_dfs)

    merge_columns = [
        ("product_id", "product_id"),   # sales_df x product_df
        ("customer_id", "customer_id"), # result x customer_df
        ("order_id", "order_id")        # result x shipping_df
        ]
    
    merged_df = merge_data(dfs=deduped_dfs, merge_columns=merge_columns, how='inner')
    merged_df = segment_deliveries(merged_df=merged_df)
    merged_df = compute_derived_columns(merged_df=merged_df)
    merged_df = categorize_products(merged_df=merged_df)

    load_raw_to_postgres(df=merged_df, table_name="raw_sales_data", if_exists="replace")
    load_transformed_to_postgres(df=merged_df, table_name="transformed_sales_data")


if __name__ == "__main__":
    run_etl_pipeline()