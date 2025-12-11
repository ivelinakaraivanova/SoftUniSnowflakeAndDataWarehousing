import sys
from pathlib import Path
import pandera.pandas as pa

sys.path.insert(0, str(Path(__file__).parent.parent))


from extract.extract_data import extract_csv, extract_json, extract_parquet, extract_db_data
from config.settings import USER, PASSWORD, HOST, PORT, DATABASE
from config.settings import BUCKET_NAME, FOLDER_NAME, CSV_FILE_NAME, JSON_FILE_NAME, PARQUET_FILE_NAME
from transform.transform import transform_data
from load.load_to_postgres import load_to_postgresql
from validations.pandera_validation import validate_sales_data


if __name__ == "__main__":

    # extract

    sales_df_csv = extract_csv(bucket_name=BUCKET_NAME, folder_name=FOLDER_NAME, file_name=CSV_FILE_NAME)
    # sales_df_json = extract_json(bucket_name=BUCKET_NAME, folder_name=FOLDER_NAME, file_name=JSON_FILE_NAME)
    sales_df_parquet = extract_parquet(bucket_name=BUCKET_NAME, folder_name=FOLDER_NAME, file_name=PARQUET_FILE_NAME)
    # sales_db_data = extract_db_data(db_url=f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
    
    # transform

    transform_csv_df = transform_data(sales_df_csv)
    transform_parquet_df = transform_data(sales_df_parquet)

    # validate

    try:
        valid_df_csv = validate_sales_data(transform_csv_df)  
        print("CSV data validation passed.")  
    except pa.errors.SchemaError as e:
        raise ValueError(f"Data validation error: {e}")
    
    try:
        valid_df_parquet = validate_sales_data(transform_parquet_df)  
        print("Parquet data validation passed.")  
    except pa.errors.SchemaError as e:
        raise ValueError(f"Data validation error: {e}")

    # load

    load_to_postgresql(valid_df_csv, table_name="sales_data_csv")
    load_to_postgresql(valid_df_parquet, table_name="sales_data_parquet")
