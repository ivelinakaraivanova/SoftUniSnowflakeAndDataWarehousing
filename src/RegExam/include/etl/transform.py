import pandas as pd
import numpy as np

from include.validations.validate_inputs import validate_input_products_schema, validate_input_sales_schema
from include.validations.validate_outputs import validate_output_products_schema, validate_output_sales_schema

from ..logger import setup_logger


logging = setup_logger("etl.transform")


    # for i, df in enumerate(dfs):
    #     if df is None or df.empty:
    #         print(f"DataFrame at index {i} is None or empty, skipping cleaning.")
    #         continue



def transform_sales_data(sales_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the sales DataFrame by cleaning and formatting.

    """

    logging.info("Cleaning sales data")

    sales_df = validate_input_sales_schema(sales_df)

    sales_df.columns = sales_df.columns.str.strip().str.lower().str.replace(' ', '_')
    sales_df = sales_df.rename(columns={'qty': 'quantity', "time_stamp": "timestamp"})
    sales_df = sales_df.dropna()
    sales_df = sales_df[(sales_df["price"] > 0) & (sales_df["quantity"] > 0)]
    sales_df["region"] = sales_df["region"].str.lower()
    sales_df["timestamp"] = pd.to_datetime(sales_df["timestamp"], format="mixed", errors="coerce")
    sales_df = sales_df.drop_duplicates().reset_index(drop=True)
    
    sales_df = validate_output_sales_schema(sales_df)

    logging.info("Sales data cleaned successfully")

    return sales_df
    

def transform_products_data(products_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the products DataFrame by cleaning and formatting.

    """

    logging.info("Cleaning products data")

    products_df = validate_input_products_schema(products_df)

    products_df.columns = products_df.columns.str.strip().str.lower().str.replace(' ', '_')    
    products_df = products_df.dropna()
    products_df["launch_date"] = pd.to_datetime(products_df["launch_date"], format="mixed", errors="coerce")
    products_df = products_df.drop_duplicates()
    
    products_df = validate_output_products_schema(products_df)

    logging.info("Products data cleaned successfully")

    return products_df
