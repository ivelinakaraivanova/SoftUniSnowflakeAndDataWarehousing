import pandas as pd
import pandera.pandas as pa

from pandera.pandas import DataFrameSchema, Column, Check
from pandera.errors import SchemaErrors

from input_schemas import sales_input_schema, product_input_schema

from ..logger import setup_logger


logging = setup_logger("validations.input_sales_schema")


def validate_input_sales_schema(sales_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the input sales DataFrame against the predefined schema.
 
    """
    logging.info("Validating input sales data schema")
    
    try:
        sales_input_schema.validate(sales_df)
        logging.info("Input sales data schema validation passed")
        return sales_df
    except SchemaErrors as e:
        logging.warning(f"Input sales data schema validation failed: {e.failure_cases}")
        return sales_df
    
logging = setup_logger("validations.input_products_schema")


def validate_input_products_schema(products_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the input products DataFrame against the predefined schema.
 
    """
    logging.info("Validating input products data schema")
    
    try:
        product_input_schema.validate(products_df)
        logging.info("Input products data schema validation passed")
        return products_df
    except SchemaErrors as e:
        logging.warning(f"Input products data schema validation failed: {e.failure_cases}")
        return products_df
