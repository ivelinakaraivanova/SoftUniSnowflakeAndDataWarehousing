import pandas as pd
import pandera.pandas as pa

from pandera.pandas import DataFrameSchema, Column, Check
from pandera.errors import SchemaErrors

from output_schemas import sales_output_schema, product_output_schema

from ..logger import setup_logger


logging = setup_logger("validations.output_schemas")


def validate_output_sales_schema(sales_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the output sales DataFrame against the predefined schema.
 
    """
    logging.info("Validating output sales data schema")
    
    return sales_output_schema.validate(sales_df)


def validate_output_products_schema(products_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the output products DataFrame against the predefined schema.
 
    """
    logging.info("Validating output products data schema")
    
    return product_output_schema.validate(products_df)

