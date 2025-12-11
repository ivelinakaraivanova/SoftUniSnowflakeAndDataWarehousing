from logger import logging
import pandas as pd
import pandera.pandas as pa

from pandera.pandas import DataFrameSchema, Column, Check
from pandera.errors import SchemaErrors

sales_input_schema = DataFrameSchema(
    {
        "sales_id": Column(int),
        "product_id": Column(int),
        "region": Column(str),
        "quantity": Column(int),
        "price": Column(float),
        "timestamp": Column(pa.DateTime),
        "total_sales": Column(float),
    })

sales_output_schema = DataFrameSchema(
    {
        "sales_id": Column(int),
        "product_id": Column(int),
        "region": Column(str, Check(lambda s: s.str.islower())),
        "quantity": Column(int, Check.gt(0)),
        "price": Column(float, Check.gt(0.0)),
        "timestamp": Column(pa.DateTime),
        "total_sales": Column(float, Check.ge(0.0)),
    })


def validate_input_sales_schema(sales_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the input sales DataFrame against the predefined schema.
 
    """
    logging.info("Validating sales data schema")
    
    try:
        sales_input_schema.validate(sales_df)
        logging.info("Sales data schema validation passed")
        return sales_df
    except SchemaErrors as e:
        logging.warning(f"Pre-sales data schema validation failed: {e.failure_cases}")
        return sales_df

def validate_output_sales_schema(sales_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the output sales DataFrame against the predefined schema.
 
    """
    logging.info("Validating output sales data schema")
    
    return sales_output_schema.validate(sales_df)
