from logger import logging
import pandas as pd
import pandera.pandas as pa

from pandera.pandas import DataFrameSchema, Column, Check
from pandera.errors import SchemaErrors

product_input_schema = DataFrameSchema(
    {
        "product_id": Column(int),
        "category": Column(str),
        "brand": Column(str),
        "rating": Column(float)
    })

product_output_schema = DataFrameSchema(
    {
        "product_id": Column(int),
        "category": Column(str, Check(lambda s: s.str.islower())),
        "brand": Column(str, Check(lambda s: s.str.isupper())),
        "rating": Column(float)
    })


def validate_input_products_schema(products_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the input products DataFrame against the predefined schema.
 
    """
    logging.info("Validating products data schema")
    
    try:
        product_input_schema.validate(products_df)
        logging.info("Products data schema validation passed")
        return products_df
    except SchemaErrors as e:
        logging.warning(f"Pre-products data schema validation failed: {e.failure_cases}")
        return products_df

def validate_output_products_schema(products_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the output products DataFrame against the predefined schema.
 
    """
    logging.info("Validating output products data schema")
    
    return product_output_schema.validate(products_df)
