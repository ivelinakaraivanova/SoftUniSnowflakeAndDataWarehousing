import pandas as pd
import pandera.pandas as pa

from pandera import Column, DataFrameSchema, Check
from pandera.errors import SchemaErrors

from ..logger import setup_logger
logging = setup_logger("validations.products_schema")


pre_products_schema = DataFrameSchema(
    {
        "product_id": Column(int, unique=True),
        "name": Column(str),
        "category": Column(str),
        "price": Column(float),
    })

post_products_schema = DataFrameSchema(
    {
        "product_id": Column(int, Check.greater_than(0), unique=True),
        "name": Column(str, Check.str_length(100)),
        "category": Column(str),
        "price": Column(float, Check.greater_than_or_equal_to(0.0)),
    })


def validate_pre_products_schema(products_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the products DataFrame against the predefined schema.

    """
    logging.info("Validating products data schema")

    try:
        pre_products_schema.validate(products_df)
        logging.info("Products data schema validation passed")
        return products_df
    except SchemaErrors as e:
        logging.warning(f"Pre-products data schema validation failed: {e.failure_cases}")
        return products_df
        

def validate_post_products_schema(products_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the transformed products DataFrame against the predefined schema.
    """
    logging.info("Validating transformed products data schema")

    return post_products_schema.validate(products_df)

