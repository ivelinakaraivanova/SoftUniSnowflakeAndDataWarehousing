import pandas as pd
import pandera.pandas as pa

from pandera import Column, DataFrameSchema, Check
from pandera.errors import SchemaErrors

from ..logger import setup_logger
logging = setup_logger("validations.sales_schema")


pre_sales_schema = DataFrameSchema(
    {
        "order_id": Column(int, unique=True),
        "customer_id": Column(int),
        "product_id": Column(int),
        "order_date": Column(pa.DateTime),
        "amount": Column(float),
        "quantity": Column(int),
        "discount": Column(float),
        "profit": Column(float),
        "total_revenue": Column(float),
    })

post_sales_schema = DataFrameSchema(
    {
        "order_id": Column(int, unique=True),
        "customer_id": Column(int, Check.greater_than(0)),
        "product_id": Column(int, Check.greater_than(0)),
        "order_date": Column(pa.DateTime),
        "amount": Column(float, Check.greater_than_or_equal_to(0.0)),
        "quantity": Column(int, Check.greater_than_or_equal_to(0)),
        "discount": Column(float, Check.between(1, 100)),
        "profit": Column(float),
        "total_revenue": Column(float, Check.greater_than_or_equal_to(0.0)),
    })


def validate_pre_sales_schema(sales_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the sales DataFrame against the predefined schema.

    """
    logging.info("Validating sales data schema")

    try:
        pre_sales_schema.validate(sales_df)
        logging.info("Sales data schema validation passed")
        return sales_df
    except SchemaErrors as e:
        logging.warning(f"Pre-sales data schema validation failed: {e.failure_cases}")
        return sales_df
        

def validate_post_sales_schema(sales_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the transformed sales DataFrame against the predefined schema.
    """
    logging.info("Validating transformed sales data schema")

    return post_sales_schema.validate(sales_df)
