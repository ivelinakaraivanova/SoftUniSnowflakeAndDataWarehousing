import logging
import pandas as pd

from pandera.pandas import DataFrameSchema, Column, Check
from pandera.errors import SchemaError

orders_schema = DataFrameSchema(
    {
        "order_id": Column(int, checks=Check(lambda s: s > 0, error="Order id must be greater than 0")),
        "customer_id": Column(int, checks=Check(lambda s: s > 0, error="Customer id must be greater than 0")),
        "product": Column(str, checks=Check.str_length(1, 100, error="Invalid product length")),
        "quantity": Column(int, checks=Check(lambda s: s > 0, error="Quantity must be greater than 0")),
        "price": Column(float, checks=Check(lambda s: s > 0.0, error="Price must be greater than 0")),
    },
    strict=True
)


def validate_orders_data(df: pd.DataFrame, lazy: bool = True) -> pd.DataFrame:
    """
    Validates the orders data DataFrame against the predefined schema.

    """
    logging.info("Starting validation of orders data")

    try:
        validated_df = orders_schema.validate(df, lazy=lazy)
    except SchemaError as e:
        logging.error(f"Orders data validation error: {e.failure_cases}")
        raise

    logging.info("Orders data validation successful")
    
    return validated_df
