import logging
import pandas as pd
import pandera.pandas as pa

from pandera.pandas import DataFrameSchema, Column, Check
from pandera.errors import SchemaError

sales_schema = DataFrameSchema(
    {
        "order_id": Column(int, checks=Check(lambda s: s > 0, error="Order id must be greater than 0")),
        "customer_id": Column(int, checks=Check(lambda s: s > 0, error="Customer id must be greater than 0")),
        "amount": Column(float, checks=Check(lambda s: s > 0.0, error="Price must be greater than 0")),
        "quantity": Column(int, checks=Check(lambda s: s > 0, error="Quantity must be greater than 0")),
        "order_date": Column(pa.DateTime, coerce=True),
    },
    strict=True
)


def validate_sales_data(df: pd.DataFrame, lazy: bool = True) -> pd.DataFrame:
    """
    Validates the sales data DataFrame against the predefined schema.

    """
    logging.info("Starting validation of sales data")

    try:
        validated_df = sales_schema.validate(df, lazy=lazy)
    except SchemaError as e:
        logging.error(f"Sales data validation error: {e.failure_cases}")
        raise

    logging.info("Sales data validation successful")
    
    return validated_df
