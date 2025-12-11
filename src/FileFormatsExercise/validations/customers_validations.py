import logging
import pandas as pd

from pandera.pandas import DataFrameSchema, Column, Check
from pandera.errors import SchemaError

customers_schema = DataFrameSchema(
    {
        "name": Column(str, checks=Check(lambda s: s.str.match(r"^[A-Za-z\s'-]+$"), error="Invalid name format")),
        "email": Column(str, checks=Check(lambda s: s.str.match(r"^[\w\.-]+@[\w\.-]+\.\w{2,4}$"), error="Invalid email format")),
        "customer_id": Column(int, checks=[
                                    Check(lambda s: s >= 0, error="customer_id must be greater than 0"),
                                    Check(lambda s: s.is_unique, error="customer_id must be unique")
                                    ]
                                ),
    },
    strict=True
)


def validate_customers_data(df: pd.DataFrame, lazy: bool = True) -> pd.DataFrame:
    """
    Validates the customers data DataFrame against the predefined schema.

    """
    logging.info("Starting validation of customers data")

    try:
        validated_df = customers_schema.validate(df, lazy=lazy)
    except SchemaError as e:
        logging.error(f"Customers data validation error: {e.failure_cases}")
        raise

    logging.info("Customers data validation successful")
    
    return validated_df
