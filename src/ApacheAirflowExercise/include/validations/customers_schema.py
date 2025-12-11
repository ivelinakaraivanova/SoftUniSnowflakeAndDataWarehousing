import pandas as pd
import pandera.pandas as pa

from pandera import Column, DataFrameSchema, Check
from pandera.errors import SchemaErrors

from ..logger import setup_logger
logging = setup_logger("validations.customers_schema")


pre_customers_schema = DataFrameSchema(
    {
        "customer_id": Column(int, unique=True),
        "name": Column(str),
        "email": Column(str),
        "signup_date": Column(pa.DateTime),
    })

post_customers_schema = DataFrameSchema(
    {
        "customer_id": Column(int, Check.greater_than(0), unique=True),
        "name": Column(str, Check.str_length(0,100)),
        "email": Column(str, Check.str_matches(r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')),
        "signup_date": Column(pa.DateTime),
    })


def validate_pre_customers_schema(customers_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the customers DataFrame against the predefined schema.

    """
    logging.info("Validating customers data schema")

    try:
        pre_customers_schema.validate(customers_df)
        logging.info("Customers data schema validation passed")
        return customers_df
    except SchemaErrors as e:
        logging.warning(f"Pre-customers data schema validation failed: {e.failure_cases}")
        return customers_df


def validate_post_customers_schema(customers_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the transformed customers DataFrame against the predefined schema.

    """
    logging.info("Validating transformed customers data schema")

    return post_customers_schema.validate(customers_df)


'''
    try:
        post_customers_schema.validate(customers_df)
        logging.info("Transformed customers data schema validation passed")
        return customers_df
    except SchemaErrors as e:
        logging.error(f"Post-customers data schema validation failed: {e.failure_cases}")
        raise
'''
