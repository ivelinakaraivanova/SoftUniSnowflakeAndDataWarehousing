import pandas as pd
import pandera.pandas as pa

from pandera import Column, DataFrameSchema, Check
from pandera.errors import SchemaErrors

from ..logger import setup_logger
logging = setup_logger("validations.aggregates_schema")


pre_aggregates_schema = DataFrameSchema(
    {
        "order_date": Column(pa.DateTime),
        "unique_customers": Column(float, Check.greater_than(0)),
        "total_sales": Column(float, Check.greater_than_or_equal_to(0.0)),
    })

post_aggregates_schema = DataFrameSchema(
    {
        "order_date": Column(pa.DateTime),
        "unique_customers": Column(float, Check.greater_than(0)),
        "total_sales": Column(float, Check.greater_than_or_equal_to(0.0)),
    })


def validate_pre_aggregates_schema(aggregates_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the aggregates DataFrame against the predefined schema.

    """
    logging.info("Validating aggregates data schema")

    try:
        pre_aggregates_schema.validate(aggregates_df)
        logging.info("Aggregates data schema validation passed")
        return aggregates_df
    except SchemaErrors as e:
        logging.warning(f"Pre-aggregates data schema validation failed: {e.failure_cases}")
        return aggregates_df
        

def validate_post_aggregates_schema(aggregates_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the transformed aggregates DataFrame against the predefined schema.
    """
    logging.info("Validating transformed aggregates data schema")

    return post_aggregates_schema.validate(aggregates_df)