import pandas as pd
import pandera.pandas as pa

from pandera.pandas import DataFrameSchema, Column, Check

from ..logger import setup_logger


logging = setup_logger("validations.revenue_concentration_schema")


revenue_concentration_schema = DataFrameSchema(
    {
        "region": Column(str, Check(lambda s: len(s) > 0)),
        "region_revenue": Column(float, Check.ge(0.0)),
        "revenue_share": Column(float, Check.in_range(min_value=0.0, max_value=1.0)),
        "cumulative_share": Column(float, Check.in_range(min_value=0.0, max_value=1.0)),
    })


def validate_revenue_concentration_schema(revenue_concentration_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the revenue concentration DataFrame against the predefined schema.

    """
    logging.info("Validating revenue concentration data schema")
    
    return revenue_concentration_schema.validate(revenue_concentration_df)

