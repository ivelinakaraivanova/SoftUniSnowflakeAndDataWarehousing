import pandas as pd
import pandera.pandas as pa

from pandera.pandas import DataFrameSchema, Column, Check

from ..logger import setup_logger


logging = setup_logger("validations.hourly_sales_schema")

hourly_sales_trend_schema = DataFrameSchema(
    {
        "region": Column(str, Check(lambda s: len(s) > 0)),
        "hour": Column(int, Check.in_range(min_value=0, max_value=23)),
        "hourly_total_sales": Column(float, Check.ge(0.0)),
    })  


def validate_output_hourly_sales_trend_schema(hourly_sales_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the hourly sales DataFrame against the predefined schema.

    """
    logging.info("Validating hourly sales data schema")
    
    return hourly_sales_trend_schema.validate(hourly_sales_df)

