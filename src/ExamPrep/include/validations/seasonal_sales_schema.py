import pandas as pd
import pandera.pandas as pa

from pandera.pandas import DataFrameSchema, Column, Check

from ..logger import setup_logger


logging = setup_logger("validations.seasonal_sales_schema")


seasonal_sales_pattern_schema = DataFrameSchema(
    {
        "quarter": Column(str),
        "category": Column(str),
        "total_sales": Column(float, Check.ge(0.0)),
    })


def validate_output_seasonal_sales_pattern_schema(seasonal_sales_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the seasonal sales pattern DataFrame against the predefined schema.

    """
    logging.info("Validating seasonal sales pattern data schema")
    
    return seasonal_sales_pattern_schema.validate(seasonal_sales_df)

