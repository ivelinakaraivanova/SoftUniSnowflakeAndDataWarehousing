import pandas as pd
import pandera.pandas as pa

from pandera import Column, DataFrameSchema, Check
from pandera.errors import SchemaErrors

from ..logger import setup_logger
logging = setup_logger("validations.forecast_schema")


forecast_schema = DataFrameSchema(
    {
        "order_date": Column(pa.DateTime),
        "total_revenue": Column(float, Check.greater_than_or_equal_to(0)),
        "sales_forecast": Column(float, Check.greater_than_or_equal_to(0)),
    })


def validate_post_forecast_schema(forecast_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the transformed forecast DataFrame against the predefined schema.

    """
    logging.info("Validating transformed forecast data schema")

    return forecast_schema.validate(forecast_df)