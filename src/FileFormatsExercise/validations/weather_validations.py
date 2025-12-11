import logging
import pandas as pd

from pandera.pandas import DataFrameSchema, Column, Check
from pandera.errors import SchemaError


weather_schema = DataFrameSchema(
    {
        "city": Column(str, checks=Check.str_length(min_value=1, max_value=100)),
        "temperature": Column(str, checks=Check.str_length(min_value=1, max_value=100, error="Invalid temperature format")),
        "feels_like": Column(str, checks=Check.str_length(min_value=1, max_value=100, error="Invalid feels_like format")),
    },
    strict=True,
)

def validate_weather_data(df: pd.DataFrame, lazy: bool = True) -> pd.DataFrame:
    """
    Validates the weather data DataFrame against the predefined schema.

    """
    logging.info("Starting validation of weather data")

    try:
        validated_df = weather_schema.validate(df, lazy=lazy)   # lazy=True to collect all errors, lazy=False to raise on first error
    except SchemaError as e:
        logging.error(f"Weather data validation error: {e}")
        raise

    logging.info("Weather data validation successful")
    
    return validated_df
