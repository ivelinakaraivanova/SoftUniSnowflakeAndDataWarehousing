from logger import logging
import pandas as pd
import pandera.pandas as pa

from pandera.pandas import DataFrameSchema, Column, Check
from pandera.errors import SchemaErrors


ranking_product_schema = DataFrameSchema(
    {
        "product_id": Column(int),
        "revenue": Column(float, Check.ge(0.0)),
        "sales_count": Column(int, Check.ge(0)),
        "value_bucket": Column(str, Check.isin(["Low Performer", "Average", "Bestseller"])),
    })


def validate_ranking_product_schema(ranking_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the ranking products DataFrame against the predefined schema.

    """
    logging.info("Validating ranking products data schema")
    
    return ranking_product_schema.validate(ranking_df)
