import pandas as pd
import pandera.pandas as pa

from pandera.pandas import DataFrameSchema, Column, Check

from ..logger import setup_logger


logging = setup_logger("validations.enrich_schema")


enrich_output_schema = DataFrameSchema(
    {
        "sales_id": Column(int),
        "product_id": Column(int),
        "region": Column(str),
        "quantity": Column(int),
        "price": Column(float),
        "timestamp": Column(pa.DateTime),
        "total_sales": Column(float),
        "product_id": Column(int),
        "category": Column(str),
        "brand": Column(str),
        "rating": Column(float),
        "month": Column(str),
        "hour": Column(int),
        "sales_bucket": Column(str),
    })


def validate_output_enrich_schema(enrich_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the enriched DataFrame against the predefined schema.

    """
    logging.info("Validating enriched data schema")
    
    return enrich_output_schema.validate(enrich_df)
