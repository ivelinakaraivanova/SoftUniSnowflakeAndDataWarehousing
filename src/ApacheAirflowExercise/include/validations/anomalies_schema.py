import pandas as pd
import pandera.pandas as pa

from pandera import Column, DataFrameSchema, Check
from pandera.errors import SchemaErrors

from ..logger import setup_logger
logging = setup_logger("validations.anomalies_schema")


anomalies_schema = DataFrameSchema(
    {
        "order_id": Column(str),
        "customer_id": Column(int, Check.greater_than(0)),
        "product_id": Column(int, Check.greater_than(0)),
        "order_date": Column(pa.DateTime),
        "total_revenue": Column(float),
    })


def validate_post_anomalies_schema(anomalies_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the transformed anomalies DataFrame against the predefined schema.
    """
    logging.info("Validating transformed anomalies data schema")
    return anomalies_schema.validate(anomalies_df)
