import pandas as pd
import pandera.pandas as pa

from pandera import Column, DataFrameSchema, Check
from pandera.errors import SchemaErrors

from ..logger import setup_logger
logging = setup_logger("validations.segmented_schema")


segmented_schema = DataFrameSchema(
    {
        "customer_id": Column(int, Check.greater_than(0), unique=True),
        "total_spent": Column(float, Check.greater_than_or_equal_to(0)),
        "customer_segment": Column(str, Check.isin(["Low", "Medium", "High", "VIP"])),
        "segmentation_date": Column(pa.DateTime),
    })


def validate_post_segmented_schema(segmented_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the transformed segmented DataFrame against the predefined schema.

    """
    logging.info("Validating transformed segmented data schema")

    return segmented_schema.validate(segmented_df)
