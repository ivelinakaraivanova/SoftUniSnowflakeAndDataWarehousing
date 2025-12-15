import pandas as pd
import pandera as pa

from pandera import DataFrameSchema, Column, Check
from pandera.errors import SchemaErrors


sales_output_schema = DataFrameSchema(
    {
        "sales_id": Column(int, Check.greater_than(0)),
        "product_id": Column(int, Check.greater_than(0)),
        "region": Column(str, Check(lambda s: s.str.islower())),
        "quantity": Column(int, Check.greater_than(0)),
        "price": Column(float, Check.greater_than(0.0)),
        "timestamp": Column(pa.DateTime),
        "discount": Column(float, Check.between(0, 1)),
        "order_status": Column(str)
    })


product_output_schema = DataFrameSchema(
    {
        "product_id": Column(int, Check.greater_than(0)),
        "category": Column(str, Check(lambda s: s.str[0].str.isupper())),
        "brand": Column(str, checks=Check.str_matches(r'^Brand[A-Z]$')),
        "rating": Column(float, Check.between(0.0, 5.0)),
        "in_stock": Column(bool),
        "launch_date": Column(pa.DateTime)
    })
