import pandas as pd
import pandera as pa

from pandera import DataFrameSchema, Column, Check


sales_input_schema = DataFrameSchema(
    {
        "sales id": Column(int),
        "proDuct Id": Column(int),
        "Region": Column(str, nullable=True),
        "qty": Column(int),
        "Price": Column(float),
        "Time stamp": Column(str, nullable=True),
        "discount": Column(float),
        "order_status": Column(str)
    })


product_input_schema = DataFrameSchema(
    {
        "product_id": Column(int),
        "category": Column(str),
        "brand": Column(str),
        "rating": Column(float),
        "in_stock": Column(bool),
        "launch_date": Column(str, nullable=True)
    })
