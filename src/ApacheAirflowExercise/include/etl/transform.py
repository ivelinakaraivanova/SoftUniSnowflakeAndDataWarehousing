import pandas as pd

from include.validations.aggregates_schema import validate_pre_aggregates_schema, validate_post_aggregates_schema
from include.validations.anomalies_schema import validate_post_anomalies_schema
from include.validations.customers_schema import validate_post_customers_schema, validate_pre_customers_schema
from include.validations.forecast_schema import validate_post_forecast_schema
from include.validations.products_schema import validate_post_products_schema, validate_pre_products_schema
from include.validations.sales_schema import validate_post_sales_schema, validate_pre_sales_schema
from include.validations.segmented_schema import validate_post_segmented_schema

from ..logger import setup_logger


logging = setup_logger("etl.transform")


def clean_sales_data(sales_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the sales data by cleaning and formatting.

    """
    logging.info("Cleaning sales data")

    sales_df = validate_pre_sales_schema(sales_df)

    sales_df.columns = sales_df.columns.str.lower().str.replace(" ", "_")
    sales_df.dropna(inplace=True)
    sales_df["order_date"] = pd.to_datetime(sales_df["order_date"], format="mixed", errors='coerce')
    sales_df["total_revenue"] = sales_df["amount"] * sales_df["quantity"]

    sales_df = validate_post_sales_schema(sales_df)

    logging.info("Sales data cleaned successfully")

    return sales_df


def clean_customers_data(customers_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the customers data by cleaning and formatting.

    """
    logging.info("Cleaning customers data")

    customers_df = validate_pre_customers_schema(customers_df)

    customers_df.columns = customers_df.columns.str.lower().str.replace(" ", "_")
    customers_df.dropna(inplace=True)
    customers_df["signup_date"] = pd.to_datetime(customers_df["signup_date"], format="mixed", errors='coerce')

    customers_df = validate_post_customers_schema(customers_df)

    logging.info("Customers data cleaned successfully")
    
    return customers_df


def clean_products_data(products_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the products data by cleaning and formatting.

    """
    logging.info("Cleaning products data")

    products_df = validate_pre_products_schema(products_df)

    products_df.columns = products_df.columns.str.lower().str.replace(" ", "_")
    products_df.dropna(inplace=True)

    products_df = validate_post_products_schema(products_df)

    logging.info("Products data cleaned successfully")
    
    return products_df


def merge_data(sales_df: pd.DataFrame, customers_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merges sales, customers, and products data into a single DataFrame.

    """
    logging.info("Merging data")

    merged_df = sales_df.merge(customers_df, on="customer_id", how="inner").copy()  # Where there is merging, filtering is a good practice to have a copy, which keeps a reference to the original dataframe, but doesn't break it
    merged_df = merged_df.merge(products_df, on="product_id", how="inner").copy()
    merged_df["profit_margin"] = merged_df["profit"] / merged_df["total_revenue"]

    logging.info("Data merged successfully")

    return merged_df


def compute_monthly_aggregates(merged_df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes monthly aggregates from the merged data.

    """
    logging.info("Computing monthly aggregates")

    merged_df = validate_pre_aggregates_schema(merged_df)

    merged_df["order_date"] = pd.to_datetime(merged_df["order_date"], format="mixed", errors='coerce')
    aggregated_df = merged_df.groupby(pd.Grouper(key="order_date", freq="M")).agg(
        total_sales = ("total_revenue", "sum"),
        unique_customers = ("customer_id", "nunique")
    ).reset_index().copy()

    aggregated_df = validate_post_aggregates_schema(aggregated_df)

    logging.info("Monthly aggregates computed successfully")

    return aggregated_df


def segment_customers(sales_df: pd.DataFrame, customers_df: pd.DataFrame) -> pd.DataFrame:
    """
    Segments customers based on their total spending.

    """
    logging.info("Segmenting customers")

    total_spent_df = sales_df.groupby("customer_id")["total_revenue"].sum().reset_index().copy()
    total_spent_df.rename(columns={"total_revenue": "total_spent"}, inplace=True)

    segmented_df = customers_df.merge(total_spent_df, on="customer_id", how="left").copy()
    segmented_df.dropna(subset=["total_spent"], inplace=True)

    segmented_df["customer_segment"] = pd.cut( 
        segmented_df["total_spent"],
        bins=[0, 1000, 5000, 10000, float("inf")],
        labels=["Low", "Medium", "High", "VIP"]
    )

    segmented_df["segmentation_date"] = pd.to_datetime(customers_df["signup_date"], format="mixed", errors='coerce')
    allowed_columns = ["customer_id", "total_spent", "customer_segment", "segmentation_date"]
    customer_segmenting = segmented_df[allowed_columns].copy()

    customer_segmenting = validate_post_segmented_schema(customer_segmenting)

    logging.info(f"Customers segmented successfully: {len(customer_segmenting)} records")

    return customer_segmenting


def detect_sales_anomalies(sales_df: pd.DataFrame) -> pd.DataFrame:
    """
    Detects anomalies in sales data based on total revenue.

    """
    logging.info("Detecting sales anomalies")

    threshold = sales_df["total_revenue"].mean() - 3 * sales_df["total_revenue"].std()
    anomalies_df = sales_df[sales_df["total_revenue"] < threshold].copy()
    allowed_columns = ["order_id", "customer_id", "product_id", "order_date", "total_revenue"]
    anomalies_df = anomalies_df[allowed_columns].copy()

    anomalies_df = validate_post_anomalies_schema(anomalies_df)

    logging.info(f"Detected {len(anomalies_df)} anomalies in sales data")

    return anomalies_df


def forecast_sales(sales_df: pd.DataFrame) -> pd.DataFrame:
    """
    Forecast sales for 7 days mean

    """
    logging.info("Sales forecasting ")

    sales_df["order_date"] = pd.to_datetime(sales_df["order_date"], format="mixed", errors='coerce')
    sales_df.set_index("order_date", inplace=True)
    sales_df["sales_forecast"] = sales_df["total_revenue"].rolling(window=7, min_periods=1).mean()
    sales_df.reset_index(inplace=True)
    allowed_columns = ["order_date", "total_revenue", "sales_forecast"]
    sales_df = sales_df[allowed_columns].copy()

    sales_df = validate_post_forecast_schema(sales_df)

    logging.info(f"Sales forecasting completed successfully {len(sales_df)} records")
    return sales_df
