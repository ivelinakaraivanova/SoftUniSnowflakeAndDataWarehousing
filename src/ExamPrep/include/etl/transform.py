import logging
from numpy import int64
import pandas as pd

from include.validations.enrich_schema import validate_output_enrich_schema
from include.validations.hourly_sales_schema import validate_output_hourly_sales_trend_schema
from include.validations.products_schema import validate_input_products_schema, validate_output_products_schema
from include.validations.ranking_product_schema import validate_ranking_product_schema
from include.validations.revenue_concentration_schema import validate_revenue_concentration_schema
from include.validations.sales_schema import validate_input_sales_schema, validate_output_sales_schema
from include.validations.seasonal_sales_schema import validate_output_seasonal_sales_pattern_schema


def transform_sales_data(sales_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the sales DataFrame by cleaning and formatting.

    """

    logging.info("Cleaning sales data")

    sales_df = validate_input_sales_schema(sales_df)
    
    sales_df.columns = sales_df.columns.str.strip().str.replace(' ', '_')
    sales_df["region"] = sales_df["region"].str.strip().str.lower()
    sales_df = sales_df.dropna(subset=["region", "timestamp"])
    sales_df = sales_df[(sales_df["price"] > 0) & (sales_df["quantity"] > 0)]
    sales_df["timestamp"] = pd.to_datetime(sales_df["timestamp"], format="mixed", errors="coerce")
    sales_df['total_sales'] = sales_df['quantity'] * sales_df['price']
    
    sales_df = validate_output_sales_schema(sales_df)

    logging.info("Sales data cleaned successfully")

    return sales_df


def transform_products_data(products_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the products DataFrame by cleaning and formatting.

    """

    logging.info("Cleaning products data")

    products_df = validate_input_products_schema(products_df)
    
    products_df.columns = products_df.columns.str.strip().str.replace(' ', '_')
    products_df["category"] = products_df["category"].str.strip().str.lower()
    products_df["brand"] = products_df["brand"].str.strip().str.upper()
    products_df = products_df.dropna(subset=["product_id", "rating"])
    products_df = products_df.drop_duplicates()
    
    products_df = validate_output_products_schema(products_df)

    logging.info("Products data cleaned successfully")

    return products_df


def merge_sales_and_products(sales_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merges sales and products DataFrames on product_id.

    """

    logging.info("Merging sales and products data")

    merged_df = sales_df.merge(products_df, on="product_id", how="inner")

    logging.info("Sales and products data merged successfully")

    return merged_df


def enrich_merged_data(merged_df: pd.DataFrame) -> pd.DataFrame:
    """
    Enriches the merged DataFrame with additional calculated fields.

    """

    logging.info("Enriching merged data")

    merged_df["timestamp"] = pd.to_datetime(merged_df["timestamp"], format="mixed", errors="coerce")
    merged_df["month"] = merged_df["timestamp"].dt.to_period("M").astype(str)
    merged_df["week"] = merged_df["timestamp"].dt.isocalendar().week
    merged_df["weekday"] = merged_df["timestamp"].dt.day_name()
    merged_df["hour"] = merged_df["timestamp"].dt.hour.astype(int64)

    merged_df["sales_bucket"] = pd.cut(
        merged_df["total_sales"],
        bins=[-1, 100, 500, float("inf")],
        labels=["low", "medium", "high"]
    )

    logging.info("Merged data enriched successfully")

    return validate_output_enrich_schema(merged_df)


def hourly_sales_trend(enriched_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates enriched DataFrame to get hourly sales trends.

    """

    logging.info("Calculating hourly sales trend")

    agg = enriched_df.groupby(by=["region", "category", "hour"], as_index=False).agg(hourly_sales_trend=("total_sales", "sum"))
    idx = agg.groupby(['region', 'category'])['hourly_total_sales'].idxmax()
    peaks = agg.loc[idx].reset_index(drop=True)

    logging.info("Hourly sales trend calculated successfully")

    return validate_output_hourly_sales_trend_schema(peaks)


def product_sales_ranking_with_brand(enriched_df: pd.DataFrame) -> pd.DataFrame:
    """
    Ranks products based on total sales within each brand.

    """

    logging.info("Calculating product sales ranking within brand")

    ranking_df = enriched_df.groupby(by=["brand", "product_id", "category", "rating"], as_index=False).agg(revenue=("total_sales", "sum"), sales_count=("quantity", "sum"))
    ranking_df["value_bucket"] = pd.qcut(
        ranking_df["revenue"],
        q=[0, 0.2, 0.8, 1.0],
        labels=["Low Performer", "Average", "Bestseller"]
    )

    logging.info("Product sales ranking within brand calculated successfully")

    return validate_ranking_product_schema(ranking_df)


def seasonal_sales_pattern(enriched_df: pd.DataFrame) -> pd.DataFrame:
    """
    Analyzes seasonal sales patterns from the enriched DataFrame.

    """

    logging.info("Analyzing seasonal sales patterns")

    enriched_df["timestamp"] = pd.to_datetime(enriched_df["timestamp"], format="mixed", errors="coerce")
    enriched_df["quarter"] = enriched_df["timestamp"].dt.to_period("Q").astype(str)
    seasonal_df = enriched_df.groupby(by=["quarter", "category"], as_index=False).agg(total_sales=("total_sales", "sum"))

    logging.info("Seasonal sales patterns analyzed successfully")

    return validate_output_seasonal_sales_pattern_schema(seasonal_df)


def revenue_concentration(enriched_df: pd.DataFrame) -> pd.DataFrame:
    """
    Analyzes revenue concentration across different regions.

    """

    logging.info("Analyzing revenue concentration across regions")

    revenue_df = enriched_df.groupby(by=["region"], as_index=False).agg(region_revenue=("total_sales", "sum"))
    total_revenue = revenue_df["region_revenue"].sum()
    revenue_df["revenue_share"] = revenue_df["region_revenue"] / total_revenue
    revenue_df["cumulative_share"] = revenue_df["region_revenue"].cumsum()

    logging.info("Revenue concentration analysis completed successfully")

    return validate_revenue_concentration_schema(revenue_df)

