import pandas as pd
import numpy as np


def clean_data(dfs: list[pd.DataFrame], old_column_name:str | None = None, 
               new_column_name: str | None = None) -> list[pd.DataFrame]:
    
    possible_data_columns = ["order_date", "signup_date", "delivery_date"]
    cleaned_dfs = []

    for i, df in enumerate(dfs):
        if df is None or df.empty:
            print(f"DataFrame at index {i} is None or empty, skipping cleaning.")
            continue

        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

        if old_column_name is not None and old_column_name in df.columns:
            df.rename(columns={old_column_name: new_column_name}, inplace=True)
            print(f"Renamed column {old_column_name} to {new_column_name} in DataFrame at index {i}.")

        for col in possible_data_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format = '%d-%m-%y', errors='coerce')
                print(f"Converted column {col} to datetime in DataFrame at index {i}.")

        cleaned_dfs.append(df)

    return cleaned_dfs


def remove_duplicates(dfs: list[pd.DataFrame], 
                      subset:list[str] | None = None, keep='first') -> list[pd.DataFrame]:
    
    cleaned_dfs = []

    for i, df in enumerate(dfs):
        if df is None or df.empty:
            print(f"DataFrame at index {i} is None or empty, skipping duplicate removal.")
            continue

        initial_count = len(df)
        df = df.drop_duplicates(subset=subset, keep=keep).reset_index(drop=True)
        final_count = len(df)
        print(f"Removed {initial_count - final_count} duplicates from DataFrame at index {i}.")

        cleaned_dfs.append(df)

    return cleaned_dfs


def merge_data(dfs: list[pd.DataFrame], 
               merge_columns: list[tuple[str, str]], 
               how: str = 'inner') -> pd.DataFrame:
    
    if not dfs or len(dfs) < 2:
        raise ValueError("At least two DataFrames are required for merging.")

    merged_df = dfs[0]

    for i, df in enumerate(dfs[1:]):
        left_key, right_key = merge_columns[i]
        print(f"Merging column {left_key} to {right_key}")
        merged_df = merged_df.merge(df, how=how, 
                             left_on=left_key, right_on=right_key)
        print(f"Merged DataFrame at index {i} on keys ({left_key}, {right_key}).")

    return merged_df


def compute_derived_columns(merged_df: pd.DataFrame) -> pd.DataFrame:
    
    required_columns = ['quantity', 'amount', 'profit', 'discount']
    missing = set(required_columns) - set(merged_df.columns)

    if missing:
        raise KeyError(f"Missing required columns for derived calculations: {missing}")
    
    merged_df['total_revenue'] = merged_df['quantity'] * merged_df['amount']
    merged_df['profit_margin'] = merged_df['profit'] / merged_df['total_revenue']
    merged_df['discounted_price'] = merged_df['amount'] * (1 - merged_df['discount'] / 100)

    return merged_df


def segment_deliveries(merged_df: pd.DataFrame, 
                       fast_threshold : int = 3, 
                       slow_threshold : int = 10) -> pd.DataFrame:
    
    if "shipping_days" not in merged_df.columns:
        raise KeyError("Column 'shipping_days' not found in DataFrame.")
     
    conditions = [
         merged_df["shipping_days"] < fast_threshold,
         merged_df["shipping_days"] > slow_threshold
     ]

    choices = ["fast", "slow"]

    merged_df["delivery_category"] = np.select(conditions, choices, default="standard")

    return merged_df


def categorize_products(merged_df: pd.DataFrame) -> pd.DataFrame:
    
    if "amount" not in merged_df.columns:
        raise KeyError("Column 'amount' not found in DataFrame.")
    
    bins = [0, 50, 200, np.inf]
    labels = ["low", "medium", "high"]

    merged_df["product_category"] = pd.cut(merged_df["amount"], bins=bins, labels=labels, right=True)

    return merged_df

