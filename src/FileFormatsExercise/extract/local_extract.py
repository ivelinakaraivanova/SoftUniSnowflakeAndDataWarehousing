import logging
import json

import pandas as pd


def extract_customers_from_json(file_path: str) -> pd.DataFrame:
    """
    Extracts customer data from a JSON file and returns it as a pandas DataFrame.

    """
    logging.info(f"Starting extraction of customer data from {file_path}")
    
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
    except Exception as e:
        logging.error(f"Error reading JSON file {file_path}: {e}")
        raise

    try:
        df = pd.DataFrame(data)
    except Exception as e:
        logging.error(f"Error converting data from JSON file {file_path}: {e}")
        raise

    logging.info(f"Successfully extracted data from {file_path}. Shape: {df.shape}")
    return df


def extract_and_flatten_orders_from_json(file_path: str) -> pd.DataFrame:
    """
    Extracts order data from a JSON file, flattens nested structures, and returns it as a pandas DataFrame.

    """
    logging.info(f"Starting extraction of order data from {file_path}")
    
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
    except Exception as e:
        logging.error(f"Error reading JSON file {file_path}: {e}")
        raise

    try:
        df = pd.json_normalize(data, meta=['order_id', 'customer_id'], record_path=['order_details'])
    except Exception as e:
        logging.error(f"Error flattening data from JSON file {file_path}: {e}")
        raise

    logging.info(f"Successfully extracted and flattened data from {file_path}. Shape: {df.shape}")
    return df
