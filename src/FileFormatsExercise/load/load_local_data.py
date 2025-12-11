import logging
import pandas as pd

from pathlib import Path

PROJECT_ROOT= Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "output"

def load_local_data_file(df: pd.DataFrame, file_name: str, file_format: str = "csv") -> None:
    """
    Loads the given DataFrame to a local file in the specified format (CSV or JSON).

    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    file_path = OUTPUT_DIR / f"{file_name}.{file_format}"

    logging.info(f"Starting to save DataFrame to {file_path}")

    try:
        if file_format == "csv":
            df.to_csv(file_path, index=False)
        elif file_format == "json":
            df.to_json(file_path, orient="records")
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
    except Exception as e:
        logging.error(f"Error saving DataFrame to {file_path}: {e}")
        raise

    logging.info(f"Successfully saved DataFrame to {file_path}")
    