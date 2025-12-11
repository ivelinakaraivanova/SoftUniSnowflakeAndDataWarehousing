import pandas as pd
import psycopg2
from sqlalchemy import create_engine

from config.settings import USER, PASSWORD, HOST, PORT, DATABASE


def load_raw_to_postgres(df: pd.DataFrame, table_name: str, if_exists: str = "append") -> None:
   
    try:
        connection_string = f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}'
        engine = create_engine(connection_string)
        with engine.connect() as connection:
            df.to_sql(name=table_name, con=connection, if_exists=if_exists, index=False)
        print(f"Data loaded successfully into table '{table_name}'.")
    except Exception as e:
        print(f"Error loading data into PostgreSQL table '{table_name}': {e}")
        raise


def load_transformed_to_postgres(df: pd.DataFrame, table_name: str) -> None:
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        order_id INT PRIMARY KEY,
        customer_id INT,
        total_revenue NUMERIC(10, 2),
        profit_margin NUMERIC(10, 2),
        shipping_days INT
    );
    """
    insert_sql = f"""
    INSERT INTO {table_name} (order_id, customer_id, total_revenue, profit_margin, shipping_days)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (order_id) DO UPDATE SET
        customer_id = EXCLUDED.customer_id,
        total_revenue = EXCLUDED.total_revenue,
        profit_margin = EXCLUDED.profit_margin,
        shipping_days = EXCLUDED.shipping_days;
    """

    db_params = {"host": HOST, "port": PORT, "database": DATABASE, "user": USER, "password": PASSWORD}

    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_table_sql)
                values= [
                    (
                        row['order_id'],
                        row['customer_id'],
                        row['total_revenue'],
                        row['profit_margin'],
                        row['shipping_days']
                    )
                    for _, row in df.iterrows()
                    ]
                cursor.executemany(insert_sql, values)
                conn.commit()
                print(f"Transformed data loaded successfully into table '{table_name}'.")
    except Exception as e:
        print(f"Error loading transformed data into PostgreSQL table '{table_name}': {e}")
        raise

