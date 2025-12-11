import pandas as pd
from sqlalchemy import create_engine

from config.settings import HOST, PASSWORD, PORT, USER, DATABASE


def load_to_postgresql(df: pd.DataFrame, table_name: str):
    connection_string = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
    engine = create_engine(connection_string)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
