import pandas as pd
from sqlalchemy import create_engine
from config import DB_URI


def extract_data(table_name: str) -> pd.DataFrame:
    """
    Connects to MySQL and extracts table into pandas DataFrame
    """

    engine = create_engine(DB_URI)

    query = f"SELECT * FROM {table_name}"

    df = pd.read_sql(query, engine)

    print(f"Extracted {len(df)} rows from {table_name}")

    return df
