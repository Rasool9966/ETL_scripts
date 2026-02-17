import pandas as pd


def transform_data(df: pd.DataFrame) -> pd.DataFrame:

    df["etl_created_at"] = pd.Timestamp.now()

    df["source"] = "mysql"

    return df
