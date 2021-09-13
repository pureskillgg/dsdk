from typing import Dict
import pandas as pd


def handle_value_error(value_error: object, channel: Dict) -> pd.DataFrame:
    if str(value_error) == "need at least one array to concatenate":
        return pd.DataFrame({col["name"]: [] for col in channel["columns"]})
    raise Exception(f"Couldn't read in parquet file {channel['channel']}")
