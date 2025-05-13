import pandas as pd
import os

def rank_dataframe(df: pd.DataFrame, rank_column_name: str = "Rank") -> pd.DataFrame:
  return (
    df.reset_index(drop=True)
      .rename_axis(rank_column_name)
      .reset_index()
      .assign(**{rank_column_name: lambda x: x[rank_column_name] + 1})
  )

def get_base_data_url():
  if "HOME" in os.environ and "streamlit" in os.environ["HOME"]:
    BASE_DATA_URL = os.path.join(os.getcwd(), "VincentLeV", "app", "data", "banned_books")
  else:
    BASE_DATA_URL = os.path.join(os.getcwd(), "data", "banned_books")
  return BASE_DATA_URL