"""
Script to look up the data for originalpaperdoi from CrossRef
"""

import os
import pandas as pd

INPUT_DIR = "data"
INPUT_RW_PARQUET = os.path.join(INPUT_DIR, "retraction_watch_etl.parquet")
OUTPUT_RW_PARQUET = os.path.join(INPUT_DIR, "retraction_watch_etl_sampled.parquet")
OUTPUT_RW_CSV = os.path.join(INPUT_DIR, "retraction_watch_etl_sampled.csv")

def load_parquet(file_path: str) -> pd.DataFrame:
    """
    Load the Parquet file into a pandas DataFrame.
    """
    print(f"Loading Parquet file from {file_path}...")
    df = pd.read_parquet(file_path)
    print(f"Loaded {len(df)} rows from {file_path}")
    return df

def save_parquet(df: pd.DataFrame, file_path: str) -> None:
    """
    Save the DataFrame to a Parquet file.
    """
    print(f"Saving DataFrame to {file_path}...")
    df.to_parquet(file_path, index=False)
    print(f"Saved DataFrame to {file_path}")

def save_csv(df: pd.DataFrame, file_path: str) -> None:
    """
    Save the DataFrame to a CSV file.
    """
    print(f"Saving DataFrame to {file_path}...")
    df.to_csv(file_path, index=False)
    print(f"Saved DataFrame to {file_path}")

def main():
    # Load the Parquet file
    df_full = load_parquet(INPUT_RW_PARQUET)

    df_samples = pd.DataFrame()

    # remove rows without 'originalpaperdoi' (empty string, NaN, None, 'Unavailable')
    df_full = df_full[df_full['originalpaperdoi'].notna() & (df_full['originalpaperdoi'] != '') & (df_full['originalpaperdoi'] != 'Unavailable')]

    # sample 5000 rows from df_full
    df_samples = df_full.sample(n=5000, random_state=1)
    df_samples.reset_index(drop=True, inplace=True)
    print(f"Sampled {len(df_samples)} rows from {len(df_full)} rows")

    # save the sampled DataFrame to a Parquet file
    save_parquet(df_samples, OUTPUT_RW_PARQUET)
    save_csv(df_samples, OUTPUT_RW_CSV)


if __name__ == "__main__":
    main()