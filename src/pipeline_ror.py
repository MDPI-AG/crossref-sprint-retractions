"""
This script implement a pipeline to match institution data from Retraction Watch
against the ROR API.
"""

import os
import requests
import pandas as pd
from datetime import datetime

OUTPUT_DIR = "data"

INPUT_PARQUET_ETL = os.path.join(OUTPUT_DIR, "retraction_watch_etl.parquet")
OUTPUT_PARQUET_ETL = os.path.join(OUTPUT_DIR, "ror_etl.parquet")

API_URL = "https://api.ror.org/organizations"

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
    return df

def get_ror_data(df_ror: pd.DataFrame, df_rw: pd.DataFrame) -> pd.DataFrame:
    """
    Get the ROR data from the ROR API and match it with the Retraction Watch data.
    """
    print("Getting ROR data...")
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }

    # collect all 'institution' fields from RW data (lists) and remove duplicates
    institutions = df_rw['institution'].drop_duplicates().tolist()
    print(f"Found {len(institutions)} unique institutions in Retraction Watch data")
    exit()
    
    # Get the ROR data
    response = requests.get(ror_url, headers=headers)
    if response.status_code == 200:
        ror_data = response.json()
        print(f"Retrieved {len(ror_data)} rows from ROR API")
    else:
        print(f"Failed to retrieve ROR data. Status code: {response.status_code}")
        return None
    
    # Convert the ROR data to a DataFrame
    df_ror = pd.json_normalize(ror_data)
    
    # Merge the ROR data with the Retraction Watch data
    df_merged = pd.merge(df_rw, df_ror, left_on='institution', right_on='name', how='left')
    
    return df_merged

def main():
    if not os.path.exists(INPUT_PARQUET_ETL):
        raise FileNotFoundError(f"Input Parquet file {INPUT_PARQUET_ETL} does not exist.")

    df_rw = load_parquet(INPUT_PARQUET_ETL)

    if os.path.exists(OUTPUT_PARQUET_ETL):
        df_ror = load_parquet(OUTPUT_PARQUET_ETL)
    else:
        df_ror = pd.DataFrame()
    
    # Match ROR IDs for the instituions data from RW
    df_ror = get_ror_data(df_ror, df_rw)
    
    # Save the merged data to a Parquet file
    save_parquet(df_ror, OUTPUT_PARQUET_ETL)

if __name__ == "__main__":
    main()