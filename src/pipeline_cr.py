"""
Script to look up the data for originalpaperdoi from CrossRef
"""

import os
import requests
import pandas as pd
import numpy as np

from pyalex import Works

API_URL = "https://api.crossref.org/works/{doi}"

INPUT_DIR = "data"
INPUT_RW_PARQUET = os.path.join(INPUT_DIR, "retraction_watch_etl.parquet")

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

def fetch_cr_data(doi: str) -> dict:
    """
    Fetch data from CrossRef API for a given DOI.
    """
    url = API_URL.format(doi=doi)
    print(f"Fetching data for DOI: {doi} from {url}...")
    
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        msg = data['message']

        return {
            "articletype": msg.get('type'),
            "container": msg.get('container-title'),
            "publisher": msg.get('publisher'),
            "prefix": msg.get('prefix'),
        }
    else:
        print(f"Error fetching data for DOI: {doi}, Status Code: {response.status_code}")
        return None
    
def extract_cr_data(df_rw: pd.DataFrame) -> pd.DataFrame:
    """
    Extract CrossRef data for each DOI in the DataFrame.
    """
    print("Extracting CrossRef data...")

    # make sure the columns exist in the DataFrame
    fields = ["articletype", "container", "publisher", "prefix"]
    for field in fields:
        if field not in df_rw.columns:
            df_rw[field] = None

    # loop rows in df_rw
    for index, row in df_rw.iterrows():
        doi = row['originalpaperdoi']
        if pd.notna(doi):
            data = fetch_cr_data(doi)
            if data:
                for key, value in data.items():
                    df_rw.at[index, key] = value

    exit(0)

    return df_rw

def main():
    # Load the Retraction Watch data
    df_rw = load_parquet(INPUT_RW_PARQUET)
    
    # Extract CrossRef data
    df_rw = extract_cr_data(df_rw)
    
    # Save the extracted data to a new Parquet file
    # todo - test the changes from CR data mash-up first

if __name__ == "__main__":
    main()