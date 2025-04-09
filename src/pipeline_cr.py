"""
Script to look up the data for originalpaperdoi from CrossRef
"""

import os
import requests
import time
import pandas as pd
import numpy as np

API_URL = "https://api.crossref.org/works/{doi}"

INPUT_DIR = "data"
INPUT_RW_PARQUET = os.path.join(INPUT_DIR, "retraction_watch_etl_sampled.parquet")
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

def fetch_cr_data(doi: str) -> dict:
    """
    Fetch data from CrossRef API for a given DOI.
    """
    url = API_URL.format(doi=doi)    
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        msg = data['message']

        return {
            "articletype": msg['type'],
            "container": isinstance(msg['container-title'], list) and msg['container-title'][0] or msg['container-title'],
            "publisher": msg['publisher'],
            "prefix": msg['prefix'],
        }
    else:
        print(f"Error fetching data for DOI: {doi}, Status Code: {response.status_code}")
        return False
    
def extract_cr_data(df_rw: pd.DataFrame) -> pd.DataFrame:
    """
    Extract CrossRef data for each DOI in the DataFrame.
    """
    print("Extracting CrossRef data...")

    # make sure the columns exist in the DataFrame
    fields = ["articletype", "container", "publisher", "prefix"]
    for field in fields:
        if field not in df_rw.columns:
            df_rw[field] = pd.Series(dtype=pd.StringDtype())

    # loop rows in df_rw
    count = 0
    for index, row in df_rw.iterrows():
        doi = row['originalpaperdoi']
        if pd.notna(doi):
            data = fetch_cr_data(doi)
            if data:
                for key, value in data.items():
                    if isinstance(value, (list, np.ndarray, pd.Series)):
                        value = value[0] if len(value) > 0 else None
                    df_rw.at[index, key] = value
            else:
                # drop the row, could be a non CroddRef DOI
                df_rw.drop(index, inplace=True)
        
        count += 1
        if count % 20 == 0:
            time.sleep(1)

        if count % 100 == 0:
            print(f"Processed {count} rows...")
            return df_rw

    print(f"Processed {count} rows in total.")

    return df_rw

def main():
    # Load the Retraction Watch data
    df_rw = load_parquet(INPUT_RW_PARQUET)
    
    # Extract CrossRef data
    df_rw = extract_cr_data(df_rw)

    print(df_rw.loc[:,["articletype", "container", "publisher", "prefix"]].head(100)
)
    print("Data extraction complete.")

    # make sure all columns are strings
    for col in df_rw.columns:
        df_rw[col] = df_rw[col].apply(lambda x: str(x) if not isinstance(x, str) else x)

    # Save the updated DataFrame to Parquet and CSV    
    save_parquet(df_rw, OUTPUT_RW_PARQUET)
    save_csv(df_rw, OUTPUT_RW_CSV)

if __name__ == "__main__":
    main()