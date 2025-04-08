"""
This script implements a simple ETL (Extract, Transform, Load) pipeline for processing 
retraction data from the Retraction Watch database hosted on GitLab by CrossRef. The repo
on GitLab receives almost daily updates.

Steps:
1. **Extract**: Downloads a CSV file containing retracted research articles.
2. **Transform**: Cleans the data by normalizing column names and converting date fields to datetime objects.
3. **Load**: Saves the processed data as a parquet file for efficient downstream usage.

The output is stored in the `data/` directory as both CSV and Parquet formats.
"""

import os
import requests
import pandas as pd
from datetime import datetime
from typing import Optional
from pathlib import Path

CSV_URL = "https://gitlab.com/crossref/retraction-watch-data/-/raw/main/retraction_watch.csv?ref_type=heads&inline=false"
OUTPUT_DIR = "data"
OUTPUT_CSV_RAW = os.path.join(OUTPUT_DIR, "retraction_watch_raw.csv")
OUTPUT_PARQUET_ETL = os.path.join(OUTPUT_DIR, "retraction_watch_etl.parquet")
OUTPUT_CSV_ETL = os.path.join(OUTPUT_DIR, "retraction_watch_etl.csv")

METADATA_CSV = os.path.join(OUTPUT_DIR, "metadata.csv")
POLYFILL_CSV = os.path.join(OUTPUT_DIR, "retraction_watch_polyfill.csv")

# init metadata
metadata = {
    'rw-last-downloaded': None,
}

def get_metadata() -> None:
    """
    Load metadata from the metadata CSV file.
    """
    if os.path.exists(METADATA_CSV):
        metadata_df = pd.read_csv(METADATA_CSV)
        # print the df
        print(f"Metadata loaded from {METADATA_CSV}:")
        print(metadata_df)
        
        for index, row in metadata_df.iterrows():
            metadata[row['name']] = row['value']
    else:
        print(f"Metadata file {METADATA_CSV} does not exist. Creating a new one.")
        metadata['rw-last-downloaded'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        save_metadata()
        print(f"Metadata file {METADATA_CSV} created with initial values.")

def save_metadata() -> None:
    """
    Save metadata to the metadata CSV file.
    """
    metadata_df = pd.DataFrame(list(metadata.items()), columns=['key', 'value'])
    metadata_df.to_csv(METADATA_CSV, index=False)
    print(f"Metadata saved to {METADATA_CSV}")

def download_csv(url: str, output_path: str) -> None:
    """
    Download the CSV file from the given URL and save it to the specified output path.

    Args:
        url (str): The URL of the CSV file to download.
        output_path (str): The path where the downloaded CSV file will be saved.
    """
    print(f"Downloading {url} to {output_path}...")

    response = requests.get(url)
    if response.status_code == 200:
        with open(output_path, 'wb') as f:
            f.write(response.content)
        print(f"Downloaded {url} to {output_path}")
    else:
        print(f"Failed to download {url}. Status code: {response.status_code}")

def load_csv(file_path: str) -> pd.DataFrame:
    """
    Load the CSV file into a pandas DataFrame.

    Args:
        file_path (str): The path to the CSV file.

    Returns:
        pd.DataFrame: The loaded DataFrame.
    """
    print(f"Loading CSV file from {file_path} into dataframe...")

    df = pd.read_csv(file_path)
    return df

def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
    return df

def process_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process the DataFrame by renaming columns and converting date columns to datetime.

    Args:
        df (pd.DataFrame): The DataFrame to process.

    Returns:
        pd.DataFrame: The processed DataFrame.
    """
    # Convert date columns to datetime
    date_columns = ['date', 'retraction_date']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # handle the columns with multiple values separated by semicolon
    multi_value_fields = ['urls', 'articletype', 'reason', 'institution']
    for field in multi_value_fields:
        if field in df.columns:
            # split by ; then trim
            df[field] = df[field].apply(lambda x: [item.strip() for item in str(x).split(';')] if pd.notnull(x) else [])
            # clean-up the lists where '' values left
            df[field] = df[field].apply(lambda x: [item for item in x if item != ''])
    
    # drop fields we do not need from RW
    drop_fields = ['record_id', 'retractionpubmedid', 'originalpaperpubmedid', 'title', 'subject', 'journal', 'publisher', 'country', 'author', 'paywalled']
    df.drop(columns=drop_fields, inplace=True, errors='ignore')

    return df

def polyfill_originalpaperdoi(df: pd.DataFrame) -> pd.DataFrame:
    """
    Polyfill the originalpaperdoi column in the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to polyfill.

    Returns:
        pd.DataFrame: The polyfilled DataFrame.
    """
    # polyfill 'originalpaperdoi' from 'retractiondoi' if the former is null
    df.loc[df['originalpaperdoi'].isnull() & df['retractiondoi'].notnull(), 'originalpaperdoi'] = df['retractiondoi']
    return df


def polyfill_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Polyfill the DataFrame with additional data if needed.

    Args:
        df (pd.DataFrame): The DataFrame to polyfill.

    Returns:
        pd.DataFrame: The polyfilled DataFrame.
    """
    polyfill_df = pd.read_csv(POLYFILL_CSV)

    for index, row in polyfill_df.iterrows():
        doi = row['originalpaperdoi']
        field = row['field']
        value = row['value']

        if doi in df['originalpaperdoi'].values:
            df.loc[df['originalpaperdoi'] == doi, field] = value
    
    return df

def drop_rows_with_empty_doi(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop rows with empty DOIs from the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to process.

    Returns:
        pd.DataFrame: The DataFrame with rows containing empty DOIs removed.
    """
    df = df[df['originalpaperdoi'].notnull()]
    return df

# save as parquet
def save_to_parquet(df: pd.DataFrame, output_path: str) -> None:
    """
    Save the DataFrame to a parquet file.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        output_path (str): The path where the parquet file will be saved.
    """
    df.to_parquet(output_path, index=False)
    print(f"Saved DataFrame to {output_path}")

# save as csv
def save_to_csv(df: pd.DataFrame, output_path: str) -> None:
    """
    Save the DataFrame to a CSV file.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        output_path (str): The path where the CSV file will be saved.
    """
    df.to_csv(output_path, index=False)
    print(f"Saved DataFrame to {output_path}")

def main() -> None:
    """
    Main function to execute the data pipeline.
    """
    # Create output directory if it doesn't exist
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    # only download if not downloaded today
    get_metadata()
    if metadata['rw-last-downloaded'] and metadata['rw-last-downloaded'] >= datetime.now().strftime("%Y-%m-%d"):
        print("Data already downloaded today. Skipping CSV download.")
    else:
        download_csv(CSV_URL, OUTPUT_CSV_RAW)
        metadata['rw-last-downloaded'] = datetime.now().strftime("%Y-%m-%d")
        save_metadata()

    # Load the CSV file into a DataFrame
    df = load_csv(OUTPUT_CSV_RAW)
    df = rename_columns(df)

    # Polyfill the DataFrame with additional data
    df = polyfill_originalpaperdoi(df)
    df = polyfill_data(df)

    # Process the DataFrame
    df = process_data(df)

    # Drop rows that are left without originalpapedoi
    pre_length = len(df)
    df = drop_rows_with_empty_doi(df)
    post_length = len(df)
    print(f"Dropped {pre_length - post_length} rows with empty DOIs.")

    # Save the ETL DataFrame to parquet and csv formats
    save_to_parquet(df, OUTPUT_PARQUET_ETL)
    save_to_csv(df, OUTPUT_CSV_ETL)


if __name__ == "__main__":
    main()