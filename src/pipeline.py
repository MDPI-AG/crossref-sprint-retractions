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
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "retraction_watch.csv")
OUTPUT_PARQUET = os.path.join(OUTPUT_DIR, "retraction_watch.parquet")

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

def process_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process the DataFrame by renaming columns and converting date columns to datetime.

    Args:
        df (pd.DataFrame): The DataFrame to process.

    Returns:
        pd.DataFrame: The processed DataFrame.
    """
    # Rename columns
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]

    # Convert date columns to datetime
    date_columns = ['date', 'retraction_date']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

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

def main() -> None:
    """
    Main function to execute the data pipeline.
    """
    # Create output directory if it doesn't exist
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    # Download the CSV file
    download_csv(CSV_URL, OUTPUT_CSV)

    # Load the CSV file into a DataFrame
    df = load_csv(OUTPUT_CSV)

    # Process the DataFrame
    df = process_data(df)

    # Save the DataFrame to a parquet file
    save_to_parquet(df, OUTPUT_PARQUET)

main()