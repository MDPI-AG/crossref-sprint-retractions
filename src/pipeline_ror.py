"""
This script implement a pipeline to match institution data from Retraction Watch
against the ROR API.
"""

import os
import requests
import time
import pandas as pd
import numpy as np
from datetime import datetime

OUTPUT_DIR = "data"

INPUT_PARQUET_ETL = os.path.join(OUTPUT_DIR, "retraction_watch_etl_sampled.parquet")
OUTPUT_PARQUET_ETL = os.path.join(OUTPUT_DIR, "ror_etl.parquet")
OUTPUT_CSV_ETL = os.path.join(OUTPUT_DIR, "ror_etl.csv")

API_URL = "https://api.ror.org/v2/organizations?query={query}"

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
    institutions = []
    for index, row in df_rw.iterrows():
        institutions.extend(row['institution'])

    print(f"Institutions found in RW data: {len(institutions)}")
    
    # Convert numpy.ndarray elements to strings before creating a set
    institutions = list(set(str(inst) if isinstance(inst, (list, np.ndarray)) else inst for inst in institutions))

    # loop institutions and get ROR data if not yet in df_ror
    counter = 0
    progress = 0
    for institution in institutions:
        progress += 1

        # dump progress every 100 iterations
        if progress % 100 == 0:
            print(f"Processed {progress} institutions...")

        if institution in df_ror['raw'].values:
            institutions.remove(institution)
            continue

        counter += 1
        ror_url = API_URL.format(query=institution)
        response = requests.get(ror_url, headers=headers)
        if response.status_code == 200:
            ror_data = response.json()
            # check for ror_data.items and get first item
            if 'items' in ror_data and isinstance(ror_data['items'], list) and len(ror_data['items']) > 0:
                item = ror_data['items'][0]
                ror_id = item['id']
                
                name = None
                if 'name' in item:
                    name = item['name']
                elif 'names' in item and len(item['names']) > 0:
                    # loop item[names] to find first with types[] that contains 'ror_display'
                    for name_item in item['names']:
                        if 'types' in name_item and ('ror_display' in name_item['types']):
                            name = name_item['value']
                            break

                country = None
                region = None
                if 'locations' in item and len(item['locations']) > 0:
                    location = item['locations'][0]['geonames_details']
                    country = location['country_name']
                    if 'country_subdivision_name' in location:
                        region = location['country_subdivision_name']

                new_row = pd.DataFrame([{
                    'raw': institution,
                    'ror': ror_id,
                    'name': name,
                    'country': country,
                    'region': region
                }])

                df_ror = pd.concat([df_ror, new_row], ignore_index=True)
            else:
                print(f"No ROR data found for institution: {institution}")
        
        # throttle requests to avoid hitting the API too hard
        if counter % 20 == 0:
            print(f"Processed {counter} institutions, sleeping for 2 seconds...")
            time.sleep(2)

        # dump files every 100 iterations
        if counter % 100 == 0:
            print(f"Processed {counter} institutions, saving intermediate results...")
            save_parquet(df_ror, OUTPUT_PARQUET_ETL)
            save_csv(df_ror, OUTPUT_CSV_ETL)

    return df_ror

def main():
    if not os.path.exists(INPUT_PARQUET_ETL):
        raise FileNotFoundError(f"Input Parquet file {INPUT_PARQUET_ETL} does not exist.")

    df_rw = load_parquet(INPUT_PARQUET_ETL)

    if os.path.exists(OUTPUT_PARQUET_ETL):
        df_ror = load_parquet(OUTPUT_PARQUET_ETL)
    else:
        # create a df with columns name, ror
        df_ror = pd.DataFrame(columns=['raw', 'ror', 'name', 'country', 'region'])
    
    # Match ROR IDs for the instituions data from RW
    df_ror = get_ror_data(df_ror, df_rw)
    
    # Save the merged data to a Parquet file
    save_parquet(df_ror, OUTPUT_PARQUET_ETL)
    save_csv(df_ror, OUTPUT_CSV_ETL)

if __name__ == "__main__":
    main()