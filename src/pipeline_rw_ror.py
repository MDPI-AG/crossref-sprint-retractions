"""
This script implement a pipeline to insert ROR IDs from the pipeline_ror back into the
Retraction Watch data set.
"""
import os
import pandas as pd

INOUT_DIR = "data"
INPUT_ROR_PARQUET = os.path.join(INOUT_DIR, "ror_etl.parquet")
OUTPUT_RW_PARQUET = os.path.join(INOUT_DIR, "retraction_watch_etl_sampled.parquet")
OUTPUT_RW_CSV = os.path.join(INOUT_DIR, "retraction_watch_etl_sampled.csv")

def load_parquet(file_path: str):
    """
    Load the Parquet file into a pandas DataFrame.
    """
    print(f"Loading Parquet file from {file_path}...")
    df = pd.read_parquet(file_path)
    print(f"Loaded {len(df)} rows from {file_path}")
    return df

def save_parquet(df, file_path: str):
    """
    Save the DataFrame to a Parquet file.
    """
    print(f"Saving DataFrame to {file_path}...")
    df.to_parquet(file_path, index=False)
    print(f"Saved DataFrame to {file_path}")

def save_csv(df, file_path: str):
    """
    Save the DataFrame to a CSV file.
    """
    print(f"Saving DataFrame to {file_path}...")
    df.to_csv(file_path, index=False)
    print(f"Saved DataFrame to {file_path}")

def merge_rors_with_rw(df_ror, df_rw):
    # loop RW data and check if the institution is in the ROR data
    # and if so, add the ROR ID to the RW data
    print("Merging ROR data with Retraction Watch data...")

    fields  = ['rorids', 'rornames', 'rorcountries', 'rorregions']
    for field in fields:
        if field not in df_rw.columns:
            df_rw[field] = [[] for _ in range(len(df_rw))]

    for index, row in df_rw.iterrows():
        institutions = row['institution']
        rorids = list(row['rorids'])
        rornames = list(row['rornames'])
        rorcountries = list(row['rorcountries'])
        rorregions = list(row['rorregions'])

        for institution in institutions:
            if institution in df_ror['raw'].values:
                ror_row = df_ror.loc[df_ror['raw'] == institution]
                rorids.append(ror_row['ror'].values[0])
                rornames.append(ror_row['name'].values[0])
                rorcountries.append(ror_row['country'].values[0])
                rorregions.append(ror_row['region'].values[0])
                
        rorids = list(set(rorids))
        df_rw.at[index, 'rorids'] = rorids
        rornames = list(set(rornames))
        df_rw.at[index, 'rornames'] = rornames
        rorcountries = list(set(rorcountries))
        df_rw.at[index, 'rorcountries'] = rorcountries
        rorregions = list(set(rorregions))
        df_rw.at[index, 'rorregions'] = rorregions

    return df_rw

def main():
    """
    Main function to run the pipeline.
    """
    # Load the Retraction Watch data
    df_rw = load_parquet(OUTPUT_RW_PARQUET)

    # Load the ROR data
    df_ror = load_parquet(INPUT_ROR_PARQUET)

    # Merge the dataframes on the 'rorids' column
    df_rw = merge_rors_with_rw(df_ror, df_rw)

    # Save the merged dataframe
    save_parquet(df_rw, OUTPUT_RW_PARQUET)
    save_csv(df_rw, OUTPUT_RW_CSV)
    
if __name__ == "__main__":
    main()