import os
import pandas as pd
import numpy as np

INPUT_DIR = "data"
PARQUET = os.path.join(INPUT_DIR, "retraction_watch_etl.parquet")

# load the parquet file
def load_parquet(file_path: str) -> pd.DataFrame:
    """
    Load the Parquet file into a pandas DataFrame.

    Args:
        file_path (str): The path to the Parquet file.

    Returns:
        pd.DataFrame: The loaded DataFrame.
    """
    print(f"Loading Parquet file from {file_path} into dataframe...")

    df = pd.read_parquet(file_path)
    return df

def head(df: pd.DataFrame, n: int = 5) -> None:
    """
    Display the first n rows of the DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to display.
        n (int): The number of rows to display. Default is 5.
    """
    print(f"Displaying the first {n} rows of the DataFrame:")
    print(df.head(n))

def columns_info(df: pd.DataFrame) -> None:
    """
    Print each column name, its data type and the first 5 rows.

    Args:
        df (pd.DataFrame): The DataFrame to display.
    """
    print("Columns information:")
    for col in df.columns:
        print(f"Column: {col}, Data Type: {df[col].dtype}")
        print(df[col].head(5))
        print()

def columns_unique_values(df: pd.DataFrame, col: str) -> None:
    """
    Print unique values of a specific column.

    Args:
        df (pd.DataFrame): The DataFrame to display.
        col (str): The column name to display unique values for.
    """
    print(f"Unique values in column '{col}':")

    # if col is a numpy.ndarray, we need to combine them first
    if isinstance(df[col].iloc[0], (list, pd.Series, np.ndarray)):
        # if col is a list or numpy.ndarray, we need to flatten it first
        unique_values = set()
        for item in df[col]:
            if isinstance(item, (list, np.ndarray)):
                unique_values.update(tuple(sub_item) if isinstance(sub_item, np.ndarray) else sub_item for sub_item in item)
            else:
                unique_values.add(tuple(item) if isinstance(item, np.ndarray) else item)
        print(unique_values)

    else:
        # if col is not a list, we can use the unique method directly
        # but we need to convert it to a set to remove duplicates
        unique_values = set(df[col].unique())
        print(unique_values)

def main():
    # Load the Parquet file
    df = load_parquet(PARQUET)

    # Display the first 5 rows
    head(df, 5)

    # Display the last 5 rows
    head(df, -5)

    # Display columns information
    columns_info(df)

    # unique values in 'retractionnature' and 'articletype' column
    columns_unique_values(df, 'retractionnature')
    columns_unique_values(df, 'reason')
    columns_unique_values(df, 'articletype')

if __name__ == "__main__":
    main()