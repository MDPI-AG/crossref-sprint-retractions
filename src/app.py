from fastapi import FastAPI, Query
from typing import Optional
import pandas as pd
import io
import os
from fastapi.responses import StreamingResponse

import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Use a non-interactive backend

INPUT_DIR = "data"
INPUT_RW_PARQUET = os.path.join(INPUT_DIR, "retraction_watch_etl_sampled.parquet")

def load_parquet(file_path: str) -> pd.DataFrame:
    """
    Load the Parquet file into a pandas DataFrame.
    """
    print(f"Loading Parquet file from {file_path}...")
    df = pd.read_parquet(file_path)
    print(f"Loaded {len(df)} rows from {file_path}")
    return df

#df = load_parquet(INPUT_RW_PARQUET)

app = FastAPI()

# Example DataFrame (replace this with your actual data loading logic)
data = {
    "publisher": ["Pub1", "Pub2", "Pub1", "Pub3"],
    "journal": ["Journal1", "Journal2", "Journal1", "Journal3"],
    "country": ["US", "UK", "US", "CA"],
    #"region": ["US", "UK", "US", "CA"],
    "institute": ["Inst1", "Inst2", "Inst1", "Inst3"],
    "funder": ["Funder1", "Funder2", "Funder1", "Funder3"],
    "retraction_reason": ["Reason1", "Reason2", "Reason1", "Reason3"],
    "value": [10, 20, 30, 40],
}
df = pd.DataFrame(data)

@app.get("/create-chart")
def create_chart(
    publisher: Optional[str] = Query(None),
    journal: Optional[str] = Query(None),
    country: Optional[str] = Query(None),
    institute: Optional[str] = Query(None),
    funder: Optional[str] = Query(None),
    retraction_reason: Optional[str] = Query(None),
):
    # Filter the DataFrame based on the provided query parameters
    filtered_df = df.copy()
    if publisher:
        filtered_df = filtered_df[filtered_df["publisher"] == publisher]
    if journal:
        filtered_df = filtered_df[filtered_df["journal"] == journal]
    if country:
        filtered_df = filtered_df[filtered_df["country"] == country]
    if institute:
        filtered_df = filtered_df[filtered_df["institute"] == institute]
    if funder:
        filtered_df = filtered_df[filtered_df["funder"] == funder]
    if retraction_reason:
        filtered_df = filtered_df[filtered_df["retraction_reason"] == retraction_reason]

    # Create a simple chart (e.g., bar chart of values)
    plt.figure(figsize=(10, 6))
    filtered_df["value"].plot(kind="bar")
    plt.title("Filtered Data Chart")
    plt.xlabel("Index")
    plt.ylabel("Value")
    plt.tight_layout()

    # Save the chart to a BytesIO object
    buf = io.BytesIO()
    plt.savefig(buf, format="png")
    buf.seek(0)
    plt.close()

    # Return the chart as a streaming response
    return StreamingResponse(buf, media_type="image/png")