from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
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

df = load_parquet(INPUT_RW_PARQUET)

app = FastAPI()

# Set up Jinja2 templates
templates = Jinja2Templates(directory="./src/templates")

# Serve static files (optional for styling)
app.mount("/static", StaticFiles(directory="./src/static"), name="static")

# get allowed filter values from df
allowed_values = [
    {
        "param": "publisher",
        "label": "Publisher Name",
        "options": df["publisher"].unique()
    }, 
    {
        "param": "prefix",
        "label": "DOI Prefix",
        "options": df["prefix"].unique()
    },
    {
        "param": "container",
        "label": "Container Title",
        "options": df["container"].unique()
    },
    {
        "param": "country",
        "label": "Country Name",
        "options": df["rorcountries"].unique()
    },
    {
        "param": "institute",
        "label": "Institute Name",
        "options": df["rornames"].unique()
    },
    {
        "param": "funder",
        "label": "Funder Name",
        "options": df["funder"].unique()
    },
    {
        "param": "retraction_reason",
        "label": "Retraction Type",
        "options": df["reason"].unique()
    },
]

# Convert the values to a list of strings
for allowed_value in allowed_values:    
    allowed_value["options"] = [str(value) for value in allowed_value["options"]]
    allowed_value["options"] = sorted(allowed_value["options"])

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "allowed_values": allowed_values,
    })

@app.get("/create-chart")
def create_chart(
    publisher: Optional[str] = Query(None),
    container: Optional[str] = Query(None),
    country: Optional[str] = Query(None),
    institute: Optional[str] = Query(None),
    funder: Optional[str] = Query(None),
    retraction_reason: Optional[str] = Query(None),
):
    # Filter the DataFrame based on the provided query parameters
    filtered_df = df.copy()
    if publisher:
        filtered_df = filtered_df[filtered_df["publisher"] == publisher]
    if container:
        filtered_df = filtered_df[filtered_df["container"] == container]
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