from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from typing import Optional
import pandas as pd
import numpy as np
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
        "param": "funder",
        "label": "Funder Name",
        "options": df["funder"].unique()
    },
    {
        "param": "retraction_type",
        "label": "Retraction Type",
        "options": df["retractionnature"].unique()
    },
]

# Convert the values to a list of strings
for allowed_value in allowed_values:    
    allowed_value["options"] = [str(value) for value in allowed_value["options"]]
    allowed_value["options"] = sorted(allowed_value["options"])

def get_filtered_df(
        publisher = None,
        prefix = None,
        container = None,
        funder = None,
        retraction_type = None
):
    filtered_df = df.copy()
    if publisher:
        filtered_df = filtered_df[filtered_df["publisher"] == publisher]
    if prefix:
        filtered_df = filtered_df[filtered_df["prefix"] == prefix]
    if container:
        filtered_df = filtered_df[filtered_df["container"] == container]
    if funder:
        filtered_df = filtered_df[filtered_df["funder"] == funder]
    if retraction_type:
        filtered_df = filtered_df[filtered_df["retractionnature"] == retraction_type]

    return filtered_df

def get_chart_title(title = "", publisher = None, prefix = None, container = None, funder = None, retraction_type = None):
    if publisher:
        title += f" - Publisher: {publisher}"
    if prefix:
        title += f" - DOI Prefix: {prefix}"
    if container:
        title += f" - Container Title: {container}"
    if funder:
        title += f" - Funder: {funder}"
    if retraction_type:
        title += f" - Retraction Type: {retraction_type}"

    return title

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "allowed_values": allowed_values,
    })

@app.get("/chart-year")
async def create_chart_year(
    publisher: Optional[str] = Query(None),
    prefix: Optional[str] = Query(None),
    container: Optional[str] = Query(None),
    funder: Optional[str] = Query(None),
    retraction_type: Optional[str] = Query(None),
):
    filtered_df = get_filtered_df(
        publisher,
        prefix,
        container,
        funder,
        retraction_type
    )

    # Create a chart by year from 'originalpaperdate'
    filtered_df["year"] = pd.to_datetime(filtered_df["originalpaperdate"]).dt.year
    filtered_df["value"] = 1
    filtered_df = filtered_df.groupby("year").sum().reset_index()
    filtered_df = filtered_df.set_index("year")

    # Define full range of years (e.g. from min to max year)
    year_range = range(filtered_df.index.min(), filtered_df.index.max() + 1)

    # Reindex to include all years, fill missing with 0
    filtered_df = filtered_df.reindex(year_range, fill_value=0)
    filtered_df = filtered_df.sort_index()

    # Plot
    plt.figure(figsize=(10, 6))
    filtered_df["value"].plot(kind="bar")
    plt.title(get_chart_title("Evolution by Year", publisher, prefix, container, funder, retraction_type))
    plt.xlabel("Year")
    plt.ylabel("Value")
    plt.tight_layout()

    # Save chart to a temporary file and return it
    temp_file = "chart_year.png"
    plt.savefig(temp_file, format="png")
    plt.close()

    # Open the file and send it as a response
    return StreamingResponse(open(temp_file, "rb"), media_type="image/png")

@app.get("/chart-article-type")
async def create_chart_year(
    publisher: Optional[str] = Query(None),
    prefix: Optional[str] = Query(None),
    container: Optional[str] = Query(None),
    funder: Optional[str] = Query(None),
    retraction_type: Optional[str] = Query(None),
):
    filtered_df = get_filtered_df(
        publisher,
        prefix,
        container,
        funder,
        retraction_type
    )

    # Create a bar chart by 'articletype'
    filtered_df["value"] = 1
    filtered_df = filtered_df.groupby("articletype").sum().reset_index()
    filtered_df = filtered_df.set_index("articletype")
    filtered_df = filtered_df.sort_index()

    # Plot
    plt.figure(figsize=(10, 6))
    filtered_df["value"].plot(kind="bar")
    plt.title(get_chart_title("Evolution by Article Type", publisher, prefix, container, funder, retraction_type))
    plt.xlabel("Article Type")
    plt.ylabel("Value")
    plt.tight_layout()

    # Save chart to a temporary file and return it
    temp_file = "chart_article_type.png"
    plt.savefig(temp_file, format="png")
    plt.close()

    # Open the file and send it as a response
    return StreamingResponse(open(temp_file, "rb"), media_type="image/png")


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
