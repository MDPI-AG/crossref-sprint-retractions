# CrossRef Metadata Sprint, Madrid

## Retractions Data Analysis

Purpose: Connect Retraction Watch (RW) data set with references and cited-by
data from CrossRef; e.g. analyze the doi prefixes citing retracted papers
pre- and post-retraction.

# Running the Analysis

## Prerequisites

Have a local copy of ROR-API running via Docker: https://github.com/ror-community/ror-api#readme

## Setup

1. Clone the repository
1. Active venv environment:
   ```bash
   source .venv/bin/activate
   ```
1. Install the requirements:
   ```bash
   pip install -r requirements.txt
   ```

## Runing the Scripts

1. Run the pipeline to ETL the RW data set:
   ```bash
   python src/pipeline_rw.py
   ```
1. Run the pipeline to match ROR IDs for affiliations:
   ```bash
   python src/pipeline_ror.py
   ```
1. Dig into the RW data set:
   ```bash
   python src/analyze_rw_dataset.py
   ```

# Notes / Todos

General note: articles in RW may not have a DOI that is registered in CrossRef.
We need to check if the DOI is registered in CrossRef.

Data fields in Retraction Watch (RW) data set:

1. **subject**: subject seems not easily usable; may need a subject classification API
1. **institution**: this piece of data might not be present in CrossRef, so we can take
   it from the RW data set. We need to match it again ROR API to get the institution's
   main ROR ID.
1. **journal** and **publisher**: we should not rely on RW data for this but the from
   CrossRef.
1. **country**: we ignore this piece of information as we will infer it from the
   institution's ROR record.
1. **author**: we can take from CrossRef data set.
1. **urls**: needs preprocessing as may contain multiple URLs. Contains URL to Retraction
   Watch blog if any.
1. **articletype**: this is the type of the article, which is any of:
   `['Clinical Study', 'Supplementary Materials', 'Auto/Biography', 'Interview/Q&A', 'Expression of Concern', 'Book Chapter/Reference Work', 'Case Report', 'Trade Magazines', 'Conference Abstract/Paper', 'Correction/Erratum/Corrigendum', 'Dissertation/Thesis', 'Preprint', 'Meta-Analysis', 'Commentary/Editorial', 'Research Article', 'Review Article', 'Article in Press', 'Other', 'Legal Case/Analysis', 'Retraction Notice', 'Technical Report/White Paper', 'Guideline', 'Government Publication', 'Letter', 'Retracted Article', 'Revision']`
   We will use this to filter out the articles we are interested in.
1. **retractiondate**: we keep this info; we need to check if any cited-by publication is
   published after this date. Key data point.
1. **retractiondoi**: this is the DOI of the RETRACTION NOTICE.
1. **originalpaperdoi**: this is the DOI of the articles that has been RETRACTED. Key data
   point.
1. **retractionnature**: type of the retraction, which is any of:
   `['Retraction' 'Correction' 'Expression of concern' 'Reinstatement']`
