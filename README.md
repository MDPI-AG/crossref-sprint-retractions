# CrossRef Metadata Sprint, Madrid

## Retractions Data Analysis

Purpose: Connect retraction watch data set with references and cited-by
data from CrossRef; e.g. analyze the doi prefixes citing retracted papers
pre- and post-retraction.

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
   `['retraction' 'expression-of-concern' 'correction' 'reinstatement']`
   We will use this to filter out the articles we are interested in.
1. **retractiondate**: we keep this info; we need to check if any cited-by publication is
   published after this date. Key data point.
1. **retractiondoi**: this is the DOI of the RETRACTION NOTICE.
1. **originalpaperdoi**: this is the DOI of the articles that has been RETRACTED. Key data
   point.
1. **retractionnature**: type of the retraction, which is any of:
   `['Retraction' 'Correction' 'Expression of concern' 'Reinstatement']`
