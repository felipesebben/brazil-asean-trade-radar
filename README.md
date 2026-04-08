# Brazil-ASEAN Trade Radar

An end-to-end data pipeline and dashboard analyzing Brazilian export data to ASEAN countries (2019-2026). Designed for diplomatic and executive stakeholders, this project bridges data engineering solutions with attempts to reduce friction in UX.

## Architecture

This project utilizes a Medallion Architecture (Bronze, Silver, Gold) strictly using open-source tools before visualizing in Tableau Public. It was engineerind to process massive, sparse government datasets locally without memory bottlenecks.

* **Bronze Layer:** Raw historical data (CSV) ingested directly from the Comex Stat portal.
* **Silver Layer:** Cleaned and modeled Parquet files (Star Schema) using DuckDB.
* **Gold Layer:** Highly aggregated, lightweight CSV files strictly tailored for Tableau Public ingestion.

## Engineering Highlights
* **Dealing with sparse data**. Government trade data frequently omits rows for months with zero trade. This pipeline uses dynamic `FULL OUTER JOIN` and `COALESCE` logic in DuckDB to ensure Month-over-Month (MoM) and Year-over-Year (YoY) statistical variances.
* **Out-of-Core Processing**. Instead of using `pandas` , used DuckDB to bypass RAM limitations, transforming gigabytes of raw csv/parquet files directly on-disk.
* **Statistical Anomaly Detection**. The dashboard UI applies thresholds defined after interviews to visually isolate macroecnomic outliers and prevent alert fatigue.
* **Executive UI/UX**. Dashboard design features unified mental model, custom modal overalays (glossary and how-to guides), as well as native pipeline timestamps to build data trust.

## Tech Stack
* **Data Processing and SQL Engine:** DuckDB
* **Pipeline scripting**: 
* **Environment Management:** Poetry
* **Visualization:** Tableau Public
* **Design:** Figma

## Setup Instructions
1. Clone the repository.
2. Run `poetry install` to build the environment.
3. Create the required local directories for the Bronze layer
   ```bash
   mkdir -p data/raw/dimensions
   mkdir -p data/raw/exports
   ```

## Pipeline execution
To generate the final Gold layer for Tableau, place your downloaded Comex Stat CSV files into the `data/raw/` folders, then run the pipeline strictly in this order:
1. **Ingest Dimensions**: (Builds the dictionaries)
   ```bash
    poetry run python scripts/01_ingest_dimensions.py
   ```

2. **Ingest Historical Facts**: (Compiles and compresses the multi-year raw exports)
    ```bash
    poetry run python scripts/02_ingest_historical.py
    ```

3. **Build Gold Layer**: (Filters for ASEAN block 53, handles sparse data, injects pipeline timestamps, calculates YoY metrics)
   ```bash
   poetry run scripts/03_build_gold.py
   ```

## Data Source
All raw data is publicly provided by the Brazilian Government via the [Comex Stat Portal](https://www.gov.br/mdic/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta).