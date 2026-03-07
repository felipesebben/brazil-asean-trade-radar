# Brazil-ASEAN Trade Radar

An end-to-end data pipeline and dashboard analyzing Brazilian export data to ASEAN countries (2019-2026), designed for diplomatic and executive stakeholders.

## Architecture

This project utilizes a Medallion Architecture (Bronze, Silver, Gold) strictly using open-source tools before visualizing in Tableau Public.

* **Bronze Layer:** Raw historical data (CSV) ingested directly from the Comex Stat portal.
* **Silver Layer:** Cleaned and modeled Parquet files (Star Schema) using DuckDB.
* **Gold Layer:** Highly aggregated, lightweight CSV files strictly tailored for Tableau Public ingestion.

## Tech Stack
* **Data Processing:** DuckDB
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

2. **Ingest Historical Facts**: (Compiles the multi-year raw exports)
    ```bash
    poetry run python scripts/02_ingest_historical.py
    ```

3. **Build Gold Layer**: (Filters for ASEAN block 53, groups to SH6, calculates YoY metrics)
   ```bash
   poetry run scripts/03_build_gold.py
   ```

## Data Source
All raw data is publicly provided by the Brazilian Government via the [Comex Stat Portal](https://www.gov.br/mdic/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta).