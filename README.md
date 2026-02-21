# Brazil-ASEAN Trade Radar

An end-to-end data pipeline and dashboard analyzing Brazilian export data to ASEAN countries (1997-2026), designed for diplomatic and executive stakeholders.

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
3. (Scripts pipeline documentation pending...)