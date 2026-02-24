import duckdb
import os

class HistoricalIngestor:
    """
    A class to handle the bulk ingestion of yearly Comex Stat export data.
    Combines muyltiple yearly CSVs into a single, optimized Parquet fact table.
    """

    def __init__(self, raw_exports_dir: str = "data/raw/exports", silver_dir: str = "data/silver"):
        self.raw_exports_dir = raw_exports_dir
        self.silver_dir = silver_dir

        # Ensure directories exist
        os.makedirs(self.raw_exports_dir, exist_ok=True)
        os.makedirs(self.silver_dir, exist_ok=True)

        self.con = duckdb.connect()
        print(f"Initialized HistoricalIngestor. Reading from {self.raw_exports_dir}/")

    def process_historical_load(self):
        """
        Uses DuckDB globbing to read all yearly CSVs at once and writes them
        to a single Parque fact table.
        """
        # Target output file
        output_path = os.path.join(self.silver_dir, "fact_exports.parquet")

        # The Glob pattern - looks for any file starting with EXP_ and ending in .csv.
        glob_pattern = os.path.join(self.raw_exports_dir, "EXP_*.csv")

        print(f"Reading and combining files matching: {glob_pattern}")

        # DuckDB query with globbing
        query = f"""
            COPY(
                SELECT * FROM read_csv(
                    '{glob_pattern}',
                    delim=';',
                    header=True,
                    normalize_names=True,
                    ignore_errors=True
                )
            ) TO '{output_path}' (FORMAT PARQUET);
        """

        try:
            self.con.execute(query)
            print(f"Success! Historical fact table saved to {output_path}")
        except Exception as e:
            print(f"Error during historical load: {e}")
    
if __name__ == "__main__":
    ingestor = HistoricalIngestor()
    ingestor.process_historical_load()