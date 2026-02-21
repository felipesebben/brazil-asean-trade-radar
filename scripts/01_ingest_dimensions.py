import duckdb
import os


class DimensionIngestor:
    """
    A class to handle the bulk ingestion and conversion of Comex Stat
    dimension tables from raw CSVs to optimized Parquet files.
    """

    def __init__(self, raw_dir: str = "data/raw", silver_dir: str = "data/silver"):
        """
        Initializes the ingestor, ensures directories exist,
        and opens a shared DuckDB connection.
        """
        self.raw_dir = raw_dir
        self.silver_dir = silver_dir

        # Ensure the destination directory exists
        os.makedirs(self.silver_dir, exist_ok=True)

        # Open one connection to be reused by all methods
        self.con = duckdb.connect()
        print(f"Initialized DimensionIngestor. Output targeting: {self.silver_dir}/")

    def process_file(self, raw_filename: str, parquet_filename: str):
        """
        Reads a single raw CSV, normalize headers, and saves it as a Parquet file.

        Args:
            raw_filename (str): The name of the source CSV.
            parquet_filename (str): The desired Parquet name.
        """
        input_path = os.path.join(self.raw_dir, raw_filename)
        output_path = os.path.join(self.silver_dir, parquet_filename)

        if not os.path.exists(input_path):
            print(f"Warning: {raw_filename} not found in {self.raw_dir}. Skipping.")
            return
        
        print(f"Processing {raw_filename}...")

        # DuckDB Query
        query = f"""
            COPY (
                SELECT * FROM read_csv(
                    '{input_path}',
                    delim=';',
                    header=True,
                    normalize_names=True
                )
            ) TO '{output_path}' (FORMAT PARQUET);
        """

        try:
            self.con.execute(query)
            print(f"Saved to {output_path}")
        except Exception as e:
            print(f"Failed to process {raw_filename}: {e}")

    def run_all(self):
        """
        Orchestrates the ingestion of all defined dimension tables using a mapping dictionary.
        """
        print("Starting Dimension Pipeline...\n")

        # Mapping dictionary: source CSV -> Target parquet
        dimension_map = {
            "NCM.csv": "dim_ncm.parquet",
            "NCM_SH.csv": "dim_ncm_sh.parquet",
            "ISIC_CUCI.csv": "dim_isic_cuci.parquet",
            "PAIS.csv": "dim_pais.parquet",
            "PAIS_BLOCO.csv": "dim_pais_bloco.parquet"
        }

        for raw_csv, target_parquet in dimension_map.items():
            self.process_file(raw_csv, target_parquet)
        
        print("\nDimension Pipeline Complete!")


if __name__ == "__main__":
    # Instantiate the class and run the pipeline
    ingestor = DimensionIngestor()
    ingestor.run_all()


