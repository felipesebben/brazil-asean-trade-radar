import duckdb
import os

class GoldLayerBuilder:
    """
    A class to build the final, aggregated Gold Layer dataset for Tableau.
    Filters by economic block, aggregates to SH6, and formats dates.
    """

    def __init__(self, target_block_code: int = 53, silver_dir: str = "data/silver", gold_dir: str = "data/gold"):
        """
        Initializes the builder. The code `53` (ASEAN) was set as default,
        though it can be changed to something else in the future.
        """
        self.target_block_code = target_block_code
        self.silver_dir = silver_dir
        self.gold_dir = gold_dir
        self.con = duckdb.connect()

        # Ensure the new Gold directory exists
        os.makedirs(self.gold_dir, exist_ok=True)
        print(f"Initialized GoldLayerBuilder for Block {self.target_block_code}.")

    def build_executive_dashboard_view(self):
        """
        Executes the final data modeling query and exports directly to a CSV
        optimized for Tableau.
        """
        output_csv = os.path.join(self.gold_dir, f"gold_exports_block_{self.target_block_code}.csv")

        # Master query
        query = f"""
            COPY(
                WITH filtered_facts AS (
                    -- 1. Filter selected target block
                    SELECT
                        f.co_ano,
                        f.co_mes,
                        f.co_ncm,
                        f.co_pais,
                        f.vl_fob,
                        f.kg_liquido
                    FROM '{self.silver_dir}/fact_exports.parquet' AS f
                    INNER JOIN '{self.silver_dir}/dim_pais_bloco.parquet' AS b
                        ON f.co_pais = b.co_pais
                    WHERE b.co_bloco = '{self.target_block_code}'
                ),
                aggregated_monthly AS(
                    -- 2. Roll up from NCM to SH6, create tons measurement
                    SELECT
                        CAST(ff.co_ano AS INTEGER) AS year, 
                        CAST(ff.co_mes AS INTEGER) AS month,
                        ff.co_pais,
                        dim_ncm.co_sh6,
                        SUM(ff.vl_fob) AS total_value_usd,
                        SUM(ff.kg_liquido) AS total_weight_kg,
                        SUM(ff.kg_liquido)/1000.0 AS total_weight_ton
                    FROM filtered_facts ff
                    LEFT JOIN '{self.silver_dir}/dim_ncm.parquet' AS dim_ncm
                        ON ff.co_ncm = dim_ncm.co_ncm
                    GROUP BY 1, 2, 3, 4
                ),
                cy_py_joined AS (
                    -- 3. Self-Join
                    -- Use FULL OUTER JOIN to catch months where a product was sold in PY but not in CY (and vice-versa)
                    SELECT
                        COALESCE(cy.year, py.year + 1) AS year,
                        COALESCE(cy.month, py.month) AS month,
                        COALESCE(cy.co_pais, py.co_pais) AS co_pais,
                        COALESCE(cy.co_sh6, py.co_sh6) AS co_sh6,

                        -- Current Year Metrics (If null, no exports in this year)
                        COALESCE(cy.total_value_usd, 0) AS cy_value_usd,
                        COALESCE(cy.total_weight_kg, 0) AS cy_weight_kg,
                        COALESCE(cy.total_weight_ton, 0) AS cy_weight_ton,

                        -- Previous Year Metrics
                        COALESCE(py.total_value_usd, 0) AS py_value_usd,
                        COALESCE(py.total_weight_kg, 0) AS py_weight_kg,
                        COALESCE(py.total_weight_ton, 0) AS py_weight_ton,
                    FROM aggregated_monthly cy
                    FULL OUTER JOIN aggregated_monthly py
                        ON cy.co_pais = py.co_pais
                        AND cy.co_sh6 = py.co_sh6
                        AND cy.month = py.month
                        AND cy.year = py.year + 1
                )
                -- 4. Bring in English nomenclature, create date, and format for Tableau
                SELECT
                    make_date(j.year, j.month, 1) AS date,
                    j.co_pais,
                    p.no_pais_ing AS country_name,
                    sh.co_sh2,
                    sh.no_sh2_ing AS sh2_description,
                    sh.co_sh4,
                    sh.no_sh4_ing AS sh4_description,
                    j.co_sh6,
                    sh.no_sh6_ing AS sh6_description,
                    j.cy_value_usd,
                    j.py_value_usd,
                    j.cy_weight_kg,
                    j.py_weight_kg,
                    j.cy_weight_ton,
                    j.py_weight_ton
                FROM cy_py_joined j
                LEFT JOIN '{self.silver_dir}/dim_pais.parquet' AS p
                    ON j.co_pais = p.co_pais
                LEFT JOIN '{self.silver_dir}/dim_ncm_sh.parquet' AS sh
                    ON j.co_sh6 = sh.co_sh6
                WHERE j.year >= 2019
                ORDER BY date DESC, j.cy_value_usd DESC
                ) TO '{output_csv}' (HEADER, DELIMITER ',');
        """

        try:
            print("Preparing Gold Layer...")
            self.con.execute(query)
            print(f"Success! Gold file ready for Tableau: {output_csv}")
        except Exception as e:
            print(f"Failed to build Gold Layer: {e}")

if __name__ == "__main__":
        builder = GoldLayerBuilder(target_block_code=53)
        builder.build_executive_dashboard_view()