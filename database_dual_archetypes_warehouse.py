import duckdb
import pandas as pd
import warnings

# Import all your scrapers
from Appearance_rate import HonkaiStatistics
from Appearance_rate_Pure_fiction import HonkaiStatistics_Pure
from Appearance_rate_Apocalytic_Shadow import HonkaiStatistics_APOC
from Appearance_rate_anomaly import HonkaiStatistics_Anomaly
import os
from dotenv import load_dotenv
# Load the .env file
load_dotenv()
warnings.filterwarnings("ignore", category=RuntimeWarning)

class HonkaiDualArchetypeWarehouse:
    def __init__(self, db_name="honkai_star_rail_stats2.duckdb"):
        self.db_name = db_name
        # Configuration for Archetype-specific tables
        
        # Helper to get lists from env
        def get_env_list(key):
            val = os.getenv(key)
            return val.split(",") if val else []
        
        
        self.config = {
            "MOC": {
                "class": HonkaiStatistics,
                "table": "moc_stats_archetypes",
                "dual_table": "moc_stats_dual_archetypes",
                "versions": get_env_list("MOC_VERSIONS"),
                "default_floor": 12
            },
            "PURE_FICTION": {
                "class": HonkaiStatistics_Pure,
                "table": "pure_fiction_stats_archetypes",
                "dual_table": "pure_fiction_stats_dual_archetypes",
                "versions": get_env_list("PF_VERSIONS"),
                "default_floor": 4
            },
            "APOC": {
                "class": HonkaiStatistics_APOC,
                "table": "apoc_stats_archetypes",
                "dual_table": "apoc_stats_dual_archetypes",
                "versions": get_env_list("APOC_VERSIONS"),
                "default_floor": 4
            },
            "ANOMALY": {
                "class": HonkaiStatistics_Anomaly,
                "table": "anomaly_stats_archetypes",
                "dual_table": "anomaly_stats_dual_archetypes",
                "versions": get_env_list("ANOMALY_VERSIONS"),
                "default_floor": 0
            }
        }

    def _standardize(self, df, mode, version, eidolon, floor, node=None):
        """Standardizes column names for Archetype data."""
        df['version'] = version
        df['mode'] = mode
        df['floor'] = floor
        df['eidolon_level'] = eidolon
        df['node'] = node if node is not None else "Both"

        rename_map = {
            'Appearance Rate (%)': 'Appearance_Rate_pct',
            'Average Cycles': 'Average_Score',
            'Average Score': 'Average_Score',
            'Min Cycles': 'Min_Score',
            'Max Cycles': 'Max_Score',
            'Min Score': 'Min_Score',
            'Max Score': 'Max_Score',
            'Std Dev Cycles': 'Std_Dev',
            'Std Dev': 'Std_Dev',
            '25th Percentile': 'Percentile_25',
            'Median Score': 'Median_Score',
            'Median Cycles': 'Median_Score',
            '75th Percentile': 'Percentile_75'
        }
        df.rename(columns=rename_map, inplace=True)
        
        # Clean column names for DuckDB
        df.columns = [c.replace(' ', '_').replace('(', '').replace(')', '').replace('%', 'pct').replace('•', '') for c in df.columns]
        
        # Ensure Archetype columns are strings
        arch_cols = [c for c in df.columns if 'Archetype' in c]
        for col in arch_cols:
            df[col] = df[col].astype(str)
            
        return df

    def run_dual(self, target_mode=None, target_version=None, eidolons=[0, 1, 2, 6]):
        """Runs the Dual-Archetype (Both Sides) pipeline."""
        modes_to_run = [target_mode] if target_mode else self.config.keys()
        conn = duckdb.connect(self.db_name)

        for mode in modes_to_run:
            cfg = self.config[mode]
            versions = [target_version] if target_version else cfg["versions"]
            
            print(f"\n>>> Processing Dual-Archetype Stats for {mode}...")

            for v in versions:
                for e in eidolons:
                    try:
                        scraper = cfg["class"](version=v, floor=cfg["default_floor"], by_ed=e)
                        
                        # Swap to the dual-archetype method
                        df = scraper.print_dual_archetypes(output=False)

                        if df is not None and not df.empty:
                            # Explicitly drop skewness and kurtosis if they exist
                            df = df.drop(columns=[c for c in ['Skewness', 'Kurtosis'] if c in df.columns])
                            
                            df_clean = self._standardize(df, mode, v, e, cfg["default_floor"])
                            
                            table_name = cfg['dual_table']
                            if self._table_exists(conn, table_name):
                                conn.execute(f"INSERT INTO {table_name} SELECT * FROM df_clean")
                            else:
                                conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df_clean")
                            
                            print(f"Added Dual Arch: {mode} | Ver {v} | E{e}")
                        
                    except Exception as ex:
                        print(f"Error at Dual {mode} {v} E{e}: {ex}")
                
                conn.commit()

        conn.close()
        print("\nDual Archetype Pipeline Complete.")

    def _table_exists(self, conn, table_name):
        return conn.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()[0] > 0

if __name__ == "__main__":
    pipeline = HonkaiDualArchetypeWarehouse()
    pipeline.run_dual()