import duckdb
import pandas as pd
import warnings
import os
from dotenv import load_dotenv

# Import all your scrapers
from Appearance_rate import HonkaiStatistics
from Appearance_rate_Pure_fiction import HonkaiStatistics_Pure
from Appearance_rate_Apocalytic_Shadow import HonkaiStatistics_APOC
from Appearance_rate_anomaly import HonkaiStatistics_Anomaly

# Load the .env file
load_dotenv()
warnings.filterwarnings("ignore", category=RuntimeWarning)

class HonkaiArchetypeWarehouse:
    def __init__(self, db_name="honkai_star_rail_stats2.duckdb"):
        self.db_name = db_name
        
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
        """Standardizes column names for DuckDB/SQL compatibility."""
        df['version'] = version
        df['mode'] = mode
        df['floor'] = floor
        df['eidolon_level'] = eidolon
        if mode == "ANOMALY":
            df['node'] = "N/A" # Force N/A for single-mode anomaly
        else:
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
        
        # Ensure Archetype/Team columns are strings to prevent DuckDB type mismatch
        string_targets = [c for c in df.columns if 'Archetype' in c or 'Team' in c]
        for col in string_targets:
            df[col] = df[col].astype(str)
            
        return df

    def run_dual(self, target_mode=None, target_version=None, eidolons=[0, 1, 2, 6]):
        """Runs the Dual-Archetype (Combined Sides) pipeline."""
        modes_to_run = [target_mode] if target_mode else self.config.keys()
        conn = duckdb.connect(self.db_name)

        for mode in modes_to_run:
            cfg = self.config[mode]
            versions = [target_version] if target_version else cfg["versions"]
            
            print(f"\n>>> Processing DUAL Archetype Stats for {mode}...")

            for v in versions:
                for e in eidolons:
                    try:
                        scraper = cfg["class"](version=v, floor=cfg["default_floor"], by_ed=e)
                        df = scraper.print_archetypes_both_sides(output=False)

                        if df is not None and not df.empty:
                            # Drop statistical noise
                            df = df.drop(columns=[c for c in ['Skewness', 'Kurtosis'] if c in df.columns], errors='ignore')
                            
                            df_clean = self._standardize(df, mode, v, e, cfg["default_floor"])
                            
                            target_table = cfg['dual_table']
                            if self._table_exists(conn, target_table):
                                conn.execute(f"INSERT INTO {target_table} SELECT * FROM df_clean")
                            else:
                                conn.execute(f"CREATE TABLE {target_table} AS SELECT * FROM df_clean")
                            
                            print(f"Added Dual Arch: {mode} | Ver {v} | E{e}")
                        
                    except Exception as ex:
                        print(f"Error at Dual {mode} {v} E{e}: {ex}")
                
                conn.commit()
        conn.close()

    def run(self, target_mode=None, target_version=None, eidolons=[0, 1, 2, 6]):
        """Runs the Single-Node Archetype pipeline."""
        modes_to_run = [target_mode] if target_mode else self.config.keys()
        conn = duckdb.connect(self.db_name)

        for mode in modes_to_run:
            cfg = self.config[mode]
            versions = [target_version] if target_version else cfg["versions"]
            
            print(f"\n>>> Processing SINGLE Archetype Stats for {mode}...")

            for v in versions:
                for e in eidolons:
                    # Anomaly uses floors 0-4, others use nodes 0-2
                    sub_loops = [0, 1, 2, 3, 4] if mode == "ANOMALY" else [0, 1, 2]

                    for val in sub_loops:
                        try:
                            if mode == "ANOMALY":
                                scraper = cfg["class"](version=v, floor=val, by_ed=e)
                                current_floor, current_node = val, None
                            else:
                                scraper = cfg["class"](version=v, floor=cfg["default_floor"], by_ed=e, node=val)
                                current_floor, current_node = cfg["default_floor"], val

                            # Target the single-archetype method
                            df = scraper.print_archetypes(output=False)

                            if df is not None and not df.empty:
                                df = df.drop(columns=[c for c in ['Skewness', 'Kurtosis'] if c in df.columns], errors='ignore')
                                df_clean = self._standardize(df, mode, v, e, current_floor, current_node)
                                
                                target_table = cfg['table']
                                if self._table_exists(conn, target_table):
                                    conn.execute(f"INSERT INTO {target_table} SELECT * FROM df_clean")
                                else:
                                    conn.execute(f"CREATE TABLE {target_table} AS SELECT * FROM df_clean")
                                
                                print(f"Added Arch: {mode} | Ver {v} | E{e} | Node/Floor {val}")
                        
                        except Exception as ex:
                            print(f"Error at {mode} {v} E{e} Node {val}: {ex}")
                
                conn.commit()
        conn.close()

    def _table_exists(self, conn, table_name):
        """Helper to check if table exists in DuckDB."""
        return conn.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()[0] > 0

if __name__ == "__main__":
    pipeline = HonkaiArchetypeWarehouse()
    
    # Choose which to run:
    # pipeline.run()       # Standard Side 1 / Side 2 / Both nodes
    pipeline.run_dual()    # Combined Both Sides only