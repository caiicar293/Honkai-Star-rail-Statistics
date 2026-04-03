import duckdb
import pandas as pd
import warnings
import os

# Import all your scrapers
from Appearance_rate import HonkaiStatistics
from Appearance_rate_Pure_fiction import HonkaiStatistics_Pure
from Appearance_rate_Apocalytic_Shadow import HonkaiStatistics_APOC
from Appearance_rate_anomaly import HonkaiStatistics_Anomaly

warnings.filterwarnings("ignore", category=RuntimeWarning)
import os
from dotenv import load_dotenv
# Load the .env file
load_dotenv()


class HonkaiArchetypeWarehouse:
    def __init__(self, db_name="honkai_star_rail_archetypes.duckdb"):
        self.db_name = db_name
        
        # Helper to get lists from env
        def get_env_list(key):
            val = os.getenv(key)
            return val.split(",") if val else []
        
        
        self.config = {
            "MOC": {
                "class": HonkaiStatistics,
                "table": "moc_stats_archetypes",
                "versions": get_env_list("MOC_VERSIONS"),
                "default_floor": 12
            },
            "PURE_FICTION": {
                "class": HonkaiStatistics_Pure,
                "table": "pure_fiction_stats_archetypes",
                "versions": get_env_list("PF_VERSIONS"),
                "default_floor": 4
            },
            "APOC": {
                "class": HonkaiStatistics_APOC,
                "table": "apoc_stats_archetypes",
                "versions": get_env_list("APOC_VERSIONS"),
                "default_floor": 4
            },
            "ANOMALY": {
                "class": HonkaiStatistics_Anomaly,
                "table": "anomaly_stats_archetypes",
                "versions": get_env_list("ANOMALY_VERSIONS"),
                "default_floor": 0
            }
        }

    def _standardize(self, df, mode, version, eidolon, floor, node):
        """Standardizes column names for Archetype data."""
        df['version'] = version
        df['mode'] = mode
        df['floor'] = floor
        df['eidolon_level'] = eidolon
        if node is not None: df['node'] = node

        # Mapping for Archetype-specific columns
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
            'Median Cycles': 'Median_Score',
            'Median Score': 'Median_Score',
            '75th Percentile': 'Percentile_75'
        }
        df.rename(columns=rename_map, inplace=True)
        
        # Clean headers (spaces to underscores, remove special chars)
        df.columns = [c.replace(' ', '_').replace('(', '').replace(')', '').replace('%', 'pct').replace('•', '') for c in df.columns]
        
        # Ensure Archetype tuple is stringified for DuckDB
        if 'Archetype' in df.columns:
            df['Archetype'] = df['Archetype'].astype(str)
            
        return df

    def run(self, target_mode=None, target_version=None, eidolons=[0, 1, 2, 6]):
        modes_to_run = [target_mode] if target_mode else self.config.keys()
        conn = duckdb.connect(self.db_name)

        for mode in modes_to_run:
            cfg = self.config[mode]
            versions = [target_version] if target_version else cfg["versions"]
            table_name = cfg["table"]
            
            print(f"\n>>> Processing Archetype Stats for {mode}...")

            for v in versions:
                for e in eidolons:
                    sub_loops = [0, 1, 2, 3, 4] if mode == "ANOMALY" else [0, 1, 2]

                    for val in sub_loops:
                        try:
                            if mode == "ANOMALY":
                                scraper = cfg["class"](version=v, floor=val, by_ed=e)
                                current_floor, current_node = val, None
                            else:
                                scraper = cfg["class"](version=v, floor=cfg["default_floor"], by_ed=e, node=val)
                                current_floor, current_node = cfg["default_floor"], val

                            # Use print_archetypes instead of appearance_rates
                            df = scraper.print_archetypes(output=False)

                            if df is not None and not df.empty:
                                # Remove Skewness/Kurtosis if they exist in the DF before saving
                                cols_to_drop = ['Skewness', 'Kurtosis']
                                df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])
                                
                                df_clean = self._standardize(df, mode, v, e, current_floor, current_node)
                                
                                # Fast insertion logic
                                try:
                                    conn.execute(f"INSERT INTO {table_name} SELECT * FROM df_clean")
                                except duckdb.CatalogException:
                                    conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df_clean")
                                    print(f"Created Table: {table_name}")
                                
                                print(f"Added Archetypes: {mode} | Ver {v} | E{e} | {'Floor' if mode=='ANOMALY' else 'Node'} {val}")
                        
                        except Exception as ex:
                            print(f"Error at {mode} {v} E{e}: {ex}")

        conn.close()
        print("\nArchetype Pipeline Complete.")

if __name__ == "__main__":
    pipeline = HonkaiArchetypeWarehouse()
    pipeline.run()