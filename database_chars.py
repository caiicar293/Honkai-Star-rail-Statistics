import duckdb
import pandas as pd
import warnings

# Import all scrapers
from Appearance_rate import HonkaiStatistics
from Appearance_rate_Pure_fiction import HonkaiStatistics_Pure
from Appearance_rate_Apocalytic_Shadow import HonkaiStatistics_APOC
from Appearance_rate_anomaly import HonkaiStatistics_Anomaly
import os
from dotenv import load_dotenv
# Load the .env file
load_dotenv()
warnings.filterwarnings("ignore", category=RuntimeWarning)

class HonkaiCharacterWarehouse:
    def __init__(self, db_name="honkai_star_rail_stats2.duckdb"):
        self.db_name = db_name
        
        
        # Helper to get lists from env
        def get_env_list(key):
            val = os.getenv(key)
            return val.split(",") if val else []
        
        
        self.config = {
            "MOC": {
                "class": HonkaiStatistics,
                "versions": get_env_list("MOC_VERSIONS"),
                "default_floor": 12,
                "has_node": True
            },
            "PURE_FICTION": {
                "class": HonkaiStatistics_Pure,
                "versions":  get_env_list("PF_VERSIONS"),
                "default_floor": 4,
                "has_node": True
            },
            "APOC": {
                "class": HonkaiStatistics_APOC,
                "versions": get_env_list("APOC_VERSIONS"),
                "default_floor": 4,
                "has_node": True
            },
            "ANOMALY": {
                "class": HonkaiStatistics_Anomaly,
                "versions": get_env_list("ANOMALY_VERSIONS"),
                "default_floor": None, 
                "has_node": False
            }
        }

    def _standardize(self, df, mode, version, eidolon, floor, node=None):
        """Standardizes character stats and strips unwanted metrics."""
        df['version'] = version
        df['mode'] = mode
        df['floor'] = floor
        df['eidolon_level'] = eidolon
        # --- ANOMALY NODE HANDLING ---
        if mode == "ANOMALY":
            # For Anomaly, we explicitly set this to N/A or None to avoid 
            # confusing it with side-specific data.
            df['node'] = "N/A"

        rename_map = {
            'Appearance Rate (%)': 'Appearance_Rate_pct',
            'Average Cycles': 'Average_Score',
            'Average Score': 'Average_Score',
            'Min Cycles': 'Min_Score',
            'Min Score': 'Min_Score',
            'Max Cycles': 'Max_Score',
            'Max Score': 'Max_Score',
            'Std Dev Cycles': 'Std_Dev',
            'Std Dev Score': 'Std_Dev',
            '25th Percentile Cycles': 'Percentile_25',
            '25th Percentile': 'Percentile_25',
            'Median Cycles': 'Median_Score',
            'Median Score': 'Median_Score',
            '75th Percentile Cycles': 'Percentile_75',
            '75th Percentile': 'Percentile_75'
        }
        df.rename(columns=rename_map, inplace=True)
        
        # Explicitly remove Skewness and Kurtosis
        df = df.drop(columns=[c for c in ['Skewness', 'Kurtosis'] if c in df.columns], errors='ignore')

        # SQL compatible headers
        df.columns = [c.replace(' ', '_').replace('(', '').replace(')', '').replace('%', 'pct').replace('•', '').strip() for c in df.columns]
        
        return df

    def run(self, target_mode=None, target_version=None, eidolons=[0, 1, 2, 6]):
        """Executes character pipeline with optional mode and version targeting."""
        conn = duckdb.connect(self.db_name)
        modes_to_run = [target_mode] if target_mode else self.config.keys()

        for mode in modes_to_run:
            cfg = self.config[mode]
            versions = [target_version] if target_version else cfg["versions"]
            
            print(f"\n>>> Running Character Pipeline: {mode}")

            for v in versions:
                # Anomaly iterates floors 0-4; others use the default endgame floor (12 or 4)
                floors = [0, 1, 2, 3, 4] if mode == "ANOMALY" else [cfg["default_floor"]]
                
                for f in floors:
                    for e in eidolons:
                        # Logic for Node-based modes (0=Both, 1=Side1, 2=Side2)
                        nodes = [0, 1, 2] if cfg["has_node"] else [None]

                        for n in nodes:
                            try:
                                if mode == "ANOMALY":
                                    h = cfg["class"](version=v, floor=f, by_ed=e)
                                else:
                                    h = cfg["class"](version=v, floor=f, by_ed=e, node=n)

                                df = h.print_appearance_rate_by_char(output=False)

                                if df is not None and not df.empty:
                                    df_clean = self._standardize(df, mode, v, e, f, n)
                                    
                                    # Insert or Create table
                                    try:
                                        conn.execute("INSERT INTO character_stats SELECT * FROM df_clean")
                                    except duckdb.CatalogException:
                                        conn.execute("CREATE TABLE character_stats AS SELECT * FROM df_clean")
                                        print("Created Table: character_stats")

                                    print(f"Success: {mode} | {v} | E{e} | Floor {f} | Node {n}")
                                
                            except Exception as ex:
                                print(f"Error at {mode} {v} E{e} F{f}: {ex}")

        conn.close()
        print(f"\nPipeline Finished. Database: {self.db_name}")

if __name__ == "__main__":
    warehouse = HonkaiCharacterWarehouse()
    
    # Usage Examples:
    # warehouse.run() # Run everything
    # warehouse.run(target_mode="MOC", target_version="4.0.2") # Target specific
    warehouse.run()