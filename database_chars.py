import duckdb
import pandas as pd
import warnings
import os
import requests
from dotenv import load_dotenv

# Import all scrapers
from Appearance_rate import HonkaiStatistics
from Appearance_rate_Pure_fiction import HonkaiStatistics_Pure
from Appearance_rate_Apocalytic_Shadow import HonkaiStatistics_APOC
from Appearance_rate_anomaly import HonkaiStatistics_Anomaly

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
            "MOC": {"class": HonkaiStatistics, "versions": get_env_list("MOC_VERSIONS"), "default_floor": 12, "has_node": True},
            "PURE_FICTION": {"class": HonkaiStatistics_Pure, "versions": get_env_list("PF_VERSIONS"), "default_floor": 4, "has_node": True},
            "APOC": {"class": HonkaiStatistics_APOC, "versions": get_env_list("APOC_VERSIONS"), "default_floor": 4, "has_node": True},
            "ANOMALY": {"class": HonkaiStatistics_Anomaly, "versions": get_env_list("ANOMALY_VERSIONS"), "default_floor": None, "has_node": False}
        }
        
        self.char_map = self._fetch_character_metadata()

    def _fetch_character_metadata(self):
        """Fetches and processes character metadata from GitHub."""
        try:
            print("Fetching character metadata for enrichment...")
            url = "https://raw.githubusercontent.com/LvlUrArti/MocStats/main/data/characters.json"
            response = requests.get(url)
            json_data = response.json()
            
            char_map = {}
            for name, info in json_data.items():
                meta = {k: v for k, v in info.items() if k != 'slug'}
                if 'role' in meta and isinstance(meta['role'], list):
                    meta['role'] = ", ".join(meta['role'])
                char_map[name] = meta
            return char_map
        except Exception as e:
            print(f"Warning: Metadata enrichment failed to load: {e}")
            return {}

    def _standardize(self, df, mode, version, eidolon, floor, node=None):
        """Standardizes stats and enriches with Character Metadata."""
        # 1. Basic Metadata
        df['version'] = version
        df['mode'] = mode
        df['floor'] = floor
        df['eidolon_level'] = eidolon
        
        # 2. Node Handling (0, 1, 2)
        if mode == "ANOMALY":
            df['node'] = None
        else:
            # Ensures node is 0, 1, or 2 (as passed from the loop)
            df['node'] = str(node) if node is not None else "0"

        # 3. Enrich from JSON
        if self.char_map:
            sample_meta = next(iter(self.char_map.values()))
            for meta_key in sample_meta.keys():
                df[meta_key] = df['Character'].map(lambda x: self.char_map.get(x, {}).get(meta_key))

        # 4. Rename and Clean
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
        df = df.drop(columns=[c for c in ['Skewness', 'Kurtosis'] if c in df.columns], errors='ignore')

        # SQL compatible headers
        df.columns = [c.replace(' ', '_').replace('(', '').replace(')', '').replace('%', 'pct').replace('•', '').strip() for c in df.columns]
        
        return df

    def run(self, target_mode=None, target_version=None, eidolons=[0, 1, 2, 6]):
        conn = duckdb.connect(self.db_name)
        modes_to_run = [target_mode] if target_mode else list(self.config.keys())

        # Define the EXACT order of the 32 columns based on your SQL schema
        target_columns = [
            "Rank", "Character", "Appearance_Rate_pct", "Samples", "Min_Score",
            "Percentile_25", "Median_Score", "Percentile_75", "Average_Score",
            "Std_Dev", "Max_Score", "Sustain_Samples", "Sustain_Percentage",
            "Eidolon_0_pct", "Eidolon_1_pct", "Eidolon_2_pct", "Eidolon_3_pct",
            "Eidolon_4_pct", "Eidolon_5_pct", "Eidolon_6_pct", "version", "mode",
            "floor", "eidolon_level", "node", "id", "rarity", "path",
            "element", "availability", "release_phase", "role"
        ]

        for mode in modes_to_run:
            cfg = self.config[mode]
            versions = [target_version] if target_version else cfg["versions"]
            print(f"\n>>> Running Character Pipeline: {mode}")

            for v in versions:
                floors = [0, 1, 2, 3, 4] if mode == "ANOMALY" else [cfg["default_floor"]]
                for f in floors:
                    for e in eidolons:
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
                                    
                                    # FORCED REORDERING: Ensures 32 columns in the right order
                                    for col in target_columns:
                                        if col not in df_clean.columns:
                                            df_clean[col] = None
                                    
                                    df_final = df_clean[target_columns]

                                    # INSERTION
                                    try:
                                        # Using a view avoids 'BY NAME' parser errors
                                        conn.register('temp_df', df_final)
                                        conn.execute("INSERT INTO character_stats SELECT * FROM temp_df")
                                        conn.unregister('temp_df')
                                    except duckdb.CatalogException:
                                        conn.execute("CREATE TABLE character_stats AS SELECT * FROM df_final")
                                        print("Created Table: character_stats")

                                    print(f"Success: {mode} | {v} | E{e} | F{f} | N{n}")
                                
                            except Exception as ex:
                                print(f"Error at {mode} {v} E{e} F{f}: {ex}")

        conn.close()
        print(f"\nPipeline Finished. Database: {self.db_name}")

if __name__ == "__main__":
    warehouse = HonkaiCharacterWarehouse()
    warehouse.run(target_version="4.1.1",target_mode="ANOMALY")