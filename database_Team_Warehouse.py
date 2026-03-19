import sqlite3
import pandas as pd
import warnings

# Import all your scrapers
from Appearance_rate import HonkaiStatistics
from Appearance_rate_Pure_fiction import HonkaiStatistics_Pure
from Appearance_rate_Apocalytic_Shadow import HonkaiStatistics_APOC
from Appearance_rate_anomaly import HonkaiStatistics_Anomaly

warnings.filterwarnings("ignore", category=RuntimeWarning)

class HonkaiTeamWarehouse:
    def __init__(self, db_name="honkai_star_rail_stats2.db"):
        self.db_name = db_name
        # Configuration for all game modes
        self.config = {
            "MOC": {
                "class": HonkaiStatistics,
                "table": "moc_stats_teams",
                "versions": [
        "2.3.3", "2.4.3", "2.5.3", "2.6.3", "2.7.3", 
        "3.1.2", "3.2.2", "3.3.2", "3.4.2", "3.5.2", 
        "3.6.2", "3.7.2", "3.8.2", "4.0.1", "4.0.2"
    ],
                "default_floor": 12
            },
            "PURE_FICTION": {
                "class": HonkaiStatistics_Pure,
                "table": "pure_fiction_stats_teams",
                "versions": [
        "2.3.2", "2.4.2", "2.5.2", "2.6.2", "2.7.2", 
        "3.1.1", "3.2.1", "3.3.1", "3.4.1", "3.5.1", 
        "3.6.1", "3.7.1", "3.8.1", "3.8.4", "4.0.2"
    ],
                "default_floor": 4
            },
            "APOC": {
                "class": HonkaiStatistics_APOC,
                "table": "apoc_stats_teams",
                "versions": [
        "2.3.1", "2.4.1", "2.5.1", "2.6.1", "2.7.1", 
        "3.0.3", "3.1.3", "3.2.3", "3.3.3", "3.4.3", 
        "3.5.3", "3.6.3", "3.7.3", "3.8.3", "4.0.2"
    ],
                "default_floor": 4
            },
            "ANOMALY": {
                "class": HonkaiStatistics_Anomaly,
                "table": "anomaly_stats_teams",
                "versions": ["3.6.3", "3.7.3", "3.8.4", "4.0.2"],
                "default_floor": 0
            }
        }

    def _standardize(self, df, mode, version, eidolon, floor, node):
        """Standardizes column names for SQL compatibility."""
        df['version'] = version
        df['mode'] = mode
        df['floor'] = floor
        df['eidolon_level'] = eidolon
        if node is not None: df['node'] = node

        # Rename MOC/Anomaly Cycles or APOC/PF Scores to a unified format
        rename_map = {
            'Appearance Rate (%)': 'Appearance_Rate_pct',
            'Average Cycles': 'Average_Score',
            'Average Score': 'Average_Score',
            'Min Cycles': 'Min_Score',
            'Max Cycles': 'Max_Score',
            'Std Dev Cycles': 'Std_Dev',
            '25th Percentile Cycles': 'Percentile_25',
            'Median Cycles': 'Median_Score',
            '75th Percentile Cycles': 'Percentile_75'
        }
        df.rename(columns=rename_map, inplace=True)
        
        # Clean special characters from headers
        df.columns = [c.replace(' ', '_').replace('(', '').replace(')', '').replace('%', 'pct').replace('•', '') for c in df.columns]
        
        # Ensure the 'Team' tuple is saved as a string
        if 'Team' in df.columns:
            df['Team'] = df['Team'].astype(str)
            
        return df

    def run(self, target_mode=None, target_version=None, eidolons=[0, 1, 2, 6]):
        """
        Runs the team appearance rate pipeline.
        - target_mode: Select "MOC", "PURE_FICTION", etc. (None runs all).
        - target_version: Select a specific patch (None runs all).
        """
        modes_to_run = [target_mode] if target_mode else self.config.keys()
        conn = sqlite3.connect(self.db_name)

        for mode in modes_to_run:
            cfg = self.config[mode]
            versions = [target_version] if target_version else cfg["versions"]
            
            print(f"\n>>> Processing Team Stats for {mode}...")

            for v in versions:
                for e in eidolons:
                    # Anomaly uses floors 0-4; others use nodes 0-2
                    if mode == "ANOMALY":
                        sub_loops = [0, 1, 2, 3, 4] # Anomaly Floors
                    else:
                        sub_loops = [0, 1, 2] # Nodes

                    for val in sub_loops:
                        try:
                            # Dynamic init based on mode
                            if mode == "ANOMALY":
                                scraper = cfg["class"](version=v, floor=val, by_ed=e)
                                current_floor = val
                                current_node = None
                            else:
                                scraper = cfg["class"](version=v, floor=cfg["default_floor"], by_ed=e, node=val)
                                current_floor = cfg["default_floor"]
                                current_node = val

                            # Use the TEAM specific method
                            df = scraper.print_appearance_rates(output=False)

                            if df is not None and not df.empty:
                                df_clean = self._standardize(df, mode, v, e, current_floor, current_node)
                                df_clean.to_sql(cfg["table"], conn, if_exists='append', index=False)
                                print(f"Added: {mode} | Ver {v} | E{e} | {'Floor' if mode=='ANOMALY' else 'Node'} {val}")
                        
                        except Exception as ex:
                            print(f"Error at {mode} {v} E{e}: {ex}")

        conn.close()
        print("\nTeam Pipeline Complete.")

# --- EXECUTION ---
if __name__ == "__main__":
    pipeline = HonkaiTeamWarehouse()
    # Example: Run just for MOC version 4.0.2
    pipeline.run()