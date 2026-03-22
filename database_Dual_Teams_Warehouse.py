import duckdb
import pandas as pd
import warnings

# Import all your scrapers
from Appearance_rate import HonkaiStatistics
from Appearance_rate_Pure_fiction import HonkaiStatistics_Pure
from Appearance_rate_Apocalytic_Shadow import HonkaiStatistics_APOC
from Appearance_rate_anomaly import HonkaiStatistics_Anomaly

warnings.filterwarnings("ignore", category=RuntimeWarning)

class HonkaiTeamWarehouse:
    def __init__(self, db_name="honkai_star_rail_stats.duckdb"):
        self.db_name = db_name
        # Configuration for all game modes
        self.config = {
            "MOC": {
                "class": HonkaiStatistics,
                "table": "moc_stats_teams",
                "dual_table": "moc_stats_dual_teams",
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
                "dual_table": "pure_fiction_stats_dual_teams",
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
                "dual_table": "apoc_stats_dual_teams",
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
                "dual_table": "anomaly_stats_triple_teams",
                "versions": ["3.6.3", "3.7.3", "3.8.4", "4.0.2"],
                "default_floor": 0
            }
        }

    def _standardize(self, df, mode, version, eidolon, floor, node=None):
        """Standardizes column names for DuckDB/SQL compatibility."""
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
            'Std Dev Cycles': 'Std_Dev',
            '25th Percentile Cycles': 'Percentile_25',
            '25th Percentile': 'Percentile_25',
            'Median Cycles': 'Median_Score',
            'Median Score': 'Median_Score',
            '75th Percentile Cycles': 'Percentile_75',
            '75th Percentile': 'Percentile_75'
        }
        df.rename(columns=rename_map, inplace=True)
        
        # Clean column names for DuckDB
        df.columns = [c.replace(' ', '_').replace('(', '').replace(')', '').replace('%', 'pct').replace('•', '') for c in df.columns]
        
        # Ensure Team columns are strings
        team_cols = [c for c in df.columns if 'Team' in c]
        for col in team_cols:
            df[col] = df[col].astype(str)
            
        return df

    def run_dual(self, target_mode=None, target_version=None, eidolons=[0, 1, 2, 6]):
        """Runs the Dual-Team (Both Sides) pipeline using DuckDB."""
        modes_to_run = [target_mode] if target_mode else self.config.keys()
        
        # Connect to DuckDB
        conn = duckdb.connect(self.db_name)

        for mode in modes_to_run:
            cfg = self.config[mode]
            versions = [target_version] if target_version else cfg["versions"]
            
            print(f"\n>>> Processing Dual-Team Stats for {mode}...")

            for v in versions:
                for e in eidolons:
                    try:
                        scraper = cfg["class"](version=v, floor=cfg["default_floor"], by_ed=e)
                        df = scraper.print_appearance_rates_both_sides(output=False)

                        if df is not None and not df.empty:
                            df_clean = self._standardize(df, mode, v, e, cfg["default_floor"])
                            # DuckDB can register the dataframe and insert directly
                            conn.execute(f"INSERT INTO {cfg['dual_table']} SELECT * FROM df_clean") if self._table_exists(conn, cfg['dual_table']) else conn.execute(f"CREATE TABLE {cfg['dual_table']} AS SELECT * FROM df_clean")
                            
                            print(f"Added Dual: {mode} | Ver {v} | E{e}")
                        
                    except Exception as ex:
                        print(f"Error at Dual {mode} {v} E{e}: {ex}")
                
                # Commit after each version to ensure visibility in viewers
                conn.commit()

        conn.close()
        print("\nDual Team Pipeline Complete.")

    def run(self, target_mode=None, target_version=None, eidolons=[0, 1, 2, 6]):
        """Runs the standard single-node team pipeline using DuckDB."""
        modes_to_run = [target_mode] if target_mode else self.config.keys()
        conn = duckdb.connect(self.db_name)

        for mode in modes_to_run:
            cfg = self.config[mode]
            versions = [target_version] if target_version else cfg["versions"]
            
            print(f"\n>>> Processing Single-Node Team Stats for {mode}...")

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

                            df = scraper.print_appearance_rates(output=False)

                            if df is not None and not df.empty:
                                df_clean = self._standardize(df, mode, v, e, current_floor, current_node)
                                # Efficient DuckDB insertion
                                if self._table_exists(conn, cfg['table']):
                                    conn.execute(f"INSERT INTO {cfg['table']} SELECT * FROM df_clean")
                                else:
                                    conn.execute(f"CREATE TABLE {cfg['table']} AS SELECT * FROM df_clean")
                                
                                print(f"Added: {mode} | Ver {v} | E{e} | Node/Floor {val}")
                        
                        except Exception as ex:
                            print(f"Error at {mode} {v} E{e}: {ex}")
                
                conn.commit()

        conn.close()

    def _table_exists(self, conn, table_name):
        """Helper to check if table exists in DuckDB."""
        return conn.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()[0] > 0

if __name__ == "__main__":
    pipeline = HonkaiTeamWarehouse()
    # pipeline.run()
    pipeline.run_dual()