import duckdb
import pandas as pd
import warnings
import os
import orjson
from dotenv import load_dotenv

# Scraper Imports
from Appearance_rate import HonkaiStatistics
from Appearance_rate_Pure_fiction import HonkaiStatistics_Pure
from Appearance_rate_Apocalytic_Shadow import HonkaiStatistics_APOC
from Appearance_rate_anomaly import HonkaiStatistics_Anomaly

load_dotenv()
warnings.filterwarnings("ignore", category=RuntimeWarning)

class HonkaiDataPlatform:
    def __init__(self, db_name=os.getenv("DB_File")):
        self.db_name = db_name
        self.char_map = self._fetch_character_metadata()
        
        def get_env_list(key):
            val = os.getenv(key)
            return val.split(",") if val else []

        # Mapping configuration
        self.config = {
            "MOC": {"class": HonkaiStatistics, "prefix": "moc", "versions": get_env_list("MOC_VERSIONS"), "floor": 12, "has_node": True},
            "PURE_FICTION": {"class": HonkaiStatistics_Pure, "prefix": "pure_fiction", "versions": get_env_list("PF_VERSIONS"), "floor": 4, "has_node": True},
            "APOC": {"class": HonkaiStatistics_APOC, "prefix": "apoc", "versions": get_env_list("APOC_VERSIONS"), "floor": 4, "has_node": True},
            "ANOMALY": {"class": HonkaiStatistics_Anomaly, "prefix": "anomaly", "versions": get_env_list("ANOMALY_VERSIONS"), "floor": 0, "has_node": False}
        }

    def _fetch_character_metadata(self):
        try:
            with open('characters.json', 'rb') as f:
                json_data = orjson.loads(f.read())
            return {name: {k: (", ".join(v) if isinstance(v, list) else v) for k, v in info.items() if k != 'slug'} 
                    for name, info in json_data.items()}
        except: return {}

    def _standardize(self, df, mode, v, e, f, n, is_char=False):
        # Check if df is NOT a dataframe (e.g., if it's a string error message)
        if not isinstance(df, pd.DataFrame):
            return None
            
        # Now it is safe to check .empty
        if df.empty: 
            return None
        df = df.copy()
        df['version'], df['mode'], df['floor'], df['eidolon_level'] = v, mode, f, e
        df['node'] = "N/A" if mode == "ANOMALY" else str(n)

        # Metadata Enrichment
        if is_char and self.char_map:
            for k in next(iter(self.char_map.values())).keys():
                df[k] = df['Character'].map(lambda x: self.char_map.get(x, {}).get(k))

        # Column cleanup
        df.columns = [c.replace(' ', '_').replace('(', '').replace(')', '').replace('%', 'pct').strip() for c in df.columns]
        return df.drop(columns=['Skewness', 'Kurtosis'], errors='ignore')

    def _db_save(self, conn, df, table):
        if df is None: return
        conn.register('temp_df', df)
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM temp_df WHERE 1=0")
        conn.execute(f"INSERT INTO {table} SELECT * FROM temp_df")
        conn.unregister('temp_df')

    def orchestrate_update(self, target_mode=None, target_version=None, eidolons=[0, 1, 2, 6]):
        """Main entry point: One instance per loop, multiple data extractions."""
        conn = duckdb.connect(self.db_name)
        modes = [target_mode] if target_mode else self.config.keys()

        for mode in modes:
            cfg = self.config[mode]
            versions = [target_version] if target_version else cfg["versions"]
            
            for v in versions:
                for e in eidolons:
                    # Anomaly floors 0-4, others use default floor
                    floors = [0, 1, 2, 3, 4] if mode == "ANOMALY" else [cfg["floor"]]
                    for f in floors:
                        # Standard Nodes (0, 1, 2)
                        nodes = [0, 1, 2] if cfg["has_node"] else [None]
                        for n in nodes:
                            print(f"Updating {mode} {v} E{e} Floor{f} Node{n}...")
                            
                            # CREATE SINGLE INSTANCE
                            scraper = cfg["class"](version=v, floor=f, by_ed=e, node=n) if cfg["has_node"] else cfg["class"](version=v, floor=f, by_ed=e)

                            # 1. Characters
                            df_char = self._standardize(scraper.print_appearance_rate_by_char(output=False), mode, v, e, f, n, is_char=True)
                            self._db_save(conn, df_char, "character_stats")

                            # 2. Archetypes
                            df_arch = self._standardize(scraper.print_archetypes(output=False), mode, v, e, f, n)
                            self._db_save(conn, df_arch, f"{cfg['prefix']}_stats_archetypes")

                            # 3. Teams
                            df_team = self._standardize(scraper.print_appearance_rates(output=False), mode, v, e, f, n)
                            self._db_save(conn, df_team, f"{cfg['prefix']}_stats_teams")

                            # Handle "Both Sides" (Dual) 
                            if n ==0:
                                print(f"Updating Dual Stats for {mode} {v} E{e}...")
                                
                                
                                df_dual_arch = self._standardize(scraper.print_archetypes_both_sides(output=False), mode, v, e, f, "Both")
                                self._db_save(conn, df_dual_arch, f"{cfg['prefix']}_stats_dual_archetypes")

                                df_dual_team = self._standardize(scraper.print_appearance_rates_both_sides(output=False), mode, v, e, f, "Both")
                                self._db_save(conn, df_dual_team, f"{cfg['prefix']}_stats_dual_teams")
                                
                            if f==0 and mode == "ANOMALY":
                                print(f"Updating Dual Stats for {mode} {v} E{e}...")
                                df_dual_arch = self._standardize(scraper.print_archetypes_both_sides(output=False), mode, v, e, f, n)
                                self._db_save(conn, df_dual_arch, f"{cfg['prefix']}_stats_triple_archetypes")

                                df_dual_team = self._standardize(scraper.print_appearance_rates_both_sides(output=False), mode, v, e, f, n)
                                self._db_save(conn, df_dual_team, f"{cfg['prefix']}_stats_triple_teams")
                
                conn.commit()
        conn.close()

if __name__ == "__main__":
    platform = HonkaiDataPlatform()
    # Runs everything for a specific version
    platform.orchestrate_update(target_version="4.1.1")