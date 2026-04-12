import duckdb
import polars as pl
import warnings
import os
import orjson
from dotenv import load_dotenv

# Scraper Imports (V2 return Polars natively)
from Appearance_rate_V2 import HonkaiStatistics_V2
from Appearance_rate_Pure_fiction_V2 import HonkaiStatistics_V2_Pure
from Appearance_rate_Apocalytic_Shadow_V2 import HonkaiStatistics_V2_APOC
from Appearance_rate_anomaly_V2 import HonkaiStatistics_Anomaly_V2

load_dotenv()
warnings.filterwarnings("ignore", category=RuntimeWarning)

class HonkaiDataPlatform:
    def __init__(self, db_name=os.getenv("DB_File")):
        self.db_name = db_name
        self.char_metadata_pl = self._fetch_character_metadata_pl()
        
        def get_env_list(key):
            val = os.getenv(key)
            return val.split(",") if val else []

        self.config = {
            "MOC": {"class": HonkaiStatistics_V2, "prefix": "moc", "versions": get_env_list("MOC_VERSIONS"), "floor": 12, "has_node": True},
            "PURE_FICTION": {"class": HonkaiStatistics_V2_Pure, "prefix": "pure_fiction", "versions": get_env_list("PF_VERSIONS"), "floor": 4, "has_node": True},
            "APOC": {"class": HonkaiStatistics_V2_APOC, "prefix": "apoc", "versions": get_env_list("APOC_VERSIONS"), "floor": 4, "has_node": True},
            "ANOMALY": {"class": HonkaiStatistics_Anomaly_V2, "prefix": "anomaly", "versions": get_env_list("ANOMALY_VERSIONS"), "floor": 0, "has_node": False}
        }

        self.rename_map = {
            # Existing entries...
            'Appearance Rate (%)': 'Appearance_Rate_pct',
            # Catching "Points" (Pure Fiction) and "Scores" (Apocalyptic Shadow)
            'Average Cycles': 'Average_Score',
            'Average Points': 'Average_Score',
            'Average Scores': 'Average_Score',
            'Average Score': 'Average_Score',
            'Avg Cycles': 'Average_Score',
            'Avg Points': 'Average_Score',
            'Avg Scores': 'Average_Score',
            'Avg Score': 'Average_Score',
            'Min Cycles': 'Min_Score',
            'Min Points': 'Min_Score',
            'Min Scores': 'Min_Score',
            'Min Score': 'Min_Score',
            'Max Cycles': 'Max_Score',
            'Max Points': 'Max_Score',
            'Max Scores': 'Max_Score',
            'Max Score': 'Max_Score',
            'Std Dev Cycles': 'Std_Dev',
            'Std Dev Points': 'Std_Dev',
            'Std Dev Scores': 'Std_Dev',
            'Std Dev Score': 'Std_Dev',
            # Percentiles
            '25th Percentile Cycles': 'Percentile_25',
            '25th Percentile Points': 'Percentile_25',
            '25th Percentile Scores': 'Percentile_25',
            'Median Cycles': 'Median_Score',
            'Median Points': 'Median_Score',
            'Median Scores': 'Median_Score',
            '75th Percentile Cycles': 'Percentile_75',
            '75th Percentile Points': 'Percentile_75',
            '75th Percentile Scores': 'Percentile_75'
        }

    def _fetch_character_metadata_pl(self):
        try:
            with open('characters.json', 'rb') as f:
                json_data = orjson.loads(f.read())
            data = [{"Character": name, **{k: (", ".join(v) if isinstance(v, list) else v) 
                    for k, v in info.items() if k != 'slug'}} 
                    for name, info in json_data.items()]
            return pl.DataFrame(data)
        except:
            return None

    def _standardize(self, df, mode, v, e, f, n, is_char=False):
        if df is None or not isinstance(df, pl.DataFrame) or df.is_empty():
            return None
            
        if is_char and self.char_metadata_pl is not None:
            df = df.join(self.char_metadata_pl, on="Character", how="left")

        # --- NEW: Specific Eidolon Column Cleaning ---
        # This turns "Eidolon 0.0 (%)" or "Eidolon 0 (%)" into "Eidolon_0_pct"
        eid_rename = {}
        for col in df.columns:
            if "Eidolon" in col and "%" in col:
                clean_name = col.replace(" (%)", "").replace(" ", "_").replace(".0", "") + "_pct"
                eid_rename[col] = clean_name
        if eid_rename:
            df = df.rename(eid_rename)

        # Apply standard Rename Map
        current_cols = df.columns
        rename_dict = {k: v for k, v in self.rename_map.items() if k in current_cols}
        df = df.rename(rename_dict)

        node_val = None if (n is None or mode == "ANOMALY") else str(n)

        df = df.with_columns([
            pl.lit(v).alias('version'),
            pl.lit(mode).alias('mode'),
            pl.lit(f).alias('floor'),
            pl.lit(e).alias('eidolon_level'),
            pl.lit(node_val, dtype=pl.Utf8).alias('node')
        ])

        # Final sweep to replace spaces and symbols
        df.columns = [c.replace(' ', '_').replace('(', '').replace(')', '').replace('%', 'pct').strip() for c in df.columns]
        
        numeric_cols = ['Appearance_Rate_pct', 'Average_Score', 'Percentile_25', 
                        'Median_Score', 'Percentile_75', 'Min_Score', 'Max_Score', 'Std_Dev']
        
        df = df.with_columns([
            pl.col(c).cast(pl.Float64, strict=False) for c in numeric_cols if c in df.columns
        ])

        return df.drop([c for c in ['Skewness', 'Kurtosis'] if c in df.columns])

    def _db_save(self, conn, df, table):
        if df is None: return
        
        # Registering Polars DF is zero-copy via Apache Arrow
        conn.register('temp_df', df)
        
        # Create table if it doesn't exist using the schema of the first scan
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM temp_df WHERE 1=0")
        
        # CRITICAL: BY NAME goes before the SELECT
        try:
            conn.execute(f"INSERT INTO {table} BY NAME SELECT * FROM temp_df")
        except Exception as ex:
            print(f"!!! Failed to append to {table}: {ex}")
            
        conn.unregister('temp_df')

    def orchestrate_update(self, target_mode=None, target_version=None, eidolons=[0, 1, 2, 6]):
        conn = duckdb.connect(self.db_name)
        modes = [target_mode] if target_mode else self.config.keys()

        for mode in modes:
            cfg = self.config[mode]
            versions = [target_version] if target_version else cfg["versions"]
            
            for v in versions:
                for e in eidolons:
                    floors = [0, 1, 2, 3, 4] if mode == "ANOMALY" else [cfg["floor"]]
                    for f in floors:
                        nodes = [0, 1, 2] if cfg["has_node"] else [None]
                        for n in nodes:
                            print(f"Updating {mode} {v} E{e} Floor{f} Node{n}...")
                            scraper = cfg["class"](version=v, floor=f, by_ed=e, node=n) if cfg["has_node"] else cfg["class"](version=v, floor=f, by_ed=e)

                            # FIXED: Added () to actually call the methods and get the DataFrame
                            self._db_save(conn, self._standardize(scraper.get_char_df(), mode, v, e, f, n, is_char=True), "character_stats")
                            self._db_save(conn, self._standardize(scraper.get_archetype_df(), mode, v, e, f, n), f"{cfg['prefix']}_stats_archetypes")
                            self._db_save(conn, self._standardize(scraper.get_team_df(), mode, v, e, f, n), f"{cfg['prefix']}_stats_teams")
                            self._db_save(conn, self._standardize(scraper.plot_statistics_all(cumulative=True,output=False), mode, v, e, f, n), f"{cfg['prefix']}_distributions")

                            # Combined / Dual Stats Logic
                            if n == 0 or (mode == "ANOMALY" and f == 0):
                                label = "Both" if mode != "ANOMALY" else None
                                suffix = "dual" if mode != "ANOMALY" else "triple"
                                
                                print(f"Updating Combined {suffix.upper()} Stats for {mode}...")
                                # FIXED: Added () here too
                                self._db_save(conn, self._standardize(scraper.get_combined_archetype_df(), mode, v, e, f, label), f"{cfg['prefix']}_stats_{suffix}_archetypes")
                                self._db_save(conn, self._standardize(scraper.get_combined_team_df(), mode, v, e, f, label), f"{cfg['prefix']}_stats_{suffix}_teams")
                                self._db_save(conn, self._standardize(scraper.plot_statistics_all_combined(cumulative=True,output=False), mode, v, e, f, label), f"{cfg['prefix']}_stats_{suffix}_distributions")
                conn.commit()
        conn.close()
        
if __name__ == "__main__":
    platform = HonkaiDataPlatform()
    platform.orchestrate_update()