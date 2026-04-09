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
        
        # Helper to get lists from env
        def get_env_list(key):
            val = os.getenv(key)
            return val.split(",") if val else []

        # Unified Configuration
        self.config = {
            "MOC": {
                "class": HonkaiStatistics,
                "tables": {"char": "character_stats", "arch": "moc_stats_archetypes", "team": "moc_stats_teams"},
                "dual_tables": {"arch": "moc_stats_dual_archetypes", "team": "moc_stats_dual_teams"},
                "versions": get_env_list("MOC_VERSIONS"),
                "default_floor": 12,
                "has_node": True
            },
            "PURE_FICTION": {
                "class": HonkaiStatistics_Pure,
                "tables": {"char": "character_stats", "arch": "pure_fiction_stats_archetypes", "team": "pure_fiction_stats_teams"},
                "dual_tables": {"arch": "pure_fiction_stats_dual_archetypes", "team": "pure_fiction_stats_dual_teams"},
                "versions": get_env_list("PF_VERSIONS"),
                "default_floor": 4,
                "has_node": True
            },
            "APOC": {
                "class": HonkaiStatistics_APOC,
                "tables": {"char": "character_stats", "arch": "apoc_stats_archetypes", "team": "apoc_stats_teams"},
                "dual_tables": {"arch": "apoc_stats_dual_archetypes", "team": "apoc_stats_dual_teams"},
                "versions": get_env_list("APOC_VERSIONS"),
                "default_floor": 4,
                "has_node": True
            },
            "ANOMALY": {
                "class": HonkaiStatistics_Anomaly,
                "tables": {"char": "character_stats", "arch": "anomaly_stats_archetypes", "team": "anomaly_stats_teams"},
                "dual_tables": {"arch": "anomaly_stats_triple_archetypes", "team": "anomaly_stats_triple_teams"},
                "versions": get_env_list("ANOMALY_VERSIONS"),
                "default_floor": 0,
                "has_node": False
            }
        }
        
        self.char_map = self._fetch_character_metadata()

    # --- INTERNAL HELPERS ---

    def _fetch_character_metadata(self):
        """Loads character metadata from local JSON."""
        try:
            with open('characters.json', 'rb') as f:
                json_data = orjson.loads(f.read())
            char_map = {}
            for name, info in json_data.items():
                meta = {k: v for k, v in info.items() if k != 'slug'}
                if 'role' in meta and isinstance(meta['role'], list):
                    meta['role'] = ", ".join(meta['role'])
                char_map[name] = meta
            return char_map
        except Exception as e:
            print(f"Warning: Metadata failed to load: {e}")
            return {}

    def _standardize(self, df, mode, version, eidolon, floor, node=None, is_char_pipeline=False):
        """Shared standardization logic for all pipelines."""
        df['version'] = version
        df['mode'] = mode
        df['floor'] = floor
        df['eidolon_level'] = eidolon
        
        # Node Handling
        if mode == "ANOMALY":
            df['node'] = "N/A"
        else:
            df['node'] = str(node) if node is not None else "Both"

        # Character Enrichment (Only for Character Pipeline)
        if is_char_pipeline and self.char_map:
            for meta_key in next(iter(self.char_map.values())).keys():
                df[meta_key] = df['Character'].map(lambda x: self.char_map.get(x, {}).get(meta_key))

        # Rename Map (Consolidated from all files)
        rename_map = {
            'Appearance Rate (%)': 'Appearance_Rate_pct',
            'Average Cycles': 'Average_Score', 'Average Score': 'Average_Score',
            'Min Cycles': 'Min_Score', 'Min Score': 'Min_Score',
            'Max Cycles': 'Max_Score', 'Max Score': 'Max_Score',
            'Std Dev Cycles': 'Std_Dev', 'Std Dev Score': 'Std_Dev', 'Std Dev': 'Std_Dev',
            '25th Percentile Cycles': 'Percentile_25', '25th Percentile': 'Percentile_25',
            'Median Cycles': 'Median_Score', 'Median Score': 'Median_Score',
            '75th Percentile Cycles': 'Percentile_75', '75th Percentile': 'Percentile_75'
        }
        df.rename(columns=rename_map, inplace=True)
        
        # SQL Cleanup
        df.columns = [c.replace(' ', '_').replace('(', '').replace(')', '').replace('%', 'pct').replace('•', '').strip() for c in df.columns]
        
        # Type Casting for reliability
        for col in [c for c in df.columns if any(x in c for x in ['Archetype', 'Team', 'Character'])]:
            df[col] = df[col].astype(str)
            
        return df

    def _table_exists(self, conn, table_name):
        return conn.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()[0] > 0

    # --- WAREHOUSE PIPELINES ---

    def run_character_warehouse(self, target_mode=None, target_version=None, eidolons=[0, 1, 2, 6]):
        """Ingests raw character appearance data."""
        conn = duckdb.connect(self.db_name)
        modes = [target_mode] if target_mode else self.config.keys()
        
        target_columns = [
            "Rank", "Character", "Appearance_Rate_pct", "Samples", "Min_Score",
            "Percentile_25", "Median_Score", "Percentile_75", "Average_Score",
            "Std_Dev", "Max_Score", "Sustain_Samples", "Sustain_Percentage",
            "Eidolon_0_pct", "Eidolon_1_pct", "Eidolon_2_pct", "Eidolon_3_pct",
            "Eidolon_4_pct", "Eidolon_5_pct", "Eidolon_6_pct", "version", "mode",
            "floor", "eidolon_level", "node", "id", "rarity", "path",
            "element", "availability", "release_phase", "role"
        ]

        for mode in modes:
            cfg = self.config[mode]
            versions = [target_version] if target_version else cfg["versions"]
            for v in versions:
                floors = [0, 1, 2, 3, 4] if mode == "ANOMALY" else [cfg["default_floor"]]
                for f in floors:
                    for e in eidolons:
                        nodes = [0, 1, 2] if cfg["has_node"] else [None]
                        for n in nodes:
                            try:
                                h = cfg["class"](version=v, floor=f, by_ed=e, node=n) if cfg["has_node"] else cfg["class"](version=v, floor=f, by_ed=e)
                                df = h.print_appearance_rate_by_char(output=False)
                                if df is not None and not df.empty:
                                    df_clean = self._standardize(df, mode, v, e, f, n, is_char_pipeline=True)
                                    for col in target_columns:
                                        if col not in df_clean.columns: df_clean[col] = None
                                    df_final = df_clean[target_columns]
                                    
                                    conn.register('temp_df', df_final)
                                    if self._table_exists(conn, "character_stats"):
                                        conn.execute("INSERT INTO character_stats SELECT * FROM temp_df")
                                    else:
                                        conn.execute("CREATE TABLE character_stats AS SELECT * FROM temp_df")
                                    conn.unregister('temp_df')
                                    print(f"Char Success: {mode} | {v} | E{e} | F{f}")
                            except Exception as ex: print(f"Error Char {mode} {v}: {ex}")
                conn.commit()
        conn.close()

    def run_group_warehouse(self, pipeline_type="arch", is_dual=False, target_mode=None, target_version=None, eidolons=[0, 1, 2, 6]):
        """Unified warehouse for Archetypes and Teams (Standard & Dual)."""
        conn = duckdb.connect(self.db_name)
        modes = [target_mode] if target_mode else self.config.keys()

        for mode in modes:
            cfg = self.config[mode]
            table_key = 'dual_tables' if is_dual else 'tables'
            target_table = cfg[table_key][pipeline_type]
            versions = [target_version] if target_version else cfg["versions"]

            for v in versions:
                for e in eidolons:
                    sub_loops = [None] if is_dual else ([0, 1, 2, 3, 4] if mode == "ANOMALY" else [0, 1, 2])
                    for val in sub_loops:
                        try:
                            # Selection logic for Scraper Method
                            h = cfg["class"](version=v, floor=(val if mode=="ANOMALY" else cfg["default_floor"]), by_ed=e, node=(None if mode=="ANOMALY" else val))
                            
                            if pipeline_type == "arch":
                                df = h.print_archetypes_both_sides(output=False) if is_dual else h.print_archetypes(output=False)
                            else:
                                df = h.print_appearance_rates_both_sides(output=False) if is_dual else h.print_appearance_rates(output=False)

                            if df is not None and not df.empty:
                                df = df.drop(columns=['Skewness', 'Kurtosis'], errors='ignore')
                                df_clean = self._standardize(df, mode, v, e, (val if mode=="ANOMALY" else cfg["default_floor"]), val)
                                
                                if self._table_exists(conn, target_table):
                                    conn.execute(f"INSERT INTO {target_table} SELECT * FROM df_clean")
                                else:
                                    conn.execute(f"CREATE TABLE {target_table} AS SELECT * FROM df_clean")
                                print(f"Added {pipeline_type.upper()} ({'Dual' if is_dual else 'Single'}): {mode} | {v} | E{e}")
                        except Exception as ex: print(f"Error {pipeline_type} {mode} {v}: {ex}")
                conn.commit()
        conn.close()

    # --- SUMMARY / ANALYSIS PIPELINES ---

    def run_meta_summaries(self, target_type="archetype"):
        """Generates Archetype or Team summaries (Historical and Recent)."""
        conn = duckdb.connect(self.db_name)
        
        # Define tasks based on type
        suffix = "archetypes" if target_type == "archetype" else "teams"
        group_col = "Archetype" if target_type == "archetype" else "Team"
        
        tasks = [
            {"mode": "MOC", "table": f"moc_stats_{suffix}", "floor": 12, "perf": "MIN", "node_col": "node", "node_val": "'0'"},
            {"mode": "APOC", "table": f"apoc_stats_{suffix}", "floor": 4, "perf": "MAX", "node_col": "node", "node_val": "'0'"},
            {"mode": "PURE_FICTION", "table": f"pure_fiction_stats_{suffix}", "floor": 4, "perf": "MAX", "node_col": "node", "node_val": "'0'"},
            {"mode": "ANOMALY_F0", "table": f"anomaly_stats_{suffix}", "floor": 0, "perf": "MIN", "node_col": None, "node_val": None},
            {"mode": "ANOMALY_F4", "table": f"anomaly_stats_{suffix}", "floor": 4, "perf": "MIN", "node_col": None, "node_val": None}
        ]

        all_history, all_recent = [], []

        for task in tasks:
            for is_recent in [False, True]:
                node_filter = f"AND {task['node_col']} = {task['node_val']}" if task['node_col'] else ""
                recent_filter = f"AND version IN (SELECT DISTINCT version FROM {task['table']} ORDER BY version DESC LIMIT 3)" if is_recent else ""
                
                query = f"""
                    SELECT '{task['mode']}' as Game_Mode, eidolon_level, {group_col},
                    ROUND(AVG(Appearance_Rate_pct), 2) as Simple_Avg_Appearance,
                    ROUND(AVG(Average_Score), 2) as Simple_Avg_Score,
                    ROUND(SUM(Average_Score * Samples) / SUM(Samples), 2) as Weighted_Avg_Score,
                    {task['perf']}(Average_Score) as Best_Version_Avg,
                    SUM(Samples) as Total_Samples,
                    STRING_AGG(DISTINCT version, ', ' ORDER BY version DESC) as Versions_Used
                    FROM {task['table']} WHERE Samples > 10 AND floor = {task['floor']} {node_filter} {recent_filter}
                    GROUP BY 1, 2, 3
                """
                try:
                    df = conn.execute(query).df()
                    if not df.empty:
                        (all_recent if is_recent else all_history).append(df)
                except Exception as e: print(f"Summary Error {task['mode']}: {e}")

        # Write to DB
        for data, name in [(all_history, f"{target_type}_meta_summary"), (all_recent, f"{target_type}_recent_meta_summary")]:
            if data:
                final_df = pd.concat(data, ignore_index=True)
                conn.execute(f"DROP TABLE IF EXISTS {name}")
                conn.execute(f"CREATE TABLE {name} AS SELECT * FROM final_df")
        
        conn.close()
        print(f">>> {target_type.capitalize()} Summaries Updated.")

# --- EXECUTION ---
if __name__ == "__main__":
    platform = HonkaiDataPlatform()
    
    # 1. Ingest Data (Example for a single version update)
    V = "4.1.1"
    platform.run_character_warehouse(target_version=V)
    platform.run_group_warehouse(pipeline_type="arch", target_version=V)
    platform.run_group_warehouse(pipeline_type="team", target_version=V)
    
    # 2. Run Meta Summaries
    platform.run_meta_summaries(target_type="archetype")
    platform.run_meta_summaries(target_type="team")