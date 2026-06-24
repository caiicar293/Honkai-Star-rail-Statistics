import duckdb
import polars as pl
import warnings
import os
import orjson
from dotenv import load_dotenv

from Appearance_rate_V2_batch import HonkaiStatistics_V2_Batch
from Appearance_rate_V2_batch_Pure_fiction import HonkaiStatistics_V2_Pure_fiction_Batch
from Appearance_rate_V2_batch_Apocalytic_Shadow import HonkaiStatistics_V2_APOC_Batch
from Appearance_rate_V2_batch_anomaly import HonkaiStatistics_V2_Anomaly_Batch
from Appearance_rates_Legacy import HonkaiStatistics_Legacy ,HonkaiStatistics_Legacy_Batch
from Appearance_rate_builds import HonkaiStatistics_builds
load_dotenv()
warnings.filterwarnings("ignore", category=RuntimeWarning)


class HonkaiDataPlatform:

    def __init__(self, db_name=os.getenv("DB_File")):
        self.db_name = db_name
        self.char_metadata_pl = self._fetch_character_metadata_pl()

        def get_env_list(key):
            val = os.getenv(key)
            return val.split(",") if val else []

        # -----------------------------------------------------------------
        # Mode config
        # era="MODERN" -> uses eidolon loop [0,1,2,6], full schema
        # era="LEGACY" -> no eidolon loop, up_to_eidolon=6, eidolon cols NULL
        # -----------------------------------------------------------------
        self.config = {
            # ---- LEGACY (pre-2.2.2, no cons cols in main parquet) --------
            "MOC_LEGACY": {
                "class":    HonkaiStatistics_Legacy_Batch,
                "prefix":   "moc",
                "versions": get_env_list("MOC_VERSIONS_LEGACY"),
                "floor":    10,
                "has_node": True,
                "era":      "LEGACY",
                "mode_arg": "moc_legacy",
            },
            "MOC_LATE_LEGACY": {
                "class":    HonkaiStatistics_Legacy_Batch,
                "prefix":   "moc",
                "versions": get_env_list("MOC_VERSIONS_LATE_LEGACY"),
                "floor":    12,
                "has_node": True,
                "era":      "LEGACY",
                "mode_arg": "moc_late_legacy",
            },
            "PURE_FICTION_LEGACY": {
                "class":    HonkaiStatistics_Legacy_Batch,
                "prefix":   "pure_fiction",
                "versions": get_env_list("PF_VERSIONS_LEGACY"),
                "floor":    4,
                "has_node": True,
                "era":      "LEGACY",
                "mode_arg": "pf_legacy",
            },
            # ---- MODERN (2.3+, has cons cols) ----------------------------
            "MOC": {
                "class":    HonkaiStatistics_V2_Batch,
                "prefix":   "moc",
                "versions": get_env_list("MOC_VERSIONS")[::-1],
                "floor":    12,
                "has_node": True,
                "era":      "MODERN",
            },
            "PURE_FICTION": {
                "class":    HonkaiStatistics_V2_Pure_fiction_Batch,
                "prefix":   "pure_fiction",
                "versions": get_env_list("PF_VERSIONS")[::-1],
                "floor":    4,
                "has_node": True,
                "era":      "MODERN",
            },
            "APOC": {
                "class":    HonkaiStatistics_V2_APOC_Batch,
                "prefix":   "apoc",
                "versions": get_env_list("APOC_VERSIONS")[::-1],  # reverse to go from newest to oldest
                "floor":    4,
                "has_node": True,
                "era":      "MODERN",
            },
            "ANOMALY": {
                "class":    HonkaiStatistics_V2_Anomaly_Batch,
                "prefix":   "anomaly",
                "versions": get_env_list("ANOMALY_VERSIONS"),
                "floor":    0,
                "has_node": False,
                "era":      "MODERN",
            },
        }

        self.rename_map = {
            'Appearance Rate (%)':       'Appearance_Rate_pct',
            'Average Cycles':            'Average_Score',
            'Average Points':            'Average_Score',
            'Average Scores':            'Average_Score',
            'Average Score':             'Average_Score',
            'Avg Cycles':                'Average_Score',
            'Avg_Cycles':                'Average_Score',
            'Avg Points':                'Average_Score',
            'Avg_Points':                'Average_Score',
            'Avg Scores':                'Average_Score',
            'Avg_Scores':                'Average_Score',
            'Avg_Score':                 'Average_Score',
            'Avg Score':                 'Average_Score',
            'Min':                       'Min_Score',
            'Min Cycles':                'Min_Score',
            'Min_Cycles':                'Min_Score',
            'Min Points':                'Min_Score',
            'Min_Points':                'Min_Score',
            'Min Scores':                'Min_Score',
            'Min Score':                 'Min_Score',
            'Max':                       'Max_Score',
            'Max Cycles':                'Max_Score',
            'Max_Cycles':                'Max_Score',
            'Max Points':                'Max_Score',
            'Max_Points':                'Max_Score',
            'Max Scores':                'Max_Score',
            'Max_Scores':                'Max_Score',
            'Max Score':                 'Max_Score',
            'Max_Score':                 'Max_Score',
            'Std Dev Cycles':            'Std_Dev',
            'Std Dev Points':            'Std_Dev',
            'Std Dev Scores':            'Std_Dev',
            'Std Dev Score':             'Std_Dev',
            'Std Dev':                   'Std_Dev',
            'Std':                       'Std_Dev',
            'Std_Points':                'Std_Dev',
            'Std_Cycles':                'Std_Dev',
            'Std_Scores':                'Std_Dev',
            '25th %':                    'Percentile_25',
            '25th Percentile Cycles':    'Percentile_25',
            '25th Percentile Points':    'Percentile_25',
            '25th Percentile Scores':    'Percentile_25',
            '25th Percentile':           'Percentile_25',
            'Median Cycles':             'Median_Score',
            'Median_Cycles':             'Median_Score',
            'Median Points':             'Median_Score',
            'Median_Points':             'Median_Score',
            'Median Scores':             'Median_Score',
            'Median':                    'Median_Score',
            '75th %':                    'Percentile_75',
            '75th Percentile Cycles':    'Percentile_75',
            '75th Percentile Points':    'Percentile_75',
            '75th Percentile Scores':    'Percentile_75',
            '75th Percentile':           'Percentile_75',
            'Min':                       'Min_Score',
            'Max':                       'Max_Score',
            'Average':                   'Average_Score',
            'Points':                    'Scores',
            'Cycles':                    'Scores',
        }

    # ------------------------------------------------------------------
    def _fetch_character_metadata_pl(self):
        try:
            with open('characters.json', 'rb') as f:
                json_data = orjson.loads(f.read())
            data = [
                {"Character": name,
                 **{k: (", ".join(v) if isinstance(v, list) else v)
                    for k, v in info.items() if k != 'slug'}}
                for name, info in json_data.items()
            ]
            return pl.DataFrame(data)
        except Exception:
            return None

    # ------------------------------------------------------------------
    def _standardize(self, df, mode, v, e, f, n, era, is_char=False):
        if df is None or not isinstance(df, pl.DataFrame) or df.is_empty():
            return None

        if is_char and self.char_metadata_pl is not None:
            df = df.join(self.char_metadata_pl, on="Character", how="left")

        # Normalise eidolon percentage column names
        eid_rename = {}
        for col in df.columns:
            if "Eidolon" in col and "%" in col:
                clean = col.replace(" (%)", "").replace(" ", "_").replace(".0", "") + "_pct"
                eid_rename[col] = clean
        if eid_rename:
            df = df.rename(eid_rename)

        # Apply standard rename map
        rename_dict = {k: v2 for k, v2 in self.rename_map.items() if k in df.columns}
        df = df.rename(rename_dict)

        node_val = None if (n is None or mode == "ANOMALY") else str(n)

        # 1. Map out all the potential literal columns you want to add
        potential_cols = {
            'version': pl.lit(v),
            'mode': pl.lit(mode),
            'era': pl.lit(era),
            'floor': pl.lit(f),
            'at_eidolon_level': pl.lit(0), # Default for standardization if not provided
            'up_to_eidolon_level': pl.lit(e),
            'node': pl.lit(node_val, dtype=pl.Utf8),
        }

        # 2. Filter the dictionary to only include keys NOT already in df.columns
        # (This works perfectly for both pl.DataFrame and pl.LazyFrame)
        missing_cols = [
            expr.alias(col_name) 
            for col_name, expr in potential_cols.items() 
            if col_name not in df.columns
        ]

        # 3. Only apply with_columns if there's actually something missing
        if missing_cols:
            df = df.with_columns(missing_cols)

        # Final column name sanitise
        df.columns = [
            c.replace(' (%)', '_pct')
            .replace('(%)', '_pct')
            .replace(' ', '_').replace('(', '').replace(')', '')
            .replace('%', 'pct').strip('_')
            .replace('__', '_')   # collapse double underscores
            for c in df.columns
        ]

        numeric_cols = [
            'Appearance_Rate_pct', 'Average_Score', 'Percentile_25',
            'Median_Score', 'Percentile_75', 'Min_Score', 'Max_Score', 'Std_Dev',
        ]
        df = df.with_columns([
            pl.col(c).cast(pl.Float64, strict=False)
            for c in numeric_cols if c in df.columns
        ])

        return df.drop([c for c in ['Skewness', 'Kurtosis'] if c in df.columns])

    # ------------------------------------------------------------------
    def _db_save(self, conn, df, table):
        if df is None:
            return
        conn.register('temp_df', df)
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM temp_df WHERE 1=0"
        )
        try:
            conn.execute(f"INSERT INTO {table} BY NAME SELECT * FROM temp_df")
        except Exception as ex:
            print(f"  !!! Failed to append to {table}: {ex}")
        conn.unregister('temp_df')

    # ------------------------------------------------------------------
    def _sort_table(self, conn, table):
        """Replace a table's contents with a version-sorted copy."""
        try:
            conn.execute(f"""
                CREATE OR REPLACE TABLE {table} AS
                SELECT * FROM {table}
                ORDER BY version DESC,at_eidolon_level,up_to_eidolon_level DESC,node DESC
            """)
        except Exception:
            pass

    # ------------------------------------------------------------------
    def _build_modern_scraper(self, cfg, v, e, f, n):
        cls = cfg["class"]
        if cfg["has_node"]:
            return cls(version=v, floor=f, by_ed=e, node=n)
        else:
            return cls(version=v, floor=f, by_ed=e)

    def _build_legacy_scraper(self, cfg, v, f, n):
        cls      = cfg["class"]
        mode_arg = cfg.get("mode_arg")
        if cfg["has_node"]:
            return cls(sub_mode=mode_arg,floor=f, node=n)
        else:
            return cls(sub_mode=mode_arg,floor=f)

    # ------------------------------------------------------------------
    def _process_modern(self, conn, mode, cfg, v, e, f, n, eidolons):
        era = "MODERN"
        print(f"  [MODERN] {mode} v={v} e={e} floor={f} node={n}")
        try:
            scraper = self._build_modern_scraper(cfg, v, e, f, n)
        except Exception as ex:
            print(f"  !!! Scraper init failed: {ex}")
            return

        prefix = cfg["prefix"]

        self._db_save(conn,
            self._standardize(scraper.get_char_df(), mode, v, e, f, n, era, is_char=True),
            "character_stats")
        self._db_save(conn,
            self._standardize(scraper.get_archetype_df(), mode, v, e, f, n, era),
            f"{prefix}_stats_archetypes")
        self._db_save(conn,
            self._standardize(scraper.get_eidolon_performance_df(), mode, v, e, f, n, era),
            f"{prefix}_stats_eidolon_performance")
        self._db_save(conn,
            self._standardize(scraper.get_team_df(), mode, v, e, f, n, era),
            f"{prefix}_stats_teams")
        self._db_save(conn,
            self._standardize(scraper.get_duos_stats(), mode, v, e, f, n, era),
            f"{prefix}_stats_duos")
        self._db_save(conn,
            self._standardize(
                scraper.plot_statistics_all(cumulative=True, output=False),
                mode, v, e, f, n, era),
            f"{prefix}_stats_distributions")

        # For non-ANOMALY: combined triggers on node=0/"all"
        # For ANOMALY:     combined triggers on floor=0/"all" (floor is the equivalent axis)
        if mode == "ANOMALY":
            combined_trigger = f in (0, "all")
            gear_trigger     = f in (0, 4, "all")
        else:
            combined_trigger = n in (0, "all")
            gear_trigger     = n in (0, "all")

        if combined_trigger:
            label  = "Both" if mode != "ANOMALY" else None
            suffix = "dual_or_triple" if mode != "ANOMALY" else "triple"
            print(f"  [MODERN] Combined {suffix.upper()} for {mode} v={v} e={e}")
            self._db_save(conn,
                self._standardize(scraper.get_combined_archetype_df(), mode, v, e, f, label, era),
                f"{prefix}_stats_{suffix}_archetypes")
            self._db_save(conn,
                self._standardize(scraper.get_combined_team_df(), mode, v, e, f, label, era),
                f"{prefix}_stats_{suffix}_teams")
            self._db_save(conn,
                self._standardize(
                    scraper.plot_statistics_all_combined(cumulative=True, output=False),
                    mode, v, e, f, label, era),
                f"{prefix}_stats_{suffix}_distributions")

        if gear_trigger:
            print(f"  [MODERN] Gear for {mode} v={v} e={e}")
            self._db_save(conn,
                self._standardize(scraper.display_top_gear(), mode, v, e, f, n, era),
                f"{prefix}_stats_gear_usage")

    # ------------------------------------------------------------------
    def _process_legacy(self, conn, mode, cfg, v, f, n):
        era = "LEGACY"
        e   = 6           # sentinel: no eidolon filtering applied
        print(f"  [LEGACY] {mode} v{v} Floor{f} Node{n}")
        try:
            scraper = self._build_legacy_scraper(cfg, v, f, n)
        except Exception as ex:
            print(f"  !!! Scraper init failed: {ex}")
            return

        prefix = cfg["prefix"]
        n= None
        self._db_save(conn,
            self._standardize(scraper.get_char_df(), mode, v, e, f, n, era, is_char=True),
            "character_stats")
        self._db_save(conn,
            self._standardize(scraper.get_archetype_df(), mode, v, e, f, n, era),
            f"{prefix}_stats_archetypes")
        self._db_save(conn,
            self._standardize(scraper.get_team_df(), mode, v, e, f, n, era),
            f"{prefix}_stats_teams")
        self._db_save(conn,
            self._standardize(scraper.get_duos_stats(), mode, v, e, f, n, era),
            f"{prefix}_stats_duos")
        self._db_save(conn,
            self._standardize(
                scraper.plot_statistics_all(output=False),
                mode, v, e, f, n, era),
            f"{prefix}_stats_distributions")

        if n == 0 or v =="all" or n=="all":
            label  = "Both"
            suffix = "dual_or_triple"
            print(f"  [LEGACY] Combined {suffix.upper()} for {mode} v{v}")
            self._db_save(conn,
                self._standardize(scraper.get_combined_archetype_df(), mode, v, e, f, label, era),
                f"{prefix}_stats_{suffix}_archetypes")
            self._db_save(conn,
                self._standardize(scraper.get_combined_team_df(), mode, v, e, f, label, era),
                f"{prefix}_stats_{suffix}_teams")
            self._db_save(conn,
                self._standardize(
                    scraper.plot_statistics_all_combined(output=False),
                    mode, v, e, f, label, era),
                f"{prefix}_stats_{suffix}_distributions")

        if n == 0 or v =="all" or n=="all":
          
            print(f"  [LEGACY] Gear for {mode} v{v}")
            self._db_save(conn,
                self._standardize(scraper.display_top_gear().filter(pl.col('node')==0), mode, v, e, f,n, era),
                f"{prefix}_stats_gear_usage")

    # ------------------------------------------------------------------
    def orchestrate_update(
        self,
        target_mode=None,
        target_version=None,
        modern_strategy="all_at_once",  # all_at_once | per_version | per_node | per_eidolon | granular
        legacy_strategy="all_at_once",  # all_at_once | per_version
    ):
        conn = duckdb.connect(self.db_name)
        modes_to_run = [target_mode] if target_mode else list(self.config.keys())
        modern_modes = [m for m in modes_to_run if self.config[m]["era"] == "MODERN"]
        legacy_modes = [m for m in modes_to_run if self.config[m]["era"] == "LEGACY"]

        def specific_versions(cfg):
            if target_version:
                return [target_version]
            return cfg["versions"]

        EIDOLONS = [0, 1, 2, 6]

        # ------------------------------------------------------------------
        # PASS 1 — MODERN
        # ------------------------------------------------------------------
        print("=" * 60)
        print(f"PASS 1: MODERN  [strategy={modern_strategy}]")
        print("=" * 60)
        self._orchestrate_modern(conn, modern_modes, modern_strategy, specific_versions, EIDOLONS)

        # ------------------------------------------------------------------
        # PASS 2 — LEGACY
        # ------------------------------------------------------------------
        print("=" * 60)
        print(f"PASS 2: LEGACY  [strategy={legacy_strategy}]")
        print("=" * 60)
        self._orchestrate_legacy(conn, legacy_modes, legacy_strategy, specific_versions)

        # ------------------------------------------------------------------
        # PASS 3 — Sort
        # ------------------------------------------------------------------
        print("=" * 60)
        print("PASS 3: Sorting all tables")
        print("=" * 60)
        all_tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
        for (tbl,) in all_tables:
            print(f"  Sorting {tbl}...")
            self._sort_table(conn, tbl)

        conn.commit()
        conn.close()
        print("Done.")

        # ------------------------------------------------------------------
        # PASS 4 — Builds
        # ------------------------------------------------------------------
        builds = HonkaiStatistics_builds()
        builds.save_to_db()

    # ------------------------------------------------------------------
    def _orchestrate_modern(self, conn, modern_modes, strategy, specific_versions, EIDOLONS):
        for mode in modern_modes:
            cfg = self.config[mode]

            # ANOMALY iterates floors the same way non-ANOMALY iterates nodes
            if mode == "ANOMALY":
                floors = [0, 4, "all"]   # 0=normal, 4=hard, "all"=aggregate
                nodes  = [None]
            else:
                floors = [cfg["floor"]]
                nodes  = [0, 1, 2] if cfg["has_node"] else [None]

            if strategy == "all_at_once":
                # Single scraper call — version/node/floor/eidolon all aggregated internally
                if mode == "ANOMALY":
                    self._process_modern(conn, mode, cfg, v="all", e="all", f="all", n=None, eidolons="all")
                else:
                    for f in floors:
                        self._process_modern(conn, mode, cfg, v="all", e="all", f=f, n="all", eidolons="all")
                conn.commit()

            elif strategy == "per_version":
                for v in specific_versions(cfg):
                    if mode == "ANOMALY":
                        self._process_modern(conn, mode, cfg, v=v, e="all", f="all", n=None, eidolons="all")
                    else:
                        for f in floors:
                            self._process_modern(conn, mode, cfg, v=v, e="all", f=f, n="all", eidolons="all")
                    print(f"  [commit] {mode} version {v} done")
                    conn.commit()

            elif strategy == "per_node":
                # For ANOMALY this means per_floor
                if mode == "ANOMALY":
                    for f in [0, 4]:   # explicit floors, no "all"
                        self._process_modern(conn, mode, cfg, v="all", e="all", f=f, n=None, eidolons="all")
                else:
                    for f in floors:
                        for n in nodes:
                            self._process_modern(conn, mode, cfg, v="all", e="all", f=f, n=n, eidolons="all")
                conn.commit()

            elif strategy == "per_eidolon":
                if mode == "ANOMALY":
                    for e in EIDOLONS:
                        self._process_modern(conn, mode, cfg, v="all", e=e, f="all", n=None, eidolons=e)
                else:
                    for e in EIDOLONS:
                        for f in floors:
                            self._process_modern(conn, mode, cfg, v="all", e=e, f=f, n="all", eidolons=e)
                conn.commit()

            elif strategy == "granular":
                if mode == "ANOMALY":
                    for v in specific_versions(cfg):
                        for f in [0, 4]:
                            for e in EIDOLONS:
                                self._process_modern(conn, mode, cfg, v=v, e=e, f=f, n=None, eidolons=e)
                        print(f"  [commit] {mode} version {v} done")
                        conn.commit()
                else:
                    for v in specific_versions(cfg):
                        for f in floors:
                            for n in nodes:
                                for e in EIDOLONS:
                                    self._process_modern(conn, mode, cfg, v=v, e=e, f=f, n=n, eidolons=e)
                        print(f"  [commit] {mode} version {v} done")
                        conn.commit()

            else:
                raise ValueError(f"Unknown modern_strategy: {strategy!r}")

    # ------------------------------------------------------------------
    def _orchestrate_legacy(self, conn, legacy_modes, strategy, specific_versions):
        for mode in legacy_modes:
            cfg = self.config[mode]
            f   = cfg["floor"]

            if strategy == "all_at_once":
                self._process_legacy(conn, mode, cfg, v="all", f=f, n="all")
                conn.commit()

            elif strategy == "per_version":
                for v in specific_versions(cfg):
                    self._process_legacy(conn, mode, cfg, v=v, f=f, n="all")
                    print(f"  [commit] {mode} legacy version {v} done")
                    conn.commit()

            else:
                raise ValueError(f"Unknown legacy_strategy: {strategy!r}")

if __name__ == "__main__":
    platform = HonkaiDataPlatform()
    # Default — both use all_at_once
    platform.orchestrate_update()

    
