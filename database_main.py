import duckdb
import polars as pl
import warnings
import os
import orjson
from dotenv import load_dotenv

from Appearance_rate_V2 import HonkaiStatistics_V2
from Appearance_rate_Pure_fiction_V2 import HonkaiStatistics_V2_Pure
from Appearance_rate_Apocalytic_Shadow_V2 import HonkaiStatistics_V2_APOC
from Appearance_rate_anomaly_V2 import HonkaiStatistics_Anomaly_V2
from Appearance_rates_Legacy import HonkaiStatistics_Legacy

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
                "class":    HonkaiStatistics_Legacy,
                "prefix":   "moc",
                "versions": get_env_list("MOC_VERSIONS_LEGACY"),
                "floor":    10,
                "has_node": True,
                "era":      "LEGACY",
                "mode_arg": "moc",
            },
            "MOC_LATE_LEGACY": {
                "class":    HonkaiStatistics_Legacy,
                "prefix":   "moc",
                "versions": get_env_list("MOC_VERSIONS_LATE_LEGACY"),
                "floor":    12,
                "has_node": True,
                "era":      "LEGACY",
                "mode_arg": "moc",
            },
            "PURE_FICTION_LEGACY": {
                "class":    HonkaiStatistics_Legacy,
                "prefix":   "pure_fiction",
                "versions": get_env_list("PF_VERSIONS_LEGACY"),
                "floor":    4,
                "has_node": True,
                "era":      "LEGACY",
                "mode_arg": "pf",
            },
            # ---- MODERN (2.3+, has cons cols) ----------------------------
            "MOC": {
                "class":    HonkaiStatistics_V2,
                "prefix":   "moc",
                "versions": get_env_list("MOC_VERSIONS"),
                "floor":    12,
                "has_node": True,
                "era":      "MODERN",
            },
            "PURE_FICTION": {
                "class":    HonkaiStatistics_V2_Pure,
                "prefix":   "pure_fiction",
                "versions": get_env_list("PF_VERSIONS"),
                "floor":    4,
                "has_node": True,
                "era":      "MODERN",
            },
            "APOC": {
                "class":    HonkaiStatistics_V2_APOC,
                "prefix":   "apoc",
                "versions": get_env_list("APOC_VERSIONS"),
                "floor":    4,
                "has_node": True,
                "era":      "MODERN",
            },
            "ANOMALY": {
                "class":    HonkaiStatistics_Anomaly_V2,
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

        df = df.with_columns([
            pl.lit(v).alias('version'),
            pl.lit(mode).alias('mode'),
            pl.lit(era).alias('era'),
            pl.lit(f).alias('floor'),
            pl.lit(e).alias('up_to_eidolon_level'),
            pl.lit(node_val, dtype=pl.Utf8).alias('node'),
        ])

        # Final column name sanitise
        df.columns = [
            c.replace(' ', '_').replace('(', '').replace(')', '')
             .replace('%', 'pct').strip()
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
                ORDER BY version
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
        mode_arg = cfg.get("mode_arg", "moc")
        if cfg["has_node"]:
            return cls(version=v, floor=f, mode=mode_arg, node=n)
        else:
            return cls(version=v, floor=f, mode=mode_arg)

    # ------------------------------------------------------------------
    def _process_modern(self, conn, mode, cfg, v, e, f, n, eidolons):
        era = "MODERN"
        print(f"  [MODERN] {mode} v{v} E{e} Floor{f} Node{n}")
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

        if n == 0 or (mode == "ANOMALY" and f == 0):
            label  = "Both" if mode != "ANOMALY" else None
            suffix = "dual" if mode != "ANOMALY" else "triple"
            print(f"  [MODERN] Combined {suffix.upper()} for {mode} v{v} E{e}")
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

        if n == 0 or (mode == "ANOMALY" and f in (0, 4)):
            label = "Both" if mode != "ANOMALY" else None
            print(f"  [MODERN] Gear for {mode} v{v} E{e}")
            self._db_save(conn,
                self._standardize(scraper.display_top_gear(), mode, v, e, f, label, era),
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

        if n == 0:
            label  = "Both"
            suffix = "dual"
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

        if n == 0:
            label = "Both"
            print(f"  [LEGACY] Gear for {mode} v{v}")
            self._db_save(conn,
                self._standardize(scraper.display_top_gear(), mode, v, e, f, label, era),
                f"{prefix}_stats_gear_usage")

    # ------------------------------------------------------------------
    def orchestrate_update(
        self,
        target_mode=None,
        target_version=None,
        eidolons=None,
    ):
        if eidolons is None:
            eidolons = [0, 1, 2, 6]

        conn = duckdb.connect(self.db_name)
        modes_to_run = [target_mode] if target_mode else list(self.config.keys())

        # ------------------------------------------------------------------
        # PASS 1 — MODERN (creates tables with full schema including era col)
        # ------------------------------------------------------------------
        print("=" * 60)
        print("PASS 1: MODERN data")
        print("=" * 60)
        modern_modes = [m for m in modes_to_run if self.config[m]["era"] == "MODERN"]

        for mode in modern_modes:
            cfg      = self.config[mode]
            versions = [target_version] if target_version else cfg["versions"]
            for v in versions:
                floors = [0, 1, 2, 3, 4] if mode == "ANOMALY" else [cfg["floor"]]
                for f in floors:
                    nodes = [0, 1, 2] if cfg["has_node"] else [None]
                    for n in nodes:
                        for e in eidolons:
                            self._process_modern(conn, mode, cfg, v, e, f, n, eidolons)
            conn.commit()

        # ------------------------------------------------------------------
        # PASS 2 — LEGACY (BY NAME fills missing eidolon cols with NULL)
        # ------------------------------------------------------------------
        print("=" * 60)
        print("PASS 2: LEGACY data")
        print("=" * 60)
        legacy_modes = [m for m in modes_to_run if self.config[m]["era"] == "LEGACY"]

        for mode in legacy_modes:
            cfg      = self.config[mode]
            versions = [target_version] if target_version else cfg["versions"]
            for v in versions:
                floors = [cfg["floor"]]
                for f in floors:
                    nodes = [0, 1, 2] if cfg["has_node"] else [None]
                    for n in nodes:
                        self._process_legacy(conn, mode, cfg, v, f, n)
            conn.commit()

        # ------------------------------------------------------------------
        # PASS 3 — Sort every table by version
        # ------------------------------------------------------------------
        print("=" * 60)
        print("PASS 3: Sorting all tables by version")
        print("=" * 60)
        all_tables = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main'"
        ).fetchall()

        for (tbl,) in all_tables:
            print(f"  Sorting {tbl}...")
            self._sort_table(conn, tbl)

        conn.commit()
        conn.close()
        print("Done.")


if __name__ == "__main__":
    platform = HonkaiDataPlatform()
    platform.orchestrate_update()
