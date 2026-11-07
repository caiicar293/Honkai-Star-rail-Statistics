import duckdb
import polars as pl
import warnings
import os
import orjson
from dotenv import load_dotenv
import time
from Appearance_rate_V2_batch import HonkaiStatistics_V2_Batch
from Appearance_rate_V2_batch_Pure_fiction import HonkaiStatistics_V2_Pure_fiction_Batch
from Appearance_rate_V2_batch_Apocalytic_Shadow import HonkaiStatistics_V2_APOC_Batch
from Appearance_rate_V2_batch_anomaly import HonkaiStatistics_V2_Anomaly_Batch
from Appearance_rates_Legacy import HonkaiStatistics_Legacy ,HonkaiStatistics_Legacy_Batch
from Appearance_rate_builds import HonkaiStatistics_builds
from Appearance_rate_V2_batch_all_modes_by_cost import HonkaiStatistics_V2_eidolon_batch
from snapshot import snapshot
from char_metadata_coverage import missing_char_metadata, describe_missing_char_metadata
load_dotenv()
warnings.filterwarnings("ignore", category=RuntimeWarning)


# ---------------------------------------------------------------------
# Per-gamemode ingest strategy — read from .env with the SAME keys the
# frontend orchestrator uses (frontend_orchestrator.MODE_SLOTS):
#
#     MOC = replace
#     Pure_fiction = add
#     anomaly = replace
#     apoc = replace
#
#   replace -> delete the version being added from that gamemode's tables
#              first, so re-adding refreshes its rows instead of duplicating
#   add     -> append only; nothing is deleted for that gamemode
#
# DB_STRATEGY, if set to 'replace' or 'add', still overrides every mode
# (legacy single-key behaviour / CI smoke tests).
# ---------------------------------------------------------------------
MODE_STRATEGY_ENV_KEYS = {
    # modern gamemodes
    "MOC":                  "MOC",
    "PURE_FICTION":         "Pure_fiction",
    "APOC":                 "apoc",
    "ANOMALY":              "anomaly",
    "ANOMALY_HARD":         "anomaly",
    # legacy gamemodes inherit their modern counterpart's strategy
    "MOC_LEGACY":           "MOC",
    "MOC_LATE_LEGACY":      "MOC",
    "PURE_FICTION_LEGACY":  "Pure_fiction",
    # cost-stratified tables, same gamemode mapping
    "MOC_COST":             "MOC",
    "PURE_FICTION_COST":    "Pure_fiction",
    "APOC_COST":            "apoc",
    "ANOMALY_COST":         "anomaly",
    "ANOMALY_HARD_COST":    "anomaly",
}


# Sort key for the sort pass (PASS 4) — same order the polars implementation
# used: version DESC, at_eidolon_level ASC, up_to_eidolon_level DESC, node
# DESC, all NULLS LAST. Tables missing a column simply skip that key.
SORT_SPEC = [
    ("version", "DESC"),
    ("at_eidolon_level", "ASC"),
    ("up_to_eidolon_level", "DESC"),
    ("node", "DESC"),
]


def sort_clause(columns):
    """ORDER BY fragment for the present sort keys (NULLS LAST on each)."""
    return ", ".join(f"{c} {d} NULLS LAST" for c, d in SORT_SPEC if c in columns)


def get_mode_strategy(mode, default="add"):
    """Resolve one config mode to 'replace' or 'add' from .env.

    mode is a config key such as 'MOC', 'ANOMALY_HARD' or 'APOC_COST'.
    Falls back to `default` (append-only) when the mode has no .env key or
    the key is blank/unknown.
    """
    override = (os.getenv("DB_STRATEGY") or "").strip().lower()
    if override in ("replace", "add"):
        return override

    key = MODE_STRATEGY_ENV_KEYS.get(str(mode).upper())
    if key is None:
        return default
    value = (os.getenv(key) or "").strip().lower()
    if value in ("replace", "add"):
        return value
    return default


class HonkaiDataPlatform:

    def __init__(self, db_name=os.getenv("DB_File")):
        self.db_name = db_name
        self.char_metadata_pl = self._fetch_character_metadata_pl()
        # Names already warned about, so a multi-mode/version run doesn't repeat itself.
        self._missing_char_meta_reported = set()

        def get_env_list(key):
            val = os.getenv(key)
            return val.split(",") if val else []

        self.cost_config = {
            "MOC_COST": {
                "mode_arg": "moc",
                "prefix":   "moc",
                "versions": get_env_list("MOC_VERSIONS")[::-1],
                "floor":    12,
                "has_node": True,
                "era":      "MODERN",
            },
            "PURE_FICTION_COST": {
                "mode_arg": "pure fiction",
                "prefix":   "pure_fiction",
                "versions": get_env_list("PF_VERSIONS")[::-1],
                "floor":    4,
                "has_node": True,
                "era":      "MODERN",
            },
            "APOC_COST": {
                "mode_arg": "apoc",
                "prefix":   "apoc",
                "versions": get_env_list("APOC_VERSIONS")[::-1],
                "floor":    4,
                "has_node": True,
                "era":      "MODERN",
            },
            "ANOMALY_COST": {
                "mode_arg": "anomaly",
                "prefix":   "anomaly",
                "versions": get_env_list("ANOMALY_VERSIONS"),
                "floor":    0,
                "has_node": False,
                "era":      "MODERN",
            },
            "ANOMALY_HARD_COST": {
                "mode_arg":    "anomaly",
                "prefix":      "anomaly",
                "versions":    get_env_list("ANOMALY_VERSIONS"),
                "floor":       4,
                "has_node":    False,
                "era":         "MODERN",
                "hard_mode":   True,
                "floor_label": 5,
                "db_mode":     "ANOMALY_COST",
            },
        }

        self.config = {
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
                "versions": get_env_list("APOC_VERSIONS")[::-1],
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
            "ANOMALY_HARD": {
                "class":       HonkaiStatistics_V2_Anomaly_Batch,
                "prefix":      "anomaly",
                "versions":    get_env_list("ANOMALY_VERSIONS"),
                "floor":       4,
                "has_node":    False,
                "era":         "MODERN",
                "hard_mode":   True,
                "floor_label": 5,
                "db_mode":     "ANOMALY",
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
            'Full_Clear_Rate':           'Full_Clear_Rate_pct',
            'Cycles Distributions':      'Scores_Distributions',
            'Points Distributions':      'Scores_Distributions',
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
    def _check_char_metadata_coverage(self, df):
        if self.char_metadata_pl is None or "Character" not in df.columns:
            return
        missing = missing_char_metadata(
            self.char_metadata_pl.get_column("Character").to_list(),
            df.get_column("Character").unique().to_list(),
        )
        missing = [n for n in missing if n not in self._missing_char_meta_reported]
        if not missing:
            return
        self._missing_char_meta_reported.update(missing)
        print(describe_missing_char_metadata(missing))

    # ------------------------------------------------------------------
    def _standardize(self, df, mode, v, e, f, n, era, is_char=False, force_floor=False):
        if df is None or not isinstance(df, pl.DataFrame) or df.is_empty():
            return None

        if is_char and self.char_metadata_pl is not None:
            self._check_char_metadata_coverage(df)
            df = df.join(self.char_metadata_pl, on="Character", how="left")

        eid_rename = {}
        for col in df.columns:
            if "Eidolon" in col and "%" in col:
                clean = col.replace(" (%)", "").replace(" ", "_").replace(".0", "") + "_pct"
                eid_rename[col] = clean
        if eid_rename:
            df = df.rename(eid_rename)

        rename_dict = {k: v2 for k, v2 in self.rename_map.items() if k in df.columns}
        df = df.rename(rename_dict)

        node_val = None if (n is None or mode in ("ANOMALY", "ANOMALY_HARD")) else str(n)

        potential_cols = {
            'version': pl.lit(v),
            'mode': pl.lit(mode),
            'era': pl.lit(era),
            'floor': pl.lit(f),
            'at_eidolon_level': pl.lit(0),
            'up_to_eidolon_level': pl.lit(e),
            'node': pl.lit(node_val, dtype=pl.Utf8),
        }

        missing_cols = [
            expr.alias(col_name) 
            for col_name, expr in potential_cols.items() 
            if col_name not in df.columns or (col_name == 'floor' and force_floor)
        ]

        if missing_cols:
            df = df.with_columns(missing_cols)

        df.columns = [
            c.replace(' (%)', '_pct')
            .replace('(%)', '_pct')
            .replace(' ', '_').replace('(', '').replace(')', '')
            .replace('%', 'pct').strip('_')
            .replace('__', '_')
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
    def _db_save_batch(self, conn, items):
        """Save a list of (df, table) tuples in a single explicit transaction,

        caching table existence to skip redundant CREATE TABLE checks.
        Automatically chunks DataFrames exceeding 1M rows to prevent memory spilling.
        """
        valid_items = sorted(
            [(df, table) for df, table in items if df is not None and not df.is_empty()],
            key=lambda x: len(x[0]),
            reverse=True
        )
        if not valid_items:
            return

        # Lazy-load existing tables from DuckDB's information_schema once per session
        if not hasattr(self, "_session_tables") or self._session_tables is None:
            try:
                rows = conn.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
                ).fetchall()
                self._session_tables = {row[0].lower() for row in rows}
            except Exception:
                self._session_tables = set()

        conn.execute("BEGIN TRANSACTION;")
        try:
            for idx, (df, table) in enumerate(valid_items):
                start = time.perf_counter()
                row_count = len(df)
                
                # Check cache first — create the table structure using a small header view if missing
                if table.lower() not in self._session_tables:
                    schema_view = f"temp_schema_{idx}"
                    conn.register(schema_view, df.head(1))
                    conn.execute(
                        f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM {schema_view} WHERE 1=0"
                    )
                    conn.unregister(schema_view)
                    self._session_tables.add(table.lower())

                # Chunk insertion if row count exceeds 1 million
                if row_count > 1_000_000:
                    chunk_size = 1_000_000
                    for c_idx, chunk in enumerate(df.iter_slices(n_rows=chunk_size)):
                        chunk_view = f"temp_df_{idx}_chunk_{c_idx}"
                        conn.register(chunk_view, chunk)
                        try:
                            conn.execute(f"INSERT INTO {table} BY NAME SELECT * FROM {chunk_view}")
                        except Exception as ex:
                            print(f"  !!! Failed to append chunk {c_idx} to {table}: {ex}")
                        conn.unregister(chunk_view)
                else:
                    # Standard single-shot insertion for smaller tables
                    view_name = f"temp_df_{idx}"
                    conn.register(view_name, df)
                    try:
                        conn.execute(f"INSERT INTO {table} BY NAME SELECT * FROM {view_name}")
                    except Exception as ex:
                        print(f"  !!! Failed to append to {table}: {ex}")
                    conn.unregister(view_name)
                
                end = time.perf_counter()
                print(f"  [DB SAVE] {table} ({row_count:,} rows) saved in {end - start:.4f} seconds")
                
            conn.execute("COMMIT;")
        except Exception as ex:
            conn.execute("ROLLBACK;")
            print(f"  !!! Batch save transaction failed: {ex}")
    
    # ------------------------------------------------------------------
    def _get_modern_saves(self, mode, cfg, v, e, f, n):
        era = "MODERN"
        is_anomaly_family = mode in ("ANOMALY", "ANOMALY_HARD")
        force_floor = "floor_label" in cfg
        f_out = cfg.get("floor_label", f)
        db_mode = cfg.get("db_mode", mode)
        print(f"  [MODERN] {mode} v={v} e={e} floor={f} node={n}")
        try:
            scraper = self._build_modern_scraper(cfg, v, e, f, n)
        except Exception as ex:
            print(f"  !!! Scraper init failed: {ex}")
            return []

        prefix = cfg["prefix"]

        modern_saves = [
            (self._standardize(scraper.get_char_df(), db_mode, v, e, f_out, n, era, is_char=True, force_floor=force_floor), "character_stats"),
            (self._standardize(scraper.get_archetype_df(), db_mode, v, e, f_out, n, era, force_floor=force_floor), f"{prefix}_stats_archetypes"),
            (self._standardize(scraper.get_eidolon_performance_df(), db_mode, v, e, f_out, n, era, force_floor=force_floor), f"{prefix}_stats_eidolon_performance"),
            (self._standardize(scraper.get_team_df(), db_mode, v, e, f_out, n, era, force_floor=force_floor), f"{prefix}_stats_teams"),
            (self._standardize(scraper.get_duos_stats(), db_mode, v, e, f_out, n, era, force_floor=force_floor), f"{prefix}_stats_duos"),
            (self._standardize(scraper.get_trios_stats(), db_mode, v, e, f_out, n, era, force_floor=force_floor), f"{prefix}_stats_trios"),
            (self._standardize(scraper.get_quos_stats(), db_mode, v, e, f_out, n, era, force_floor=force_floor), f"{prefix}_stats_quos"),
            (self._standardize(scraper.plot_statistics_all(cumulative=True, output=False), db_mode, v, e, f_out, n, era, force_floor=force_floor), f"{prefix}_stats_distributions"),
        ]

        if is_anomaly_family:
            combined_trigger = f in (0, "all")
            gear_trigger     = f in (0, 4, "all")
        else:
            combined_trigger = n in (0, "all")
            gear_trigger     = n in (0, "all")

        if combined_trigger:
            label  = "Both" if not is_anomaly_family else None
            suffix = "dual_or_triple" if not is_anomaly_family else "triple"
            print(f"  [MODERN] Combined {suffix.upper()} for {mode} v={v} e={e}")
            modern_saves.extend([
                (self._standardize(scraper.get_combined_archetype_df(), db_mode, v, e, f_out, label, era, force_floor=force_floor), f"{prefix}_stats_{suffix}_archetypes"),
                (self._standardize(scraper.get_combined_team_df(), db_mode, v, e, f_out, label, era, force_floor=force_floor), f"{prefix}_stats_{suffix}_teams"),
                (self._standardize(scraper.plot_statistics_all_combined(cumulative=True, output=False), db_mode, v, e, f_out, label, era, force_floor=force_floor), f"{prefix}_stats_{suffix}_distributions"),
            ])

        if gear_trigger:
            print(f"  [MODERN] Gear for {mode} v={v} e={e}")
            modern_saves.append(
                (self._standardize(scraper.display_top_gear(), db_mode, v, e, f_out, n, era, force_floor=force_floor), f"{prefix}_stats_gear_usage")
            )

        return modern_saves

    # ------------------------------------------------------------------
    def _get_cost_saves(self, mode, cfg, version):
        era       = "MODERN"
        prefix    = cfg["prefix"]
        m_arg     = cfg["mode_arg"]
        f         = cfg["floor"]
        hard_mode = cfg.get("hard_mode", False)
        v         = version
        n         = "all" if cfg["has_node"] else None
        if hard_mode:
            floor_arg = f
        else:
            floor_arg = "all" if m_arg == "anomaly" else f
        e = "all"
        force_floor = "floor_label" in cfg
        f_out = cfg.get("floor_label", f)
        db_mode = cfg.get("db_mode", mode)

        print(f"  [COST] {mode} mode={m_arg} v={v} nodes/floors=all hard_mode={hard_mode}")
        try:
            kwargs = {}
            if hard_mode:
                kwargs["hard_mode"] = hard_mode
            
            try:
                scraper = HonkaiStatistics_V2_eidolon_batch(
                    version=v,
                    floor=floor_arg,
                    node=n if n is not None else 0,
                    by_ed=None,
                    mode=m_arg,
                    **kwargs,
                )
            except TypeError:
                scraper = HonkaiStatistics_V2_eidolon_batch(
                    version=v,
                    floor=floor_arg,
                    node=n if n is not None else 0,
                    by_ed=None,
                    mode=m_arg,
                )
        except Exception as ex:
            print(f"  !!! Cost scraper init failed for {mode}: {ex}")
            return []

        return [
            (self._standardize(scraper.get_teams_df(), db_mode, v, e, f_out, n, era, force_floor=force_floor), f"{prefix}_by_cost_teams"),
            (self._standardize(scraper.get_archetypes_df(), db_mode, v, e, f_out, n, era, force_floor=force_floor), f"{prefix}_by_cost_archetypes"),
            (self._standardize(scraper.get_chars_by_cost_df(), db_mode, v, e, f_out, n, era, force_floor=force_floor), f"{prefix}_by_cost_chars"),
            (self._standardize(scraper.get_chars_by_individual_eidolons_df(), db_mode, v, e, f_out, n, era, force_floor=force_floor), f"{prefix}_by_cost_chars_by_eidolon"),
            (self._standardize(scraper.get_chars_by_cost_no_individual_eidolons_df(), db_mode, v, e, f_out, n, era, force_floor=force_floor), f"{prefix}_by_cost_chars_no_eidolon"),
            (self._standardize(scraper.get_duos_stats(), db_mode, v, e, f_out, n, era, force_floor=force_floor), f"{prefix}_by_cost_duos"),
            (self._standardize(scraper.get_combined_team_df(), db_mode, v, e, f_out, "Both", era, force_floor=force_floor), f"{prefix}_by_cost_dual_or_triple_teams"),
            (self._standardize(scraper.get_combined_archetype_df(), db_mode, v, e, f_out, "Both", era, force_floor=force_floor), f"{prefix}_by_cost_dual_or_triple_archetypes"),
        ]

    # ------------------------------------------------------------------
    def _db_save(self, conn, df, table):
        """Backward-compatible wrapper around _db_save_batch."""
        self._db_save_batch(conn, [(df, table)])

    # ------------------------------------------------------------------
    def _sort_table(self, conn, table):
        try:
            cols = {r[0] for r in conn.execute(f"DESCRIBE {table}").fetchall()}
            clause = sort_clause(cols)
            if not clause:
                return
            conn.execute(
                f"CREATE OR REPLACE TABLE {table} AS "
                f"SELECT * FROM {table} ORDER BY {clause}"
            )
        except Exception:
            pass

    # ------------------------------------------------------------------
    def _build_modern_scraper(self, cfg, v, e, f, n):
        cls = cfg["class"]
        kwargs = {}
        if "hard_mode" in cfg:
            kwargs["hard_mode"] = cfg["hard_mode"]
        if cfg["has_node"]:
            return cls(version=v, floor=f, by_ed=e, node=n, **kwargs)
        else:
            return cls(version=v, floor=f, by_ed=e, **kwargs)

    def _build_legacy_scraper(self, cfg, v, f, n):
        cls      = cfg["class"]
        mode_arg = cfg.get("mode_arg")
        if cfg["has_node"]:
            return cls(sub_mode=mode_arg, floor=f, node=n)
        else:
            return cls(sub_mode=mode_arg, floor=f)

    # ------------------------------------------------------------------
    def _process_cost(self, conn, mode, cfg, strategy="all_at_once", target_version=None):
        def specific_versions(c):
            if target_version:
                return [target_version]
            return c["versions"]

        if strategy == "all_at_once":
            saves = self._get_cost_saves(mode, cfg, version=target_version or "all")
            self._db_save_batch(conn, saves)
        elif strategy == "per_version":
            table_to_dfs = {}
            for v in specific_versions(cfg):
                saves = self._get_cost_saves(mode, cfg, version=v)
                for df, tbl in saves:
                    if df is not None and not df.is_empty():
                        table_to_dfs.setdefault(tbl, []).append(df)
                print(f"  [COST] version {v} collected for {mode}")

            final_saves = []
            for tbl, dfs in table_to_dfs.items():
                print("  [COST] Concatenating and saving table:", tbl)
                if dfs:
                    combined_df = pl.concat(dfs, how="diagonal_relaxed")
                    final_saves.append((combined_df, tbl))
            print("  [COST] All tables concatenated, saving to DB...")
            self._db_save_batch(conn, final_saves)
        else:
            raise ValueError(f"Unknown cost_strategy: {strategy!r}")

    # ------------------------------------------------------------------
    def _process_modern(self, conn, mode, cfg, v, e, f, n, eidolons):
        era = "MODERN"
        is_anomaly_family = mode in ("ANOMALY", "ANOMALY_HARD")
        force_floor = "floor_label" in cfg
        f_out = cfg.get("floor_label", f)
        db_mode = cfg.get("db_mode", mode)
        print(f"  [MODERN] {mode} v={v} e={e} floor={f} node={n}")
        try:
            scraper = self._build_modern_scraper(cfg, v, e, f, n)
        except Exception as ex:
            print(f"  !!! Scraper init failed: {ex}")
            return

        prefix = cfg["prefix"]

        modern_saves = [
            (self._standardize(scraper.get_char_df(), db_mode, v, e, f_out, n, era, is_char=True, force_floor=force_floor), "character_stats"),
            (self._standardize(scraper.get_archetype_df(), db_mode, v, e, f_out, n, era, force_floor=force_floor), f"{prefix}_stats_archetypes"),
            (self._standardize(scraper.get_eidolon_performance_df(), db_mode, v, e, f_out, n, era, force_floor=force_floor), f"{prefix}_stats_eidolon_performance"),
            (self._standardize(scraper.get_team_df(), db_mode, v, e, f_out, n, era, force_floor=force_floor), f"{prefix}_stats_teams"),
            (self._standardize(scraper.get_duos_stats(), db_mode, v, e, f_out, n, era, force_floor=force_floor), f"{prefix}_stats_duos"),
            (self._standardize(scraper.get_trios_stats(), db_mode, v, e, f_out, n, era, force_floor=force_floor), f"{prefix}_stats_trios"),
            (self._standardize(scraper.get_quos_stats(), db_mode, v, e, f_out, n, era, force_floor=force_floor), f"{prefix}_stats_quos"),
            (self._standardize(scraper.plot_statistics_all(cumulative=True, output=False), db_mode, v, e, f_out, n, era, force_floor=force_floor), f"{prefix}_stats_distributions"),
        ]

        if is_anomaly_family:
            combined_trigger = f in (0, "all")
            gear_trigger     = f in (0, 4, "all")
        else:
            combined_trigger = n in (0, "all")
            gear_trigger     = n in (0, "all")

        if combined_trigger:
            label  = "Both" if not is_anomaly_family else None
            suffix = "dual_or_triple" if not is_anomaly_family else "triple"
            print(f"  [MODERN] Combined {suffix.upper()} for {mode} v={v} e={e}")
            modern_saves.extend([
                (self._standardize(scraper.get_combined_archetype_df(), db_mode, v, e, f_out, label, era, force_floor=force_floor), f"{prefix}_stats_{suffix}_archetypes"),
                (self._standardize(scraper.get_combined_team_df(), db_mode, v, e, f_out, label, era, force_floor=force_floor), f"{prefix}_stats_{suffix}_teams"),
                (self._standardize(scraper.plot_statistics_all_combined(cumulative=True, output=False), db_mode, v, e, f_out, label, era, force_floor=force_floor), f"{prefix}_stats_{suffix}_distributions"),
            ])

        if gear_trigger:
            print(f"  [MODERN] Gear for {mode} v={v} e={e}")
            modern_saves.append(
                (self._standardize(scraper.display_top_gear(), db_mode, v, e, f_out, n, era, force_floor=force_floor), f"{prefix}_stats_gear_usage")
            )

        self._db_save_batch(conn, modern_saves)

    # ------------------------------------------------------------------
    def _process_legacy(self, conn, mode, cfg, v, f, n):
        era = "LEGACY"
        e   = 6
        print(f"  [LEGACY] {mode} v{v} Floor{f} Node{n}")
        try:
            scraper = self._build_legacy_scraper(cfg, v, f, n)
        except Exception as ex:
            print(f"  !!! Scraper init failed: {ex}")
            return

        prefix = cfg["prefix"]
        n = None

        legacy_saves = [
            (self._standardize(scraper.get_char_df(), mode, v, e, f, n, era, is_char=True), "character_stats"),
            (self._standardize(scraper.get_archetype_df(), mode, v, e, f, n, era), f"{prefix}_stats_archetypes"),
            (self._standardize(scraper.get_team_df(), mode, v, e, f, n, era), f"{prefix}_stats_teams"),
            (self._standardize(scraper.get_duos_stats(), mode, v, e, f, n, era), f"{prefix}_stats_duos"),
            (self._standardize(scraper.plot_statistics_all(output=False), mode, v, e, f, n, era), f"{prefix}_stats_distributions"),
        ]

        if n == 0 or v == "all" or n == "all":
            label  = "Both"
            suffix = "dual_or_triple"
            print(f"  [LEGACY] Combined {suffix.upper()} for {mode} v{v}")
            legacy_saves.extend([
                (self._standardize(scraper.get_combined_archetype_df(), mode, v, e, f, label, era), f"{prefix}_stats_{suffix}_archetypes"),
                (self._standardize(scraper.get_combined_team_df(), mode, v, e, f, label, era), f"{prefix}_stats_{suffix}_teams"),
                (self._standardize(scraper.plot_statistics_all_combined(output=False), mode, v, e, f, label, era), f"{prefix}_stats_{suffix}_distributions"),
            ])

        if n == 0 or v == "all" or n == "all":
            print(f"  [LEGACY] Gear for {mode} v{v}")
            legacy_saves.append(
                (self._standardize(scraper.display_top_gear(), mode, v, e, f, n, era), f"{prefix}_stats_gear_usage")
            )

        self._db_save_batch(conn, legacy_saves)

    # ------------------------------------------------------------------
    def orchestrate_update(
        self,
        target_mode=None,
        target_version=None,
        modern_strategy="all_at_once",
        legacy_strategy="all_at_once",
        cost_strategy="all_at_once",
        run_cost=True,
        do_legacy=True,
        run_builds=True,
        sort_tables=True,
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

        delete_modes = (modern_modes + legacy_modes) if do_legacy else modern_modes
        replace_modes = [m for m in delete_modes if self._strategy_for(m) == "replace"]
        add_modes = [m for m in delete_modes if m not in replace_modes]

        for mode in replace_modes:
            cfg = self.config[mode]
            version_to_replace = target_version or self._latest_version(cfg)
            if version_to_replace is None:
                continue
            self._delete_version(conn, version_to_replace, [mode])

        if replace_modes:
            print(f"  [REPLACE] removing rows for version "
                  f"{target_version or 'latest-per-gamemode'} in: {', '.join(replace_modes)}")
        if add_modes:
            print(f"  [ADD] appending without delete: {', '.join(add_modes)}")

        if run_cost:
            cost_modes_to_run = []
            if target_mode:
                if self.config[target_mode]["era"] != "LEGACY":
                    cost_key = target_mode + "_COST"
                    if cost_key in self.cost_config:
                        cost_modes_to_run = [cost_key]
                        if target_mode.upper() == "ANOMALY":
                            cost_modes_to_run.append("ANOMALY_HARD_COST")
                    else:
                        cost_modes_to_run = list(self.cost_config.keys())
            else:
                cost_modes_to_run = list(self.cost_config.keys())
            for mode in cost_modes_to_run:
                if cost_strategy == "all_at_once" and self._strategy_for(mode) != "replace":
                    print(f"  [ADD] {mode}: cost rows kept (strategy=add)")
                    continue
                self._delete_cost_rows(conn, mode, self.cost_config[mode], version=target_version)
        conn.commit()

        print("=" * 60)
        print(f"PASS 1: MODERN  [strategy={modern_strategy}]")
        print("=" * 60)
        self._orchestrate_modern(conn, modern_modes, modern_strategy, specific_versions, EIDOLONS)

        print("=" * 60)
        if do_legacy:
            print(f"PASS 2: LEGACY  [strategy={legacy_strategy}]")
            print("=" * 60)
            self._orchestrate_legacy(conn, legacy_modes, legacy_strategy, specific_versions)
        else:
            print("PASS 2: LEGACY  [skipped — do_legacy=False]")
            print("=" * 60)

        if run_cost:
            cost_scope = target_version or "all versions"
            print("=" * 60)
            print(f"PASS 3: COST-STRATIFIED  [{cost_scope}, all nodes/floors, strategy={cost_strategy}]")
            print("=" * 60)
            cost_modes_to_run = []
            if target_mode:
                if self.config[target_mode]["era"] != "LEGACY":
                    cost_key = target_mode + "_COST"
                    if cost_key in self.cost_config:
                        cost_modes_to_run = [cost_key]
                        if target_mode.upper() == "ANOMALY":
                            cost_modes_to_run.append("ANOMALY_HARD_COST")
                    else:
                        cost_modes_to_run = list(self.cost_config.keys())
            else:
                cost_modes_to_run = list(self.cost_config.keys())
            for mode in cost_modes_to_run:
                self._process_cost(conn, mode, self.cost_config[mode], strategy=cost_strategy, target_version=target_version)
                print(f"  [commit] {mode} cost pass done")
                conn.commit()

        print("=" * 60)
        if sort_tables:
            print("PASS 4: Sorting all tables (duckdb)")
            print("=" * 60)
            all_tables = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
            for (tbl,) in all_tables:
                print(f"  Sorting {tbl}...")
                self._sort_table(conn, tbl)
        else:
            print("PASS 4: Sorting skipped (deferred to the final incremental pass)")
            print("=" * 60)

        conn.commit()
        conn.close()
        print("Done.")

        if run_builds:
            print("=" * 60)
            print("PASS 5: BUILDS")
            print("=" * 60)
            builds = HonkaiStatistics_builds()
            builds.save_to_db()

    # ------------------------------------------------------------------
    def _latest_version(self, cfg):
        versions = cfg.get("versions") or []

        def key(v):
            try:
                return tuple(int(p) for p in str(v).split("."))
            except ValueError:
                return (0,)

        return max(versions, key=key) if versions else None

    # ------------------------------------------------------------------
    def _strategy_for(self, mode, default="add"):
        return get_mode_strategy(mode, default=default)

    # ------------------------------------------------------------------
    def _table_exists(self, conn, table):
        try:
            row = conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = 'main' AND table_name = ?",
                [table],
            ).fetchone()
            return row[0] > 0
        except Exception:
            return False

    # ------------------------------------------------------------------
    def _delete_version(self, conn, version, modes):
        for mode in modes:
            cfg     = self.config[mode]
            prefix  = cfg["prefix"]
            db_mode = cfg.get("db_mode", mode)
            tables  = [
                "character_stats",
                f"{prefix}_stats_archetypes",
                f"{prefix}_stats_eidolon_performance",
                f"{prefix}_stats_teams",
                f"{prefix}_stats_duos",
                f"{prefix}_stats_distributions",
                f"{prefix}_stats_dual_or_triple_archetypes",
                f"{prefix}_stats_dual_or_triple_teams",
                f"{prefix}_stats_dual_or_triple_distributions",
                f"{prefix}_stats_gear_usage",
            ]
            for tbl in tables:
                if not self._table_exists(conn, tbl):
                    continue
                if tbl == "character_stats":
                    conn.execute(
                        f"DELETE FROM {tbl} WHERE version = ? AND mode = ?",
                        [version, db_mode],
                    )
                else:
                    conn.execute(f"DELETE FROM {tbl} WHERE version = ?", [version])
            print(f"  [REPLACE] {mode}: removed version {version}")

    # ------------------------------------------------------------------
    def _delete_cost_rows(self, conn, mode, cfg, version=None):
        prefix  = cfg["prefix"]
        db_mode = cfg.get("db_mode", mode)
        tables  = [
            f"{prefix}_by_cost_teams",
            f"{prefix}_by_cost_archetypes",
            f"{prefix}_by_cost_chars",
            f"{prefix}_by_cost_chars_by_eidolon",
            f"{prefix}_by_cost_duos",
            f"{prefix}_by_cost_dual_or_triple_teams",
            f"{prefix}_by_cost_dual_or_triple_archetypes",
        ]
        for tbl in tables:
            if not self._table_exists(conn, tbl):
                continue
            if version is None:
                conn.execute(f"DELETE FROM {tbl} WHERE mode = ?", [db_mode])
            else:
                conn.execute(
                    f"DELETE FROM {tbl} WHERE version = ? AND mode = ?",
                    [version, db_mode],
                )
        scope = "all rows" if version is None else f"version {version}"
        print(f"  [REPLACE] {mode}: removed by-cost rows ({scope})")
    
    # ------------------------------------------------------------------
    def incremental_update(
        self,
        target_version=None,
        target_mode=None,
        do_legacy=False,
        run_cost=True,
    ):
        modes_to_run = [target_mode] if target_mode else list(self.config.keys())
        jobs = []
        for mode in modes_to_run:
            cfg = self.config[mode]
            if cfg["era"] == "LEGACY" and not do_legacy:
                continue
            v = target_version or self._latest_version(cfg)
            if v is not None:
                jobs.append((mode, v))

        if not jobs:
            print("No versions to process.")
            return

        try:
            snapshot()
        except Exception as ex:
            print(f"  [snapshot] warning: could not snapshot DB: {ex}")

        for i, (mode, v) in enumerate(jobs):
            print("=" * 60)
            print(f"INCREMENTAL: {mode} -> version {v}")
            print("=" * 60)
            self.orchestrate_update(
                target_mode=mode,
                target_version=v,
                modern_strategy="per_version",
                legacy_strategy="per_version",
                cost_strategy="per_version",
                run_cost=run_cost,
                do_legacy=do_legacy,
                run_builds=(i == len(jobs) - 1),
                sort_tables=(i == len(jobs) - 1),
            )

    # ------------------------------------------------------------------
    def _orchestrate_modern(self, conn, modern_modes, strategy, specific_versions, EIDOLONS):
        for mode in modern_modes:
            cfg = self.config[mode]

            if mode == "ANOMALY":
                floors = [0, 4, "all"]
                nodes  = [None]
            else:
                floors = [cfg["floor"]]
                nodes  = [0, 1, 2] if cfg["has_node"] else [None]

            if strategy == "all_at_once":
                if mode == "ANOMALY":
                    self._process_modern(conn, mode, cfg, v="all", e="all", f="all", n=None, eidolons="all")
                else:
                    for f in floors:
                        self._process_modern(conn, mode, cfg, v="all", e="all", f=f, n="all", eidolons="all")
                conn.commit()

            elif strategy == "per_version":
                table_to_dfs = {}
                for v in specific_versions(cfg):
                    if mode == "ANOMALY":
                        saves = self._get_modern_saves(mode, cfg, v=v, e="all", f="all", n=None)
                    else:
                        saves = []
                        for f in floors:
                            saves.extend(self._get_modern_saves(mode, cfg, v=v, e="all", f=f, n="all"))
                    
                    version_immediate_saves = []
                    for df, tbl in saves:
                        if df is not None and not df.is_empty():
                            if tbl.endswith("_stats_quos") or tbl.endswith("_stats_trios"):
                                version_immediate_saves.append((df, tbl))
                            else:
                                table_to_dfs.setdefault(tbl, []).append(df)
                    
                    if version_immediate_saves:
                        print(f"  [MODERN] Saving quos/trios immediately for version {v}...")
                        self._db_save_batch(conn, version_immediate_saves)

                    print(f"  [MODERN] version {v} collected")

                final_saves = []
                for tbl, dfs in table_to_dfs.items():
                    print("  [MODERN] Concatenating and saving table:", tbl)
                    if dfs:
                        combined_df = pl.concat(dfs, how="diagonal_relaxed")
                        final_saves.append((combined_df, tbl))
                
                if final_saves:
                    print("  [MODERN] All remaining tables concatenated, saving to DB...")
                    self._db_save_batch(conn, final_saves)
                
                conn.commit()
                print(f"  [commit] {mode} per_version batch done (quos/trios saved incrementally)")

            elif strategy == "per_node":
                if mode == "ANOMALY":
                    for f in [0, 4]:
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
    platform.orchestrate_update(target_mode='Anomaly', run_cost=True, sort_tables=False, modern_strategy="per_version", run_builds=False, do_legacy=False)