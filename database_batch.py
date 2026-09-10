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
from Appearance_rate_V2_batch_all_modes_by_cost import HonkaiStatistics_V2_eidolon_batch
from snapshot import snapshot
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
        # -----------------------------------------------------------------
        # Cost-stratified mode config (HonkaiStatistics_V2_eidolon_batch)
        # era="MODERN" — uses same parquet files as modern modes.
        # All versions and all nodes/floors are always aggregated together.
        # Results go into separate *_by_cost tables.
        # -----------------------------------------------------------------
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
            # ---- Hard-mode anomaly, floor 4 (stored as floor=5 to avoid colliding
            #      with the normal, non-hard floor=4 rows) ---------------------
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
            # ---- Hard-mode anomaly, floor 4 (stored as floor=5 to avoid colliding
            #      with the normal, non-hard floor=4 rows) ---------------------
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
    def _standardize(self, df, mode, v, e, f, n, era, is_char=False, force_floor=False):
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

        node_val = None if (n is None or mode in ("ANOMALY", "ANOMALY_HARD")) else str(n)

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

        # 2. Filter the dictionary to only include keys NOT already in df.columns.
        # EXCEPTION: 'floor' is force-overwritten when force_floor=True — the scraper
        # already returns a real 'floor' column (e.g. the actual queried floor=4 for
        # hard-mode anomaly), but we want to relabel it (e.g. to floor=5) so it
        # doesn't collide with the normal, non-hard rows for that same real floor.
        missing_cols = [
            expr.alias(col_name) 
            for col_name, expr in potential_cols.items() 
            if col_name not in df.columns or (col_name == 'floor' and force_floor)
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
        """Replace a table's contents with a version-sorted copy (polars).

        Reads the table through duckdb into a polars DataFrame, sorts it with
        polars (version DESC, at_eidolon_level ASC, up_to_eidolon_level DESC,
        node DESC), and writes the sorted frame back. Columns absent from a
        table are skipped, so non-stats tables are left untouched.
        """
        try:
            df = conn.execute(f"SELECT * FROM {table}").pl()
            if df.is_empty():
                return
            sort_cols = ["version", "at_eidolon_level", "up_to_eidolon_level", "node"]
            present = [c for c in sort_cols if c in df.columns]
            if not present:
                return
            descending = [c in ("version", "up_to_eidolon_level", "node") for c in present]
            df = df.sort(present, descending=descending, nulls_last=True)
            conn.register("temp_sort", df)
            conn.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM temp_sort")
            conn.unregister("temp_sort")
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
            return cls(sub_mode=mode_arg,floor=f, node=n)
        else:
            return cls(sub_mode=mode_arg,floor=f)

    # ------------------------------------------------------------------
    def _process_cost(self, conn, mode, cfg, version="all"):
        """Run the cost-stratified batch for one mode (all nodes/floors).

        version="all" -> the scraper loads every version and aggregates across
            them (used by full rebuilds).
        version=<v>   -> only that version's parquet is loaded, so just its
            rows are computed and appended (used by incremental_update so the
            by-cost tables increment too instead of recomputing all versions).
        """
        era       = "MODERN"
        prefix    = cfg["prefix"]
        m_arg     = cfg["mode_arg"]
        f         = cfg["floor"]
        hard_mode = cfg.get("hard_mode", False)
        # "all" aggregates every version; a specific version limits the load.
        # Node/floor are still all within that version.
        v = version
        n = "all" if cfg["has_node"] else None
        # Hard-mode anomaly pass targets one specific floor (e.g. 4), so it
        # should NOT be widened to "all" the way the normal anomaly pass is.
        if hard_mode:
            floor_arg = f
        else:
            floor_arg = "all" if m_arg == "anomaly" else f
        e = "all"   # eidolon sentinel — cost scraper handles eidolon internally
        # floor_label lets us write a different floor value into the DB than the
        # one used to query the data (e.g. hard-mode floor 4 stored as floor 5,
        # so it doesn't collide with the normal, non-hard floor=4 rows). force_floor
        # tells _standardize to overwrite the scraper's own 'floor' column instead
        # of leaving it as-is (which is what happens by default).
        force_floor = "floor_label" in cfg
        f_out = cfg.get("floor_label", f)
        # db_mode lets a config's DB-written 'mode' literal differ from its own
        # orchestration key — kept consistent with the modern-stats pass, though
        # the by-cost tables aren't currently filtered on 'mode' downstream.
        db_mode = cfg.get("db_mode", mode)

        print(f"  [COST] {mode} mode={m_arg} v={v} nodes/floors=all hard_mode={hard_mode}")
        try:
            scraper = HonkaiStatistics_V2_eidolon_batch(
                version=v,
                floor=floor_arg,
                node=n if n is not None else 0,
                by_ed=None,
                mode=m_arg,
                hard_mode=hard_mode,
            )
        except Exception as ex:
            print(f"  !!! Cost scraper init failed for {mode}: {ex}")
            return

        # Per-node/floor tables
        self._db_save(conn,
            self._standardize(scraper.get_teams_df(), db_mode, v, e, f_out, n, era, force_floor=force_floor),
            f"{prefix}_by_cost_teams")
        self._db_save(conn,
            self._standardize(scraper.get_archetypes_df(), db_mode, v, e, f_out, n, era, force_floor=force_floor),
            f"{prefix}_by_cost_archetypes")
        self._db_save(conn,
            self._standardize(scraper.get_chars_by_cost_df(), db_mode, v, e, f_out, n, era, force_floor=force_floor),
            f"{prefix}_by_cost_chars")
        self._db_save(conn,
            self._standardize(scraper.get_chars_by_individual_eidolons_df(), db_mode, v, e, f_out, n, era, force_floor=force_floor),
            f"{prefix}_by_cost_chars_by_eidolon")
        self._db_save(conn,
            self._standardize(scraper.get_duos_stats(), db_mode, v, e, f_out, n, era, force_floor=force_floor),
            f"{prefix}_by_cost_duos")

        # Combined (cross-node) tables
        print(f"  [COST] Combined tables for {mode}")
        self._db_save(conn,
            self._standardize(scraper.get_combined_team_df(), db_mode, v, e, f_out, "Both", era, force_floor=force_floor),
            f"{prefix}_by_cost_dual_or_triple_teams")
        self._db_save(conn,
            self._standardize(scraper.get_combined_archetype_df(), db_mode, v, e, f_out, "Both", era, force_floor=force_floor),
            f"{prefix}_by_cost_dual_or_triple_archetypes")

    # ------------------------------------------------------------------
    def _process_modern(self, conn, mode, cfg, v, e, f, n, eidolons):
        era = "MODERN"
        is_anomaly_family = mode in ("ANOMALY", "ANOMALY_HARD")
        # floor_label lets us write a different floor value into the DB than the
        # one used to query the data (e.g. hard-mode floor 4 stored as floor 5,
        # so it doesn't collide with the normal, non-hard floor=4 rows). force_floor
        # tells _standardize to overwrite the scraper's own 'floor' column instead
        # of leaving it as-is (which is what happens by default).
        force_floor = "floor_label" in cfg
        f_out = cfg.get("floor_label", f)
        # db_mode lets a config's DB-written 'mode' literal differ from its own
        # orchestration key — e.g. ANOMALY_HARD is a distinct config for scraper
        # setup, but its rows still need mode="ANOMALY" so they show up alongside
        # the rest of the anomaly section (distinguished only by floor=5).
        db_mode = cfg.get("db_mode", mode)
        print(f"  [MODERN] {mode} v={v} e={e} floor={f} node={n}")
        try:
            scraper = self._build_modern_scraper(cfg, v, e, f, n)
        except Exception as ex:
            print(f"  !!! Scraper init failed: {ex}")
            return

        prefix = cfg["prefix"]

        self._db_save(conn,
            self._standardize(scraper.get_char_df(), db_mode, v, e, f_out, n, era, is_char=True, force_floor=force_floor),
            "character_stats")
        self._db_save(conn,
            self._standardize(scraper.get_archetype_df(), db_mode, v, e, f_out, n, era, force_floor=force_floor),
            f"{prefix}_stats_archetypes")
        self._db_save(conn,
            self._standardize(scraper.get_eidolon_performance_df(), db_mode, v, e, f_out, n, era, force_floor=force_floor),
            f"{prefix}_stats_eidolon_performance")
        self._db_save(conn,
            self._standardize(scraper.get_team_df(), db_mode, v, e, f_out, n, era, force_floor=force_floor),
            f"{prefix}_stats_teams")
        self._db_save(conn,
            self._standardize(scraper.get_duos_stats(), db_mode, v, e, f_out, n, era, force_floor=force_floor),
            f"{prefix}_stats_duos")
        self._db_save(conn,
            self._standardize(
                scraper.plot_statistics_all(cumulative=True, output=False),
                db_mode, v, e, f_out, n, era, force_floor=force_floor),
            f"{prefix}_stats_distributions")

        # For non-ANOMALY: combined triggers on node=0/"all"
        # For ANOMALY family: combined triggers on floor=0/"all" (floor is the equivalent axis).
        # Hard-mode anomaly (floor=4, hard_mode=True) is a single fixed floor, so it
        # never triggers the triple-floor combined pass, only the gear pass.
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
            self._db_save(conn,
                self._standardize(scraper.get_combined_archetype_df(), db_mode, v, e, f_out, label, era, force_floor=force_floor),
                f"{prefix}_stats_{suffix}_archetypes")
            self._db_save(conn,
                self._standardize(scraper.get_combined_team_df(), db_mode, v, e, f_out, label, era, force_floor=force_floor),
                f"{prefix}_stats_{suffix}_teams")
            self._db_save(conn,
                self._standardize(
                    scraper.plot_statistics_all_combined(cumulative=True, output=False),
                    db_mode, v, e, f_out, label, era, force_floor=force_floor),
                f"{prefix}_stats_{suffix}_distributions")

        if gear_trigger:
            print(f"  [MODERN] Gear for {mode} v={v} e={e}")
            self._db_save(conn,
                self._standardize(scraper.display_top_gear(), db_mode, v, e, f_out, n, era, force_floor=force_floor),
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
                self._standardize(scraper.display_top_gear(), mode, v, e, f,n, era),
                f"{prefix}_stats_gear_usage")

    # ------------------------------------------------------------------
    def orchestrate_update(
        self,
        target_mode=None,
        target_version=None,
        modern_strategy="all_at_once",  # all_at_once | per_version | per_node | per_eidolon | granular
        legacy_strategy="all_at_once",  # all_at_once | per_version
        run_cost=True,                  # whether to run the cost-stratified pass
        do_legacy=True,                 # whether to run the LEGACY pass
        run_builds=True,                # whether to run the builds pass at the end
        sort_tables=True,               # whether to re-sort every table at the end
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
        # REPLACE MODE — .env keyword: DB_STRATEGY=replace
        # When set, the version being added is deleted from every gamemode
        # table first (if present), so re-adding replaces the old rows instead
        # of duplicating them. Without target_version, each gamemode's newest
        # version is the one replaced.
        # ------------------------------------------------------------------
        replace_mode = os.getenv("DB_STRATEGY", "").strip().lower() == "replace"
        if replace_mode:
            delete_modes = (modern_modes + legacy_modes) if do_legacy else modern_modes
            for mode in delete_modes:
                cfg = self.config[mode]
                version_to_replace = target_version or self._latest_version(cfg)
                if version_to_replace is None:
                    continue
                self._delete_version(conn, version_to_replace, [mode])

            # by-cost tables carry per-version rows — delete the replaced
            # version's rows (or the whole mode when no target_version) so the
            # cost pass re-adds them fresh instead of duplicating.
            if run_cost:
                if target_mode:
                    cost_key = target_mode + "_COST"
                    cost_modes_to_run = [cost_key] if cost_key in self.cost_config else list(self.cost_config.keys())
                else:
                    cost_modes_to_run = list(self.cost_config.keys())
                for mode in cost_modes_to_run:
                    self._delete_cost_rows(conn, mode, self.cost_config[mode], version=target_version)
            conn.commit()
            print(f"  [REPLACE] removed existing rows for version "
                  f"{target_version or 'latest-per-gamemode'}")

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
        if do_legacy:
            print(f"PASS 2: LEGACY  [strategy={legacy_strategy}]")
            print("=" * 60)
            self._orchestrate_legacy(conn, legacy_modes, legacy_strategy, specific_versions)
        else:
            print("PASS 2: LEGACY  [skipped — do_legacy=False]")
            print("=" * 60)

        # ------------------------------------------------------------------
        # PASS 3 — COST-STRATIFIED
        # ------------------------------------------------------------------
        if run_cost:
            cost_scope = target_version or "all versions"
            print("=" * 60)
            print(f"PASS 3: COST-STRATIFIED  [{cost_scope}, all nodes/floors]")
            print("=" * 60)
            # Determine which cost modes to run
            cost_modes_to_run = []
            if target_mode:
                # A legacy target mode owns no cost tables — skip rather than
                # falling back to every cost mode (which would re-append all).
                if self.config[target_mode]["era"] != "LEGACY":
                    cost_key = target_mode + "_COST"
                    cost_modes_to_run = [cost_key] if cost_key in self.cost_config else list(self.cost_config.keys())
            else:
                cost_modes_to_run = list(self.cost_config.keys())
            for mode in cost_modes_to_run:
                self._process_cost(conn, mode, self.cost_config[mode], version=target_version or "all")
                print(f"  [commit] {mode} cost pass done")
                conn.commit()

        # ------------------------------------------------------------------
        # PASS 4 — Sort (polars)
        # ------------------------------------------------------------------
        print("=" * 60)
        if sort_tables:
            print("PASS 4: Sorting all tables (polars)")
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

        # ------------------------------------------------------------------
        # PASS 5 — Builds
        # ------------------------------------------------------------------
        if run_builds:
            print("=" * 60)
            print("PASS 5: BUILDS")
            print("=" * 60)
            builds = HonkaiStatistics_builds()
            builds.save_to_db()

    # ------------------------------------------------------------------
    def _latest_version(self, cfg):
        """Newest version in a config's list, by numeric (major, minor) order —
        works whether the list is stored oldest-first or newest-first."""
        versions = cfg.get("versions") or []

        def key(v):
            try:
                return tuple(int(p) for p in str(v).split("."))
            except ValueError:
                return (0,)

        return max(versions, key=key) if versions else None

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
        """Delete every row for `version` from all tables owned by `modes`.

        Stats tables are prefix-scoped; character_stats is shared across modes
        and filtered by its mode column instead.
        """
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
        """Delete rows of one cost-stratified mode.

        version=None -> delete every row of the mode (full refresh).
        version=<v>  -> delete only that version's rows, so an incremental
            replace re-adds just that version instead of re-adding all.
        """
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
        """Incremental addition — append only the newest version's rows instead
        of rebuilding the whole database.

        - Reads .env: setting DB_STRATEGY=replace deletes the version being
          added from every gamemode first, so re-adding refreshes those rows
          instead of duplicating them (the same version's by-cost rows are
          refreshed too).
        - do_legacy=False (default) skips the slow legacy pass, which rarely
          changes once written.
        - The by-cost pass increments too: it loads only the target version's
          rows (not all versions), matching the modern-stats pass.
        - Sorting happens once, after the last mode's pass, instead of re-sorting
          every table after every mode.
        - target_version defaults to each gamemode's newest version.
        """
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

        # Snapshot the DB before any writes so a bad run can be undone.
        # Non-fatal: if we can't back up, the append itself can still proceed.
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
                run_cost=run_cost,
                do_legacy=do_legacy,
                run_builds=(i == len(jobs) - 1),    # builds once, after the last mode
                sort_tables=(i == len(jobs) - 1),   # sort once, after the last mode
            )

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
    platform.incremental_update(target_version="4.5.1", run_cost=True)

    
