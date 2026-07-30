import os
import duckdb
import polars as pl
from dotenv import load_dotenv

load_dotenv()


# =====================================================================
# Base Analyzer Class (Shared Lazy Aggregations)
# =====================================================================
class BaseCostMetaAnalyzer:
    def __init__(self, db_name=os.getenv("DB_File")):
        self.db_path = db_name

    @staticmethod
    def _apply_base_filters(lazy_df: pl.LazyFrame, task: dict) -> pl.LazyFrame:
        """Filters out non-positive sample rows, incorrect floors, and non-target nodes."""
        filtered = lazy_df.filter(
            (pl.col("Samples") > 0) & (pl.col("floor").is_in(task["floors"]))
        )
        if task["node_col"] is not None:
            node_val = str(task["node_val"]).strip("'")
            filtered = filtered.filter(
                pl.col(task["node_col"]).cast(pl.String) == node_val
            )
        return filtered

    @staticmethod
    def _get_common_agg_exprs(
        task: dict, usage_col: str, is_history: bool
    ) -> list[pl.Expr]:
        """Generates common score and metadata aggregations across all cost tables."""
        best_score_expr = (
            pl.col("Average_Score").min()
            if task["perf"] == "MIN"
            else pl.col("Average_Score").max()
        )

        exprs = []

        # Conditionally add List aggregations ONLY for full-history runs
        if is_history:
            exprs.extend(
                [
                    pl.col(usage_col).alias("Appearance_pct_List") ,
                    pl.col("Min_Score").alias("Min_Score_List"),
                    pl.col("Average_Score").alias("Average_Score_List"),
                    pl.col("Max_Score").alias("Max_Score_List"),
                    pl.col("Median_Score").alias("Median_Score_List"),
                    pl.col("Samples").alias("Samples_List"),
                    pl.col("Full_Clear_Rate_pct").alias("Full_Star_Rate_pct_List"),
                ]
            )
          
            

        # Common metric aggregations
        exprs.extend(
            [
                pl.col(usage_col).mean().round(2).alias("Simple_Avg_Appearance"),
                pl.col("Min_Score").min().round(2).alias("Min_Score"),
                pl.col("Average_Score").mean().round(2).alias("Simple_Avg_Score"),
                (
                    (pl.col("Average_Score") * pl.col("Samples")).sum()
                    / pl.col("Samples").sum()
                )
                .round(2)
                .alias("Weighted_Avg_Score"),
                (
                    (pl.col("Median_Score") * pl.col("Samples")).sum()
                    / pl.col("Samples").sum()
                )
                .round(2)
                .alias("Weighted_Avg_Median"),
                best_score_expr.round(2).alias("Best_Version_Avg"),
                pl.col("Max_Score").max().round(2).alias("Max_Score"),
                pl.col("Total_Full_Clears").sum().alias("Total_Full_Clears"),
                pl.col("Samples").sum().alias("Total_Samples"),
                (
                    100.0
                    * pl.col("Total_Full_Clears").sum()
                    / pl.col("Samples").sum()
                )
                .round(2)
                .alias("Full_Star_Rate_pct"),
                pl.col("version").n_unique().alias("Version_Count"),
                pl.col("version")
                .unique()
                .sort(descending=True)
                .cast(pl.String)
                .str.join(", ")
                .alias("Versions_Used"),
            ]
        )
        return exprs


# =====================================================================
# 1. HonkaiCostTeamMetaAnalyzer
# =====================================================================
class HonkaiCostTeamMetaAnalyzer(BaseCostMetaAnalyzer):
    def __init__(self, db_name=os.getenv("DB_File")):
        super().__init__(db_name)
        self.tasks = [
            {"mode": "MOC", "table": "moc_by_cost_teams", "floors": [10, 12], "perf": "MIN", "node_col": "node", "node_val": "0"},
            {"mode": "APOC", "table": "apoc_by_cost_teams", "floors": [4], "perf": "MAX", "node_col": "node", "node_val": "'0'"},
            {"mode": "PURE_FICTION", "table": "pure_fiction_by_cost_teams", "floors": [4], "perf": "MAX", "node_col": "node", "node_val": "'0'"},
            {"mode": "ANOMALY_F0", "table": "anomaly_by_cost_teams", "floors": [0], "perf": "MIN", "node_col": None, "node_val": None},
            {"mode": "ANOMALY_F4", "table": "anomaly_by_cost_teams", "floors": [4], "perf": "MIN", "node_col": None, "node_val": None},
            {"mode": "ANOMALY_F5", "table": "anomaly_by_cost_teams", "floors": [5], "perf": "MIN", "node_col": None, "node_val": None},
        ]

    def _aggregate(self, raw_lazy: pl.LazyFrame, task: dict, limit_recent: bool = False, is_history: bool = False) -> pl.DataFrame:
        lazy = self._apply_base_filters(raw_lazy, task)

        if limit_recent:
            recent_versions = (
                lazy.select("version")
                .unique()
                .sort("version", descending=True)
                .limit(3)
                .collect()["version"]
            )
            lazy = lazy.filter(pl.col("version").is_in(recent_versions))

        lazy = lazy.with_columns(pl.lit(task["mode"]).alias("Game_Mode"))

        group_cols = [
            "Game_Mode", "at_eidolon_level", "up_to_eidolon_level", "Team",
            "Archetype_Core", "estimated_min_cost", "estimated_max_cost",
            "max_eidolon", "has_sustain"
        ]

        agg_exprs = self._get_common_agg_exprs(task, usage_col="Appearance_Rate_pct", is_history=is_history)
        return lazy.group_by(group_cols).agg(agg_exprs).collect()

    def _aggregate_rolling(self, raw_lazy: pl.LazyFrame, task: dict, window: int = 3) -> pl.DataFrame:
        lazy = self._apply_base_filters(raw_lazy, task)

        # Rank versions
        versions_df = (
            lazy.select("version")
            .unique()
            .sort("version")
            .with_columns(pl.int_range(0, pl.len()).alias("vrank"))
            .collect()
        )

        if versions_df.is_empty():
            return pl.DataFrame()

        base = lazy.join(versions_df.lazy(), on="version")

        # Use how="cross" instead of how="inner" with pl.TRUE
        as_of_df = versions_df.rename({"version": "As_Of_Version", "vrank": "asof_vrank"}).lazy()
        joined = base.join(as_of_df, how="cross").filter(
            (pl.col("vrank") <= pl.col("asof_vrank")) &
            (pl.col("vrank") >= (pl.col("asof_vrank") - (window - 1)))
        )

        joined = joined.with_columns(
            pl.lit(task["mode"]).alias("Game_Mode"),
            (pl.col("asof_vrank").max() - pl.col("asof_vrank") + 1).alias("Version_Group_Num")
        )

        group_cols = [
            "As_Of_Version", "Game_Mode", "at_eidolon_level", "up_to_eidolon_level",
            "Team", "Archetype_Core", "estimated_min_cost", "estimated_max_cost",
            "max_eidolon", "has_sustain"
        ]

        agg_exprs = [pl.col("Version_Group_Num").first().alias("Version_Group_Num")]
        agg_exprs.extend(self._get_common_agg_exprs(task, usage_col="Appearance_Rate_pct", is_history=False))

        return joined.group_by(group_cols).agg(agg_exprs).collect()

    def run_analysis(self):
        con = duckdb.connect(self.db_path)
        all_history, all_recent, all_rolling = [], [], []

        print(f"Starting By-Cost Team Meta Analysis on {self.db_path}...")

        for task in self.tasks:
            try:
                raw_lazy = con.execute(f"SELECT * FROM {task['table']}").pl().lazy()

                df_h = self._aggregate(raw_lazy, task, limit_recent=False, is_history=True)
                if not df_h.is_empty():
                    all_history.append(df_h)

                df_r = self._aggregate(raw_lazy, task, limit_recent=True, is_history=False)
                if not df_r.is_empty():
                    all_recent.append(df_r)

                df_roll = self._aggregate_rolling(raw_lazy, task, window=3)
                if not df_roll.is_empty():
                    all_rolling.append(df_roll)

                print(f"  + {task['mode']:15s}  history={len(df_h):,}  recent={len(df_r):,}  rolling={len(df_roll):,}")
            except Exception as e:
                print(f"  ! Error on {task['mode']}: {e}")

        self._write_tables(con, all_history, all_recent, all_rolling, "team")
        con.close()

    def _write_tables(self, con, history, recent, rolling, prefix):
        if not history and not recent and not rolling:
            print("No data found. Exiting.")
            return

        con.execute("BEGIN TRANSACTION")
        try:
            if history:
                full_df = pl.concat(history, how="diagonal")
                con.execute(f"DROP TABLE IF EXISTS by_cost_{prefix}_meta_summary")
                con.execute(f"CREATE TABLE by_cost_{prefix}_meta_summary AS SELECT * FROM full_df")
                print(f"\n  Wrote by_cost_{prefix}_meta_summary     ({len(full_df):,} rows)")

            if recent:
                recent_df = pl.concat(recent, how="diagonal")
                con.execute(f"DROP TABLE IF EXISTS by_cost_{prefix}_recent_meta_summary")
                con.execute(f"CREATE TABLE by_cost_{prefix}_recent_meta_summary AS SELECT * FROM recent_df")
                print(f"  Wrote by_cost_{prefix}_recent_meta_summary ({len(recent_df):,} rows)")

            if rolling:
                rolling_df = pl.concat(rolling, how="diagonal")
                con.execute(f"DROP TABLE IF EXISTS by_cost_{prefix}_rolling_meta_summary")
                con.execute(f"CREATE TABLE by_cost_{prefix}_rolling_meta_summary AS SELECT * FROM rolling_df")
                print(f"  Wrote by_cost_{prefix}_rolling_meta_summary ({len(rolling_df):,} rows)")

            con.execute("COMMIT")
            print(f"\n>>> Analysis complete for {prefix}.")
        except Exception as e:
            con.execute("ROLLBACK")
            print(f"\n>>> Error during DB write: {e}")


# =====================================================================
# 2. HonkaiCostArchetypeMetaAnalyzer
# =====================================================================
class HonkaiCostArchetypeMetaAnalyzer(BaseCostMetaAnalyzer):
    def __init__(self, db_name=os.getenv("DB_File")):
        super().__init__(db_name)
        self.tasks = [
            {"mode": "MOC", "table": "moc_by_cost_archetypes", "floors": [10, 12], "perf": "MIN", "node_col": "node", "node_val": "0"},
            {"mode": "APOC", "table": "apoc_by_cost_archetypes", "floors": [4], "perf": "MAX", "node_col": "node", "node_val": "'0'"},
            {"mode": "PURE_FICTION", "table": "pure_fiction_by_cost_archetypes", "floors": [4], "perf": "MAX", "node_col": "node", "node_val": "'0'"},
            {"mode": "ANOMALY_F0", "table": "anomaly_by_cost_archetypes", "floors": [0], "perf": "MIN", "node_col": None, "node_val": None},
            {"mode": "ANOMALY_F4", "table": "anomaly_by_cost_archetypes", "floors": [4], "perf": "MIN", "node_col": None, "node_val": None},
            {"mode": "ANOMALY_F5", "table": "anomaly_by_cost_archetypes", "floors": [5], "perf": "MIN", "node_col": None, "node_val": None},
        ]

    def _get_archetype_exprs(self, task: dict, is_history: bool) -> list[pl.Expr]:
        exprs = self._get_common_agg_exprs(task, usage_col="Usage_pct", is_history=is_history)
        exprs.extend([
            pl.col("Sustain_Samples").sum().alias("Total_Sustain_Samples"),
            (100.0 * pl.col("Sustain_Samples").sum() / pl.col("Samples").sum()).round(2).alias("Sustain_Rate_pct"),
            pl.col("Sustain_Percentage").alias("Sustain_Percentage_List")
        ])
        return exprs

    def _aggregate(self, raw_lazy: pl.LazyFrame, task: dict, limit_recent: bool = False, is_history: bool = False) -> pl.DataFrame:
        lazy = self._apply_base_filters(raw_lazy, task)

        if limit_recent:
            recent_versions = (
                lazy.select("version")
                .unique()
                .sort("version", descending=True)
                .limit(3)
                .collect()["version"]
            )
            lazy = lazy.filter(pl.col("version").is_in(recent_versions))

        lazy = lazy.with_columns(pl.lit(task["mode"]).alias("Game_Mode"))

        group_cols = [
            "Game_Mode", "at_eidolon_level", "up_to_eidolon_level",
            "Archetype_Core", "estimated_min_cost", "estimated_max_cost", "max_eidolon"
        ]

        return lazy.group_by(group_cols).agg(self._get_archetype_exprs(task, is_history=is_history)).collect()

    def _aggregate_rolling(self, raw_lazy: pl.LazyFrame, task: dict, window: int = 3) -> pl.DataFrame:
        lazy = self._apply_base_filters(raw_lazy, task)

        versions_df = (
            lazy.select("version")
            .unique()
            .sort("version")
            .with_columns(pl.int_range(0, pl.len()).alias("vrank"))
            .collect()
        )

        if versions_df.is_empty():
            return pl.DataFrame()

        base = lazy.join(versions_df.lazy(), on="version")

        as_of_df = versions_df.rename({"version": "As_Of_Version", "vrank": "asof_vrank"}).lazy()
        joined = base.join(as_of_df, how="cross").filter(
            (pl.col("vrank") <= pl.col("asof_vrank")) &
            (pl.col("vrank") >= (pl.col("asof_vrank") - (window - 1)))
        )

        joined = joined.with_columns(
            pl.lit(task["mode"]).alias("Game_Mode"),
            (pl.col("asof_vrank").max() - pl.col("asof_vrank") + 1).alias("Version_Group_Num")
        )

        group_cols = [
            "As_Of_Version", "Game_Mode", "at_eidolon_level", "up_to_eidolon_level",
            "Archetype_Core", "estimated_min_cost", "estimated_max_cost", "max_eidolon"
        ]

        agg_exprs = [pl.col("Version_Group_Num").first().alias("Version_Group_Num")]
        agg_exprs.extend(self._get_archetype_exprs(task, is_history=False))

        return joined.group_by(group_cols).agg(agg_exprs).collect()

    def run_analysis(self):
        con = duckdb.connect(self.db_path)
        all_history, all_recent, all_rolling = [], [], []

        print(f"Starting By-Cost Archetype Meta Analysis on {self.db_path}...")

        for task in self.tasks:
            try:
                raw_lazy = con.execute(f"SELECT * FROM {task['table']}").pl().lazy()

                df_h = self._aggregate(raw_lazy, task, limit_recent=False, is_history=True)
                if not df_h.is_empty():
                    all_history.append(df_h)

                df_r = self._aggregate(raw_lazy, task, limit_recent=True, is_history=False)
                if not df_r.is_empty():
                    all_recent.append(df_r)

                df_roll = self._aggregate_rolling(raw_lazy, task, window=3)
                if not df_roll.is_empty():
                    all_rolling.append(df_roll)

                print(f"  + {task['mode']:15s}  history={len(df_h):,}  recent={len(df_r):,}  rolling={len(df_roll):,}")
            except Exception as e:
                print(f"  ! Error on {task['mode']}: {e}")

        HonkaiCostTeamMetaAnalyzer._write_tables(self, con, all_history, all_recent, all_rolling, "archetype")
        con.close()


# =====================================================================
# 3. HonkaiCostCharacterMetaAnalyzer
# =====================================================================
class HonkaiCostCharacterMetaAnalyzer(BaseCostMetaAnalyzer):
    def __init__(self, db_name=os.getenv("DB_File")):
        super().__init__(db_name)
        self.tasks = [
            {"mode": "MOC", "table": "moc_by_cost_chars", "floors": [10, 12], "perf": "MIN", "node_col": "node", "node_val": "0"},
            {"mode": "APOC", "table": "apoc_by_cost_chars", "floors": [4], "perf": "MAX", "node_col": "node", "node_val": "'0'"},
            {"mode": "PURE_FICTION", "table": "pure_fiction_by_cost_chars", "floors": [4], "perf": "MAX", "node_col": "node", "node_val": "'0'"},
            {"mode": "ANOMALY_F0", "table": "anomaly_by_cost_chars", "floors": [0], "perf": "MIN", "node_col": None, "node_val": None},
            {"mode": "ANOMALY_F4", "table": "anomaly_by_cost_chars", "floors": [4], "perf": "MIN", "node_col": None, "node_val": None},
            {"mode": "ANOMALY_F5", "table": "anomaly_by_cost_chars", "floors": [5], "perf": "MIN", "node_col": None, "node_val": None},
        ]

    def _get_character_exprs(self, task: dict, is_history: bool) -> list[pl.Expr]:
        exprs = self._get_common_agg_exprs(task, usage_col="Appearance_Rate_pct", is_history=is_history)
        exprs.extend([
            pl.col("Sustain_Samples").sum().alias("Total_Sustain_Samples"),
            (100.0 * pl.col("Sustain_Samples").sum() / pl.col("Samples").sum()).round(2).alias("Sustain_Rate_pct"),
            pl.col("Sustain_Percentage").alias("Sustain_Percentage_List")])
        return exprs

    def _aggregate(self, raw_lazy: pl.LazyFrame, task: dict, limit_recent: bool = False, is_history: bool = False) -> pl.DataFrame:
        lazy = self._apply_base_filters(raw_lazy, task)

        if limit_recent:
            recent_versions = (
                lazy.select("version")
                .unique()
                .sort("version", descending=True)
                .limit(3)
                .collect()["version"]
            )
            lazy = lazy.filter(pl.col("version").is_in(recent_versions))

        lazy = lazy.with_columns(pl.lit(task["mode"]).alias("Game_Mode"))

        group_cols = [
            "Game_Mode", "at_eidolon_level", "up_to_eidolon_level",
            "Character", "estimated_min_cost", "estimated_max_cost", "max_eidolon"
        ]

        return lazy.group_by(group_cols).agg(self._get_character_exprs(task, is_history=is_history)).collect()

    def _aggregate_rolling(self, raw_lazy: pl.LazyFrame, task: dict, window: int = 3) -> pl.DataFrame:
        lazy = self._apply_base_filters(raw_lazy, task)

        versions_df = (
            lazy.select("version")
            .unique()
            .sort("version")
            .with_columns(pl.int_range(0, pl.len()).alias("vrank"))
            .collect()
        )

        if versions_df.is_empty():
            return pl.DataFrame()

        base = lazy.join(versions_df.lazy(), on="version")

        as_of_df = versions_df.rename({"version": "As_Of_Version", "vrank": "asof_vrank"}).lazy()
        joined = base.join(as_of_df, how="cross").filter(
            (pl.col("vrank") <= pl.col("asof_vrank")) &
            (pl.col("vrank") >= (pl.col("asof_vrank") - (window - 1)))
        )

        joined = joined.with_columns(
            pl.lit(task["mode"]).alias("Game_Mode"),
            (pl.col("asof_vrank").max() - pl.col("asof_vrank") + 1).alias("Version_Group_Num")
        )

        group_cols = [
            "As_Of_Version", "Game_Mode", "at_eidolon_level", "up_to_eidolon_level",
            "Character", "estimated_min_cost", "estimated_max_cost", "max_eidolon"
        ]

        agg_exprs = [pl.col("Version_Group_Num").first().alias("Version_Group_Num")]
        agg_exprs.extend(self._get_character_exprs(task, is_history=False))

        return joined.group_by(group_cols).agg(agg_exprs).collect()

    def run_analysis(self):
        con = duckdb.connect(self.db_path)
        all_history, all_recent, all_rolling = [], [], []

        print(f"Starting By-Cost Character Meta Analysis on {self.db_path}...")

        for task in self.tasks:
            try:
                raw_lazy = con.execute(f"SELECT * FROM {task['table']}").pl().lazy()

                df_h = self._aggregate(raw_lazy, task, limit_recent=False, is_history=True)
                if not df_h.is_empty():
                    all_history.append(df_h)

                df_r = self._aggregate(raw_lazy, task, limit_recent=True, is_history=False)
                if not df_r.is_empty():
                    all_recent.append(df_r)

                df_roll = self._aggregate_rolling(raw_lazy, task, window=3)
                if not df_roll.is_empty():
                    all_rolling.append(df_roll)

                print(f"  + {task['mode']:15s}  history={len(df_h):,}  recent={len(df_r):,}  rolling={len(df_roll):,}")
            except Exception as e:
                print(f"  ! Error on {task['mode']}: {e}")

        HonkaiCostTeamMetaAnalyzer._write_tables(self, con, all_history, all_recent, all_rolling, "character")
        con.close()


if __name__ == "__main__":
    team_analyzer = HonkaiCostTeamMetaAnalyzer()
    team_analyzer.run_analysis()

    archetype_analyzer = HonkaiCostArchetypeMetaAnalyzer()
    archetype_analyzer.run_analysis()

    character_analyzer = HonkaiCostCharacterMetaAnalyzer()
    character_analyzer.run_analysis()