import duckdb
import polars as pl
import os
from dotenv import load_dotenv

load_dotenv()


class HonkaiTeamMetaAnalyzer:
    def __init__(self, db_name=os.getenv("DB_File")):
        self.db_path = db_name
        self.tasks = [
            {
                "mode":      "MOC",
                "table":     "moc_stats_teams",
                "floors":    [10, 12],
                "perf":      "MIN",
                "node_col":  "node",
                "node_val":  "0",
            },
            {
                "mode":      "APOC",
                "table":     "apoc_stats_teams",
                "floors":    [4],
                "perf":      "MAX",
                "node_col":  "node",
                "node_val":  "0",
            },
            {
                "mode":      "PURE_FICTION",
                "table":     "pure_fiction_stats_teams",
                "floors":    [4],
                "perf":      "MAX",
                "node_col":  "node",
                "node_val":  "0",
            },
            {
                "mode":      "ANOMALY_F0",
                "table":     "anomaly_stats_teams",
                "floors":    [0],
                "perf":      "MIN",
                "node_col":  None,
                "node_val":  None,
            },
            {
                "mode":      "ANOMALY_F4",
                "table":     "anomaly_stats_teams",
                "floors":    [4],
                "perf":      "MIN",
                "node_col":  None,
                "node_val":  None,
            },
            {
                "mode":      "ANOMALY_F5",
                "table":     "anomaly_stats_teams",
                "floors":    [5],
                "perf":      "MIN",
                "node_col":  None,
                "node_val":  None,
            },
        ]

    def _aggregate_lazy(self, raw_lazy: pl.LazyFrame, task: dict, limit_recent: bool = False) -> pl.DataFrame:
        """Applies lazy logical operations and executes optimized graph via .collect()."""

        # 1. Base Filters (Pushed down to source level)
        lazy = raw_lazy.filter(
            (pl.col("Samples") > 0) & 
            (pl.col("floor").is_in(task["floors"]))
        )

        if task["node_col"] is not None:
            lazy = lazy.filter(
                pl.col(task["node_col"]).cast(pl.String) == str(task["node_val"])
            )

        # 2. Limit Recent Versions Filter
        if limit_recent:
            # Evaluate distinct versions lazily
            top_3_versions = (
                lazy.select("version")
                .unique()
                .sort("version", descending=True)
                .limit(3)
                .collect()["version"]  # Collect brief sub-query
            )
            lazy = lazy.filter(pl.col("version").is_in(top_3_versions))

        # 3. Helper Columns
        lazy = lazy.with_columns(
            pl.lit(task["mode"]).alias("Game_Mode"),
            pl.col("Sustain?").alias("Sustain")
        )

        group_cols = [
            "Game_Mode",
            "at_eidolon_level",
            "up_to_eidolon_level",
            "Team",
            "Archetype_Core",
            "Sustain",
        ]

        # 4. Aggregations
        agg_exprs = []

        if not limit_recent:
            agg_exprs.extend([
                pl.col("Appearance_Rate_pct").alias("Usage_pct_List"),
                pl.col("Min_Score").alias("Min_Score_List"),
                pl.col("Average_Score").alias("Average_Score_List"),
                pl.col("Max_Score").alias("Max_Score_List"),
                pl.col("Median_Score").alias("Median_Score_List"),
                pl.col("Samples").alias("Samples_List"),
                pl.col("Full_Clear_Rate_pct").alias("Full_Star_Rate_pct_List"),
            ])

        best_score_expr = (
            pl.col("Average_Score").min() if task["perf"] == "MIN" else pl.col("Average_Score").max()
        )

        agg_exprs.extend([
            pl.col("Appearance_Rate_pct").mean().round(2).alias("Simple_Avg_Appearance"),
            pl.col("Average_Score").mean().round(2).alias("Simple_Avg_Score"),
            ((pl.col("Average_Score") * pl.col("Samples")).sum() / pl.col("Samples").sum()).round(2).alias("Weighted_Avg_Score"),
            ((pl.col("Median_Score") * pl.col("Samples")).sum() / pl.col("Samples").sum()).round(2).alias("Weighted_Avg_Median"),
            best_score_expr.alias("Best_Version_Avg"),
            pl.col("Samples").sum().alias("Total_Samples"),
            (100.0 * pl.col("Total_Full_Clears").sum() / pl.col("Samples").sum()).round(2).alias("Full_Star_Rate_pct"),
            pl.col("version").n_unique().alias("Version_Count"),
            pl.col("version").unique().sort(descending=True).cast(pl.String).str.join(", ").alias("Versions_Used"),
        ])

        # Execute optimization engine with .collect()
        return lazy.group_by(group_cols).agg(agg_exprs).collect()

    def run_analysis(self):
        con = duckdb.connect(self.db_path)
        all_history = []
        all_recent  = []

        print(f"Starting Team Meta Analysis on {self.db_path}...")

        for task in self.tasks:
            try:
                # Convert DuckDB Arrow relation directly to a LazyFrame (.pl().lazy())
                raw_lazy = con.execute(f"SELECT * FROM {task['table']}").pl().lazy()

                df_h = self._aggregate_lazy(raw_lazy, task, limit_recent=False)
                if not df_h.is_empty():
                    all_history.append(df_h)

                df_r = self._aggregate_lazy(raw_lazy, task, limit_recent=True)
                if not df_r.is_empty():
                    all_recent.append(df_r)

                print(f"  + Successfully aggregated Team stats for {task['mode']}")
            except Exception as e:
                print(f"  ! Error on {task['mode']}: {e}")

        if not all_history and not all_recent:
            print("No data found to write.")
            con.close()
            return

        con.execute("BEGIN TRANSACTION")
        try:
            if all_history:
                full_df = pl.concat(all_history, how="diagonal")
                con.execute("DROP TABLE IF EXISTS team_meta_summary")
                con.execute("CREATE TABLE team_meta_summary AS SELECT * FROM full_df")
                print(f"\n  Wrote team_meta_summary ({len(full_df):,} rows)")

            if all_recent:
                recent_df = pl.concat(all_recent, how="diagonal")
                con.execute("DROP TABLE IF EXISTS team_recent_meta_summary")
                con.execute("CREATE TABLE team_recent_meta_summary AS SELECT * FROM recent_df")
                print(f"  Wrote team_recent_meta_summary ({len(recent_df):,} rows)")

            con.execute("COMMIT")
            print(
                "\n>>> Analysis complete. "
                "Tables 'team_meta_summary' and 'team_recent_meta_summary' are now live."
            )
        except Exception as e:
            con.execute("ROLLBACK")
            print(f"\n>>> Error during DB write: {e}")
        finally:
            con.close()


if __name__ == "__main__":
    analyzer = HonkaiTeamMetaAnalyzer()
    analyzer.run_analysis()