import duckdb
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()


class HonkaiDuosSummaryAnalyzer:
    def __init__(self, db_name=os.getenv("DB_File")):
        self.db_path = db_name
        self.tasks = [
            {
                "mode": "MOC",
                "table": "moc_stats_duos",
                "floor": 12,
                "perf": "MIN",
                "node_col": "node",
                "node_val": "0",
            },
            {
                "mode": "APOC",
                "table": "apoc_stats_duos",
                "floor": 4,
                "perf": "MAX",
                "node_col": "node",
                "node_val": "0",
            },
            {
                "mode": "PURE_FICTION",
                "table": "pure_fiction_stats_duos",
                "floor": 4,
                "perf": "MAX",
                "node_col": "node",
                "node_val": "0",
            },
            {
                "mode": "ANOMALY_F0",
                "table": "anomaly_stats_duos",
                "floor": 0,
                "perf": "MIN",
                "node_col": None,
                "node_val": None,
            },
            {
                "mode": "ANOMALY_F4",
                "table": "anomaly_stats_duos",
                "floor": 4,
                "perf": "MIN",
                "node_col": None,
                "node_val": None,
            },
        ]

    def _generate_query(self, task, limit_recent=False):
        node_filter = (
            f"AND {task['node_col']} = {task['node_val']}"
            if task["node_col"] is not None
            else ""
        )

        recent_filter = ""
        if limit_recent:
            recent_filter = (
                f"AND version IN ("
                f"SELECT DISTINCT version FROM {task['table']} "
                f"ORDER BY version DESC LIMIT 3)"
            )

        return f"""
            SELECT
                '{task['mode']}' AS Game_Mode,
                up_to_eidolon_level,
                Antecedent,
                Consequent,

                -- Appearance Rate
                ROUND(AVG(Appearance_Rate_pct), 4)                                   AS Simple_Avg_Appearance_Rate,
                ROUND(SUM(Appearance_Rate_pct * Samples) / NULLIF(SUM(Samples), 0), 4) AS Weighted_Avg_Appearance_Rate,

                -- Confidence (how often Consequent appears given Antecedent)
                ROUND(AVG(Confidence), 4)                                             AS Simple_Avg_Confidence,
                ROUND(SUM(Confidence * Samples) / NULLIF(SUM(Samples), 0), 4)         AS Weighted_Avg_Confidence,

                -- Association metrics
                ROUND(AVG(Lift), 4)                                                   AS Simple_Avg_Lift,
                ROUND(SUM(Lift * Samples) / NULLIF(SUM(Samples), 0), 4)               AS Weighted_Avg_Lift,
                ROUND(AVG(Leverage), 4)                                               AS Simple_Avg_Leverage,
                ROUND(AVG(Conviction), 4)                                             AS Simple_Avg_Conviction,

                -- Score metrics
                ROUND(AVG(Average_Score), 2)                                          AS Simple_Avg_Score,
                ROUND(SUM(Average_Score * Samples) / NULLIF(SUM(Samples), 0), 2)      AS Weighted_Avg_Score,
                {task['perf']}(Average_Score)                                          AS Best_Version_Avg,
                ROUND(AVG(Median_Score), 2)                                           AS Simple_Avg_Median_Score,
                ROUND(AVG(Std_Dev), 4)                                                AS Avg_Std_Dev,

                -- Metadata
                SUM(Samples)                                                          AS Total_Samples,
                ROUND(AVG(Sustain_Percentage), 4)                                     AS Avg_Sustain_Percentage,
                STRING_AGG(DISTINCT version, ', ' ORDER BY version DESC)              AS Versions_Used
            FROM {task['table']}
            WHERE Samples > 0
              AND floor = {task['floor']}
              {node_filter}
              {recent_filter}
            GROUP BY 1, 2, 3, 4
        """

    def run_analysis(self):
        con = duckdb.connect(self.db_path)
        all_history = []
        all_recent = []

        for task in self.tasks:
            try:
                # Full history
                df_h = con.execute(self._generate_query(task, limit_recent=False)).df()
                if not df_h.empty:
                    all_history.append(df_h)

                # Recent (last 3 versions)
                df_r = con.execute(self._generate_query(task, limit_recent=True)).df()
                if not df_r.empty:
                    all_recent.append(df_r)

                print(f"  + Successfully aggregated {task['mode']}")
            except Exception as e:
                print(f"  ! Error on {task['mode']}: {e}")

        con.execute("BEGIN TRANSACTION")
        try:
            if all_history:
                full_df = pd.concat(all_history, ignore_index=True)
                con.execute("DROP TABLE IF EXISTS duos_meta_summary")
                con.execute(
                    "CREATE TABLE duos_meta_summary AS SELECT * FROM full_df"
                )
                print(
                    f"\n  Wrote duos_meta_summary ({len(full_df):,} rows)"
                )

            if all_recent:
                recent_df = pd.concat(all_recent, ignore_index=True)
                con.execute("DROP TABLE IF EXISTS duos_recent_meta_summary")
                con.execute(
                    "CREATE TABLE duos_recent_meta_summary AS SELECT * FROM recent_df"
                )
                print(
                    f"  Wrote duos_recent_meta_summary ({len(recent_df):,} rows)"
                )

            con.execute("COMMIT")
            print(
                "\n>>> Analysis complete. "
                "Tables 'duos_meta_summary' and 'duos_recent_meta_summary' are ready."
            )
        except Exception as e:
            con.execute("ROLLBACK")
            print(f"\n>>> Error during DB write: {e}")
        finally:
            con.close()


if __name__ == "__main__":
    analyzer = HonkaiDuosSummaryAnalyzer()
    analyzer.run_analysis()
