import duckdb
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()


class HonkaiGearEidolonSummaryAnalyzer:
    def __init__(self, db_name=os.getenv("DB_File")):
        self.db_path = db_name

        # ── Gear tasks ──────────────────────────────────────────────────────────
        # median_col / min_col differ between APOC ("Median_Scores","Min_Scores")
        # and every other table ("Median_Score","Min_Score").
        self.gear_tasks = [
            {
                "mode":       "MOC",
                "table":      "moc_stats_gear_usage",
                "floors":     [10, 12],
                "node_col":   "node",
                "node_val":   "'0'",
                "median_col": "Median_Score",
                "min_col":    "Min_Score",
            },
            {
                "mode":       "APOC",
                "table":      "apoc_stats_gear_usage",
                "floors":     [4],
                "node_col":   "node",
                "node_val":   "'0'",
                "median_col": "Median_Scores",   # APOC typo in source table
                "min_col":    "Min_Scores",
            },
            {
                "mode":       "PURE_FICTION",
                "table":      "pure_fiction_stats_gear_usage",
                "floors":     [4],
                "node_col":   "node",
                "node_val":   "'0'",
                "median_col": "Median_Score",
                "min_col":    "Min_Score",
            },
            {
                "mode":       "ANOMALY_F0",
                "table":      "anomaly_stats_gear_usage",
                "floors":     [0],
                "node_col":   None,
                "node_val":   None,
                "median_col": "Median_Score",
                "min_col":    "Min_Score",
            },
            {
                "mode":       "ANOMALY_F4",
                "table":      "anomaly_stats_gear_usage",
                "floors":     [4],
                "node_col":   None,
                "node_val":   None,
                "median_col": "Median_Score",
                "min_col":    "Min_Score",
            },
            {
                "mode":       "ANOMALY_F5",
                "table":      "anomaly_stats_gear_usage",
                "floors":     [5],
                "node_col":   None,
                "node_val":   None,
                "median_col": "Median_Score",
                "min_col":    "Min_Score",
            },
        ]

        # ── Eidolon-performance tasks ────────────────────────────────────────────
        # score_col differs per mode:
        #   MOC / Anomaly  → "Avg_Cycles"
        #   APOC           → "Avg_Scores"
        #   Pure Fiction   → "Avg_Points"
        self.eidolon_tasks = [
            {
                "mode":      "MOC",
                "table":     "moc_stats_eidolon_performance",
                "floors":    [10, 12],
                "node_col":  "node",
                "node_val":  "'0'",
                "score_col": "Avg_Cycles",
            },
            {
                "mode":      "APOC",
                "table":     "apoc_stats_eidolon_performance",
                "floors":    [4],
                "node_col":  "node",
                "node_val":  "'0'",
                "score_col": "Avg_Scores",
            },
            {
                "mode":      "PURE_FICTION",
                "table":     "pure_fiction_stats_eidolon_performance",
                "floors":    [4],
                "node_col":  "node",
                "node_val":  "'0'",
                "score_col": "Avg_Points",
            },
            {
                "mode":      "ANOMALY_F0",
                "table":     "anomaly_stats_eidolon_performance",
                "floors":    [0],
                "node_col":  None,
                "node_val":  None,
                "score_col": "Avg_Cycles",
            },
            {
                "mode":      "ANOMALY_F4",
                "table":     "anomaly_stats_eidolon_performance",
                "floors":    [4],
                "node_col":  None,
                "node_val":  None,
                "score_col": "Avg_Cycles",
            },
            {
                "mode":      "ANOMALY_F5",
                "table":     "anomaly_stats_eidolon_performance",
                "floors":    [5],
                "node_col":  None,
                "node_val":  None,
                "score_col": "Avg_Cycles",
            },
        ]

    # ── Helpers ─────────────────────────────────────────────────────────────────

    @staticmethod
    def _floor_filter(floors: list) -> str:
        if len(floors) == 1:
            return f"AND floor = {floors[0]}"
        return f"AND floor IN ({', '.join(str(f) for f in floors)})"

    @staticmethod
    def _node_filter(task: dict) -> str:
        if task["node_col"] is not None:
            return f"AND {task['node_col']} = {task['node_val']}"
        return ""

    @staticmethod
    def _recent_subquery(table: str) -> str:
        return (
            f"AND version IN ("
            f"SELECT DISTINCT version FROM {table} "
            f"ORDER BY version DESC LIMIT 3)"
        )

    # ── Gear query ───────────────────────────────────────────────────────────────

    def _gear_query(self, task: dict, limit_recent: bool = False) -> str:
        floor_filter  = self._floor_filter(task["floors"])
        node_filter   = self._node_filter(task)
        recent_filter = self._recent_subquery(task["table"]) if limit_recent else ""
        median_col    = task["median_col"]
        min_col       = task["min_col"]

        return f"""
            WITH agg AS (
                SELECT
                    '{task['mode']}'   
                    AS Game_Mode,
                    at_eidolon_level,
                    up_to_eidolon_level,
                    Character,
                    Eidolon,
                    Category,
                    Gear_Name,

                    -- Usage
                    SUM(Usage)                                                              AS Total_Usage,

                    -- Score aggregates (weighted by usage)
                    ROUND(SUM(Average_Score * Usage) / NULLIF(SUM(Usage), 0), 2)           AS Weighted_Avg_Score,
                    ROUND(SUM("{median_col}" * Usage) / NULLIF(SUM(Usage), 0), 2)          AS Weighted_Avg_Median,
                    MIN("{min_col}")                                                        AS Min_Avg_Score,
                    MAX(Max_Score)                                                          AS Max_Avg_Score,

                    -- Version metadata
                    COUNT(DISTINCT version)                                                 AS Version_Count,
                    STRING_AGG(DISTINCT version, ', ' ORDER BY version DESC)               AS Versions_Used
                FROM {task['table']}
                WHERE Usage > 0
                  {floor_filter}
                  {node_filter}
                  {recent_filter}
                GROUP BY 1, 2, 3, 4, 5, 6,7
            )
            SELECT
                *,
                ROUND(
                    Total_Usage::DOUBLE / NULLIF(SUM(Total_Usage) OVER (
                        PARTITION BY Game_Mode, at_eidolon_level, up_to_eidolon_level, Character, Eidolon, Category
                    ), 0),
                    4
                )                                                                           AS Usage_Rate
            FROM agg
        """

    # ── Eidolon-performance query ────────────────────────────────────────────────

    def _eidolon_query(self, task: dict, limit_recent: bool = False) -> str:
        floor_filter  = self._floor_filter(task["floors"])
        node_filter   = self._node_filter(task)
        recent_filter = self._recent_subquery(task["table"]) if limit_recent else ""
        score_col     = task["score_col"]

        eidolon_cols = "\n".join([
            f"""
                ROUND(
                    SUM("Eidolon_{i}.0_{score_col}" * Total_Samples)
                    / NULLIF(SUM(CASE WHEN "Eidolon_{i}.0_{score_col}" IS NOT NULL THEN Total_Samples END), 0),
                    2
                )                                                                       AS E{i}_Weighted_Avg_Score,
                SUM("Eidolon_{i}.0_Samples")                                      AS E{i}_Total_Samples,
                ROUND(AVG("Eidolon_{i}_Sustain_pct_pct"), 2)                           AS E{i}_Avg_Sustain_pct,
                ROUND(AVG("Eidolon_{i}_Full_Clear_pct_pct"), 2)                         AS E{i}_Avg_Full_Star_pct,"""
            for i in range(7)
        ])

        return f"""
            SELECT
                '{task['mode']}'    
                AS Game_Mode,
                at_eidolon_level,
                up_to_eidolon_level,
                Character,

                -- Sample totals
                SUM(Total_Samples)                                                      AS Total_Samples,
                SUM(Total_Sustains)                                                     AS Total_Sustains,
                {eidolon_cols}

                -- Version metadata
                COUNT(DISTINCT version)                                                 AS Version_Count,
                STRING_AGG(DISTINCT version, ', ' ORDER BY version DESC)               AS Versions_Used
            FROM {task['table']}
            WHERE Total_Samples > 0
              {floor_filter}
              {node_filter}
              {recent_filter}
            GROUP BY 1, 2, 3, 4
        """

    # ── Runner ───────────────────────────────────────────────────────────────────

    def _run_tasks(self, con, tasks: list, query_fn) -> tuple[list, list]:
        all_history, all_recent = [], []
        for task in tasks:
            try:
                df_h = con.execute(query_fn(task, limit_recent=False)).df()
                if not df_h.empty:
                    all_history.append(df_h)

                df_r = con.execute(query_fn(task, limit_recent=True)).df()
                if not df_r.empty:
                    all_recent.append(df_r)

                print(f"  + {task['mode']}")
            except Exception as e:
                print(f"  ! Error on {task['mode']}: {e}")
        return all_history, all_recent

    def run_analysis(self):
        con = duckdb.connect(self.db_path)

        try:
            print(f"\nStarting Gear Summary Analysis on {self.db_path}...")
            gear_history, gear_recent = self._run_tasks(con, self.gear_tasks, self._gear_query)

            print("\nStarting Eidolon Performance Summary Analysis...")
            eid_history, eid_recent = self._run_tasks(con, self.eidolon_tasks, self._eidolon_query)

            con.execute("BEGIN TRANSACTION")
            try:
                pairs = [
                    (gear_history,  "gear_meta_summary"),
                    (gear_recent,   "gear_recent_meta_summary"),
                    (eid_history,   "eidolon_performance_meta_summary"),
                    (eid_recent,    "eidolon_performance_recent_meta_summary"),
                ]
                for frames, table_name in pairs:
                    if frames:
                        df = pd.concat(frames, ignore_index=True)
                        con.execute(f"DROP TABLE IF EXISTS {table_name}")
                        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
                        print(f"  Wrote {table_name} ({len(df):,} rows)")

                con.execute("COMMIT")
                print(
                    "\n>>> Analysis complete. Tables written:\n"
                    "    gear_meta_summary\n"
                    "    gear_recent_meta_summary\n"
                    "    eidolon_performance_meta_summary\n"
                    "    eidolon_performance_recent_meta_summary"
                )
            except Exception as e:
                con.execute("ROLLBACK")
                print(f"\n>>> Error during DB write: {e}")

        finally:
            con.close()


if __name__ == "__main__":
    analyzer = HonkaiGearEidolonSummaryAnalyzer()
    analyzer.run_analysis()