import duckdb
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()


class HonkaiMetaAnalyzer:
    def __init__(self, db_name=os.getenv("DB_File")):
        self.db_path = db_name
        self.tasks = [
            {
                "mode":      "MOC",
                "table":     "moc_stats_archetypes",
                "floors":    [10, 12],   # floor 10 (legacy) and 12 treated as same stage
                "perf":      "MIN",
                "node_col":  "node",
                "node_val":  "0",
            },
            {
                "mode":      "APOC",
                "table":     "apoc_stats_archetypes",
                "floors":    [4],
                "perf":      "MAX",
                "node_col":  "node",
                "node_val":  "0",
            },
            {
                "mode":      "PURE_FICTION",
                "table":     "pure_fiction_stats_archetypes",
                "floors":    [4],
                "perf":      "MAX",
                "node_col":  "node",
                "node_val":  "0",
            },
            {
                "mode":      "ANOMALY_F0",
                "table":     "anomaly_stats_archetypes",
                "floors":    [0],
                "perf":      "MIN",
                "node_col":  None,
                "node_val":  None,
            },
            {
                "mode":      "ANOMALY_F4",
                "table":     "anomaly_stats_archetypes",
                "floors":    [4],
                "perf":      "MIN",
                "node_col":  None,
                "node_val":  None,
            },
        ]

    @staticmethod
    def _floor_filter(floors: list) -> str:
        if len(floors) == 1:
            return f"AND floor = {floors[0]}"
        return f"AND floor IN ({', '.join(str(f) for f in floors)})"

    def _generate_query(self, task, limit_recent=False):
        node_filter  = (
            f"AND {task['node_col']} = {task['node_val']}"
            if task["node_col"] is not None
            else ""
        )
        floor_filter = self._floor_filter(task["floors"])

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
                Archetype_Core,

                -- Appearance
                ROUND(AVG(Usage_pct), 2)                                              AS Simple_Avg_Appearance,

                -- Score
                ROUND(AVG(Average_Score), 2)                                          AS Simple_Avg_Score,
                ROUND(SUM(Average_Score * Samples) / NULLIF(SUM(Samples), 0), 2)      AS Weighted_Avg_Score,
                {task['perf']}(Average_Score)                                          AS Best_Version_Avg,

                -- Metadata
                SUM(Samples)                                                          AS Total_Samples,
                ROUND(SUM(Sustain_Samples) * 1.0 / NULLIF(SUM(Samples), 0), 2)        AS Sustain_Percentage,
                STRING_AGG(DISTINCT version, ', ' ORDER BY version DESC)              AS Versions_Used
            FROM {task['table']}
            WHERE Samples > 0
              {floor_filter}
              {node_filter}
              {recent_filter}
            GROUP BY 1, 2, 3
        """

    def run_analysis(self):
        con = duckdb.connect(self.db_path)
        all_history = []
        all_recent  = []

        for task in self.tasks:
            try:
                df_h = con.execute(self._generate_query(task, limit_recent=False)).df()
                if not df_h.empty:
                    all_history.append(df_h)

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
                con.execute("DROP TABLE IF EXISTS archetype_meta_summary")
                con.execute("CREATE TABLE archetype_meta_summary AS SELECT * FROM full_df")
                print(f"\n  Wrote archetype_meta_summary ({len(full_df):,} rows)")

            if all_recent:
                recent_df = pd.concat(all_recent, ignore_index=True)
                con.execute("DROP TABLE IF EXISTS archetype_recent_meta_summary")
                con.execute("CREATE TABLE archetype_recent_meta_summary AS SELECT * FROM recent_df")
                print(f"  Wrote archetype_recent_meta_summary ({len(recent_df):,} rows)")

            con.execute("COMMIT")
            print(
                "\n>>> Analysis complete. "
                "Tables 'archetype_meta_summary' and 'archetype_recent_meta_summary' are ready."
            )
        except Exception as e:
            con.execute("ROLLBACK")
            print(f"\n>>> Error during DB write: {e}")
        finally:
            con.close()


if __name__ == "__main__":
    analyzer = HonkaiMetaAnalyzer()
    analyzer.run_analysis()
