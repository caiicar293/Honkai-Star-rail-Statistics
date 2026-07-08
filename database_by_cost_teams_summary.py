import duckdb
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()


class HonkaiCostTeamMetaAnalyzer:
    """
    Aggregates *_by_cost_teams tables across all versions (and optionally
    the 3 most-recent versions), grouping by:
        estimated_min_cost, estimated_max_cost, max_eidolon, Team

    Full_Star_Rate is intentionally RE-CALCULATED as:
        SUM(Total_Full_Clears) / SUM(Samples)
    rather than averaging the pre-computed percentages.
    """

    def __init__(self, db_name=os.getenv("DB_File")):
        self.db_path = db_name
        self.tasks = [
            {
                "mode":     "MOC",
                "table":    "moc_by_cost_teams",
                "floors":   [10, 12],
                "perf":     "MIN",        # lower cycles-used = better for MOC
                "node_col": "node",
                "node_val": "0",          # integer node; keep only node 0 (combined)
            },
            {
                "mode":     "APOC",
                "table":    "apoc_by_cost_teams",
                "floors":   [4],
                "perf":     "MAX",
                "node_col": "node",
                "node_val": "'0'",        # VARCHAR node in apoc/anomaly
            },
            {
                "mode":     "PURE_FICTION",
                "table":    "pure_fiction_by_cost_teams",
                "floors":   [4],
                "perf":     "MAX",
                "node_col": "node",
                "node_val": "'0'",
            },
            {
                "mode":     "ANOMALY_F0",
                "table":    "anomaly_by_cost_teams",
                "floors":   [0],
                "perf":     "MIN",
                "node_col": None,
                "node_val": None,
            },
            {
                "mode":     "ANOMALY_F4",
                "table":    "anomaly_by_cost_teams",
                "floors":   [4],
                "perf":     "MIN",
                "node_col": None,
                "node_val": None,
            },
        ]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _floor_filter(floors: list) -> str:
        if len(floors) == 1:
            return f"AND floor = {floors[0]}"
        return f"AND floor IN ({', '.join(str(f) for f in floors)})"

    @staticmethod
    def _node_filter(task: dict) -> str:
        if task["node_col"] is None:
            return ""
        return f"AND {task['node_col']} = {task['node_val']}"

    @staticmethod
    def _recent_filter(table: str) -> str:
        return (
            f"AND version IN ("
            f"SELECT DISTINCT version FROM {table} "
            f"ORDER BY version DESC LIMIT 3)"
        )

    # ------------------------------------------------------------------
    # Query builder
    # ------------------------------------------------------------------

    def _generate_query(self, task: dict, limit_recent: bool = False) -> str:
        floor_f  = self._floor_filter(task["floors"])
        node_f   = self._node_filter(task)
        recent_f = self._recent_filter(task["table"]) if limit_recent else ""

        return f"""
            SELECT
                '{task['mode']}'                                                       AS Game_Mode,
                at_eidolon_level,
                up_to_eidolon_level,
                Team,
                Archetype_Core,
                estimated_min_cost,
                estimated_max_cost,
                max_eidolon,
                has_sustain,

                -- Appearance (simple average across versions)
                ROUND(AVG(Appearance_Rate_pct), 2)                                    AS Simple_Avg_Appearance,

                -- Score aggregates
                ROUND(AVG(Average_Score), 2)                                          AS Simple_Avg_Score,
                ROUND(SUM(Average_Score * Samples) / NULLIF(SUM(Samples), 0), 2)      AS Weighted_Avg_Score,
                ROUND(SUM(Median_Score  * Samples) / NULLIF(SUM(Samples), 0), 2)      AS Weighted_Avg_Median,
                {task['perf']}(Average_Score)                                          AS Best_Version_Avg,

                -- Full-star rate: recalculate from raw counts, NOT avg of pct
                SUM(Total_Full_Clears)                                                  AS Total_Total_Full_Clears,
                SUM(Samples)                                                           AS Total_Samples,
                ROUND(
                    100.0 * SUM(Total_Full_Clears) / NULLIF(SUM(Samples), 0),
                    2
                )                                                                      AS Full_Star_Rate_pct,
                

                -- Metadata
                COUNT(DISTINCT version)                                                AS Version_Count,
                STRING_AGG(DISTINCT version, ', ' ORDER BY version DESC)               AS Versions_Used
            FROM {task['table']}
            WHERE Samples > 0
              {floor_f}
              {node_f}
              {recent_f}
            GROUP BY
                1,  -- Game_Mode
                2,  -- at_eidolon_level
                3,  -- up_to_eidolon_level
                4,  -- Team
                5,  -- Archetype_Core
                6,  -- estimated_min_cost
                7,  -- estimated_max_cost
                8,  -- max_eidolon
                9   -- has_sustain
        """

    # ------------------------------------------------------------------
    # Main runner
    # ------------------------------------------------------------------

    def run_analysis(self):
        con = duckdb.connect(self.db_path)
        all_history: list[pd.DataFrame] = []
        all_recent:  list[pd.DataFrame] = []

        print(f"Starting By-Cost Team Meta Analysis on {self.db_path}...")

        for task in self.tasks:
            try:
                df_h = con.execute(self._generate_query(task, limit_recent=False)).df()
                if not df_h.empty:
                    all_history.append(df_h)

                df_r = con.execute(self._generate_query(task, limit_recent=True)).df()
                if not df_r.empty:
                    all_recent.append(df_r)

                print(f"  + {task['mode']:15s}  history={len(df_h):,}  recent={len(df_r):,}")
            except Exception as e:
                print(f"  ! Error on {task['mode']}: {e}")

        if not all_history and not all_recent:
            print("No data found. Exiting.")
            con.close()
            return

        con.execute("BEGIN TRANSACTION")
        try:
            if all_history:
                full_df = pd.concat(all_history, ignore_index=True)
                con.execute("DROP TABLE IF EXISTS by_cost_team_meta_summary")
                con.execute("CREATE TABLE by_cost_team_meta_summary AS SELECT * FROM full_df")
                print(f"\n  Wrote by_cost_team_meta_summary     ({len(full_df):,} rows)")

            if all_recent:
                recent_df = pd.concat(all_recent, ignore_index=True)
                con.execute("DROP TABLE IF EXISTS by_cost_team_recent_meta_summary")
                con.execute(
                    "CREATE TABLE by_cost_team_recent_meta_summary AS SELECT * FROM recent_df"
                )
                print(f"  Wrote by_cost_team_recent_meta_summary ({len(recent_df):,} rows)")

            con.execute("COMMIT")
            print(
                "\n>>> Analysis complete. "
                "Tables 'by_cost_team_meta_summary' and "
                "'by_cost_team_recent_meta_summary' are now live."
            )
        except Exception as e:
            con.execute("ROLLBACK")
            print(f"\n>>> Error during DB write: {e}")
        finally:
            con.close()


class HonkaiCostArchetypeMetaAnalyzer:
    """
    Aggregates *_by_cost_archetypes tables across all versions (and optionally
    the 3 most-recent versions), grouping by:
        estimated_min_cost, estimated_max_cost, max_eidolon, Archetype_Core

    Unlike the team-level tables, the archetype-level tables have no `Team`
    or `has_sustain` column -- sustain presence is instead captured via a
    raw `Sustain_Samples`, which (like Full_Star_Rate) is RE-CALCULATED here as:
        SUM(Sustain_Samples) / SUM(Samples)
    rather than averaging the pre-computed Sustain_Percentage_pct.

    Full_Star_Rate is likewise RE-CALCULATED as:
        SUM(Total_Full_Clears) / SUM(Samples)
    rather than averaging the pre-computed percentages.
    """

    def __init__(self, db_name=os.getenv("DB_File")):
        self.db_path = db_name
        self.tasks = [
            {
                "mode":     "MOC",
                "table":    "moc_by_cost_archetypes",
                "floors":   [10, 12],
                "perf":     "MIN",        # lower cycles-used = better for MOC
                "node_col": "node",
                "node_val": "0",          # integer node; keep only node 0 (combined)
            },
            {
                "mode":     "APOC",
                "table":    "apoc_by_cost_archetypes",
                "floors":   [4],
                "perf":     "MAX",
                "node_col": "node",
                "node_val": "'0'",        # VARCHAR node in apoc/anomaly
            },
            {
                "mode":     "PURE_FICTION",
                "table":    "pure_fiction_by_cost_archetypes",
                "floors":   [4],
                "perf":     "MAX",
                "node_col": "node",
                "node_val": "'0'",
            },
            {
                "mode":     "ANOMALY_F0",
                "table":    "anomaly_by_cost_archetypes",
                "floors":   [0],
                "perf":     "MIN",
                "node_col": None,
                "node_val": None,
            },
            {
                "mode":     "ANOMALY_F4",
                "table":    "anomaly_by_cost_archetypes",
                "floors":   [4],
                "perf":     "MIN",
                "node_col": None,
                "node_val": None,
            },
        ]

    # ------------------------------------------------------------------
    # Helpers (identical semantics to HonkaiCostTeamMetaAnalyzer)
    # ------------------------------------------------------------------

    @staticmethod
    def _floor_filter(floors: list) -> str:
        if len(floors) == 1:
            return f"AND floor = {floors[0]}"
        return f"AND floor IN ({', '.join(str(f) for f in floors)})"

    @staticmethod
    def _node_filter(task: dict) -> str:
        if task["node_col"] is None:
            return ""
        return f"AND {task['node_col']} = {task['node_val']}"

    @staticmethod
    def _recent_filter(table: str) -> str:
        return (
            f"AND version IN ("
            f"SELECT DISTINCT version FROM {table} "
            f"ORDER BY version DESC LIMIT 3)"
        )

    # ------------------------------------------------------------------
    # Query builder
    # ------------------------------------------------------------------

    def _generate_query(self, task: dict, limit_recent: bool = False) -> str:
        floor_f  = self._floor_filter(task["floors"])
        node_f   = self._node_filter(task)
        recent_f = self._recent_filter(task["table"]) if limit_recent else ""

        return f"""
            SELECT
                '{task['mode']}'                                                       AS Game_Mode,
                at_eidolon_level,
                up_to_eidolon_level,
                Archetype_Core,
                estimated_min_cost,
                estimated_max_cost,
                max_eidolon,

                -- Appearance (simple average across versions)
                ROUND(AVG(Usage_pct), 2)                                    AS Simple_Avg_Appearance,

                -- Score aggregates
                ROUND(AVG(Average_Score), 2)                                          AS Simple_Avg_Score,
                ROUND(SUM(Average_Score * Samples) / NULLIF(SUM(Samples), 0), 2)      AS Weighted_Avg_Score,
                ROUND(SUM(Median_Score  * Samples) / NULLIF(SUM(Samples), 0), 2)      AS Weighted_Avg_Median,
                {task['perf']}(Average_Score)                                          AS Best_Version_Avg,

                -- Full-star rate: recalculate from raw counts, NOT avg of pct
                SUM(Total_Full_Clears)                                                  AS Total_Total_Full_Clears,
                SUM(Samples)                                                           AS Total_Samples,
                ROUND(
                    100.0 * SUM(Total_Full_Clears) / NULLIF(SUM(Samples), 0),
                    2
                )                                                                      AS Full_Star_Rate_pct,

                -- Sustain rate: recalculate from raw counts, NOT avg of pct
                SUM(Sustain_Samples)                                                     AS Total_Sustain_Samples,
                ROUND(
                    100.0 * SUM(Sustain_Samples) / NULLIF(SUM(Samples), 0),
                    2
                )                                                                      AS Sustain_Rate_pct,

                -- Metadata
                COUNT(DISTINCT version)                                                AS Version_Count,
                STRING_AGG(DISTINCT version, ', ' ORDER BY version DESC)               AS Versions_Used
            FROM {task['table']}
            WHERE Samples > 0
              {floor_f}
              {node_f}
              {recent_f}
            GROUP BY
                1,  -- Game_Mode
                2,  -- at_eidolon_level
                3,  -- up_to_eidolon_level
                4,  -- Archetype_Core
                5,  -- estimated_min_cost
                6,  -- estimated_max_cost
                7   -- max_eidolon
        """

    # ------------------------------------------------------------------
    # Main runner
    # ------------------------------------------------------------------

    def run_analysis(self):
        con = duckdb.connect(self.db_path)
        all_history: list[pd.DataFrame] = []
        all_recent:  list[pd.DataFrame] = []

        print(f"Starting By-Cost Archetype Meta Analysis on {self.db_path}...")

        for task in self.tasks:
            try:
                df_h = con.execute(self._generate_query(task, limit_recent=False)).df()
                if not df_h.empty:
                    all_history.append(df_h)

                df_r = con.execute(self._generate_query(task, limit_recent=True)).df()
                if not df_r.empty:
                    all_recent.append(df_r)

                print(f"  + {task['mode']:15s}  history={len(df_h):,}  recent={len(df_r):,}")
            except Exception as e:
                print(f"  ! Error on {task['mode']}: {e}")

        if not all_history and not all_recent:
            print("No data found. Exiting.")
            con.close()
            return

        con.execute("BEGIN TRANSACTION")
        try:
            if all_history:
                full_df = pd.concat(all_history, ignore_index=True)
                con.execute("DROP TABLE IF EXISTS by_cost_archetype_meta_summary")
                con.execute("CREATE TABLE by_cost_archetype_meta_summary AS SELECT * FROM full_df")
                print(f"\n  Wrote by_cost_archetype_meta_summary     ({len(full_df):,} rows)")

            if all_recent:
                recent_df = pd.concat(all_recent, ignore_index=True)
                con.execute("DROP TABLE IF EXISTS by_cost_archetype_recent_meta_summary")
                con.execute(
                    "CREATE TABLE by_cost_archetype_recent_meta_summary AS SELECT * FROM recent_df"
                )
                print(f"  Wrote by_cost_archetype_recent_meta_summary ({len(recent_df):,} rows)")

            con.execute("COMMIT")
            print(
                "\n>>> Analysis complete. "
                "Tables 'by_cost_archetype_meta_summary' and "
                "'by_cost_archetype_recent_meta_summary' are now live."
            )
        except Exception as e:
            con.execute("ROLLBACK")
            print(f"\n>>> Error during DB write: {e}")
        finally:
            con.close()


class HonkaiCostCharacterMetaAnalyzer:
    """
    Aggregates *_by_cost_chars tables across all versions (and optionally
    the 3 most-recent versions), grouping by:
        estimated_min_cost, estimated_max_cost, Character

    Unlike the team/archetype-level tables, the character-level tables have
    no `Team`, `Archetype_Core`, `max_eidolon`, or `has_sustain` column.
    Instead they carry a raw `Sustain_Samples` (like Full_Star_Clears), which
    is RE-CALCULATED here as:
        SUM(Sustain_Samples) / SUM(Samples)
    rather than averaging the pre-computed Sustain_Percentage_pct.

    Full_Star_Rate is likewise RE-CALCULATED as:
        SUM(Full_Star_Clears) / SUM(Samples)
    rather than averaging the pre-computed percentages.
    """

    def __init__(self, db_name=os.getenv("DB_File")):
        self.db_path = db_name
        self.tasks = [
            {
                "mode":     "MOC",
                "table":    "moc_by_cost_chars",
                "floors":   [10, 12],
                "perf":     "MIN",        # lower cycles-used = better for MOC
                "node_col": "node",
                "node_val": "0",          # integer node; keep only node 0 (combined)
            },
            {
                "mode":     "APOC",
                "table":    "apoc_by_cost_chars",
                "floors":   [4],
                "perf":     "MAX",
                "node_col": "node",
                "node_val": "'0'",        # VARCHAR node in apoc/anomaly
            },
            {
                "mode":     "PURE_FICTION",
                "table":    "pure_fiction_by_cost_chars",
                "floors":   [4],
                "perf":     "MAX",
                "node_col": "node",
                "node_val": "'0'",
            },
            {
                "mode":     "ANOMALY_F0",
                "table":    "anomaly_by_cost_chars",
                "floors":   [0],
                "perf":     "MIN",
                "node_col": None,
                "node_val": None,
            },
            {
                "mode":     "ANOMALY_F4",
                "table":    "anomaly_by_cost_chars",
                "floors":   [4],
                "perf":     "MIN",
                "node_col": None,
                "node_val": None,
            },
        ]

    # ------------------------------------------------------------------
    # Helpers (identical semantics to HonkaiCostTeamMetaAnalyzer)
    # ------------------------------------------------------------------

    @staticmethod
    def _floor_filter(floors: list) -> str:
        if len(floors) == 1:
            return f"AND floor = {floors[0]}"
        return f"AND floor IN ({', '.join(str(f) for f in floors)})"

    @staticmethod
    def _node_filter(task: dict) -> str:
        if task["node_col"] is None:
            return ""
        return f"AND {task['node_col']} = {task['node_val']}"

    @staticmethod
    def _recent_filter(table: str) -> str:
        return (
            f"AND version IN ("
            f"SELECT DISTINCT version FROM {table} "
            f"ORDER BY version DESC LIMIT 3)"
        )

    # ------------------------------------------------------------------
    # Query builder
    # ------------------------------------------------------------------

    def _generate_query(self, task: dict, limit_recent: bool = False) -> str:
        floor_f  = self._floor_filter(task["floors"])
        node_f   = self._node_filter(task)
        recent_f = self._recent_filter(task["table"]) if limit_recent else ""

        return f"""
            SELECT
                '{task['mode']}'                                                       AS Game_Mode,
                at_eidolon_level,
                up_to_eidolon_level,
                Character,
                estimated_min_cost,
                estimated_max_cost,
                max_eidolon,

                -- Appearance (simple average across versions)
                ROUND(AVG(Appearance_Rate_pct), 2)                                    AS Simple_Avg_Appearance,

                -- Score aggregates
                ROUND(AVG(Average_Score), 2)                                          AS Simple_Avg_Score,
                ROUND(SUM(Average_Score * Samples) / NULLIF(SUM(Samples), 0), 2)      AS Weighted_Avg_Score,
                ROUND(SUM(Median_Score  * Samples) / NULLIF(SUM(Samples), 0), 2)      AS Weighted_Avg_Median,
                {task['perf']}(Average_Score)                                          AS Best_Version_Avg,

                -- Full-star rate: recalculate from raw counts, NOT avg of pct
                SUM(Total_Full_Clears)                                                  AS Total_Full_Star_Clears,
                SUM(Samples)                                                           AS Total_Samples,
                ROUND(
                    100.0 * SUM(Total_Full_Clears) / NULLIF(SUM(Samples), 0),
                    2
                )                                                                      AS Full_Star_Rate_pct,

                -- Sustain rate: recalculate from raw counts, NOT avg of pct
                SUM(Sustain_Samples)                                                     AS Total_Sustain_Samples,
                ROUND(
                    100.0 * SUM(Sustain_Samples) / NULLIF(SUM(Samples), 0),
                    2
                )                                                                      AS Sustain_Rate_pct,

                -- Metadata
                COUNT(DISTINCT version)                                                AS Version_Count,
                STRING_AGG(DISTINCT version, ', ' ORDER BY version DESC)               AS Versions_Used
            FROM {task['table']}
            WHERE Samples > 0
              {floor_f}
              {node_f}
              {recent_f}
            GROUP BY
                1,  -- Game_Mode
                2,  -- at_eidolon_level
                3,  -- up_to_eidolon_level
                4,  -- Character
                5,  -- estimated_min_cost
                6 ,  -- estimated_max_cost
                7   -- max_eidolon
        """

    # ------------------------------------------------------------------
    # Main runner
    # ------------------------------------------------------------------

    def run_analysis(self):
        con = duckdb.connect(self.db_path)
        all_history: list[pd.DataFrame] = []
        all_recent:  list[pd.DataFrame] = []

        print(f"Starting By-Cost Character Meta Analysis on {self.db_path}...")

        for task in self.tasks:
            try:
                df_h = con.execute(self._generate_query(task, limit_recent=False)).df()
                if not df_h.empty:
                    all_history.append(df_h)

                df_r = con.execute(self._generate_query(task, limit_recent=True)).df()
                if not df_r.empty:
                    all_recent.append(df_r)

                print(f"  + {task['mode']:15s}  history={len(df_h):,}  recent={len(df_r):,}")
            except Exception as e:
                print(f"  ! Error on {task['mode']}: {e}")

        if not all_history and not all_recent:
            print("No data found. Exiting.")
            con.close()
            return

        con.execute("BEGIN TRANSACTION")
        try:
            if all_history:
                full_df = pd.concat(all_history, ignore_index=True)
                con.execute("DROP TABLE IF EXISTS by_cost_character_meta_summary")
                con.execute("CREATE TABLE by_cost_character_meta_summary AS SELECT * FROM full_df")
                print(f"\n  Wrote by_cost_character_meta_summary     ({len(full_df):,} rows)")

            if all_recent:
                recent_df = pd.concat(all_recent, ignore_index=True)
                con.execute("DROP TABLE IF EXISTS by_cost_character_recent_meta_summary")
                con.execute(
                    "CREATE TABLE by_cost_character_recent_meta_summary AS SELECT * FROM recent_df"
                )
                print(f"  Wrote by_cost_character_recent_meta_summary ({len(recent_df):,} rows)")

            con.execute("COMMIT")
            print(
                "\n>>> Analysis complete. "
                "Tables 'by_cost_character_meta_summary' and "
                "'by_cost_character_recent_meta_summary' are now live."
            )
        except Exception as e:
            con.execute("ROLLBACK")
            print(f"\n>>> Error during DB write: {e}")
        finally:
            con.close()


if __name__ == "__main__":
    team_analyzer = HonkaiCostTeamMetaAnalyzer()
    team_analyzer.run_analysis()

    archetype_analyzer = HonkaiCostArchetypeMetaAnalyzer()
    archetype_analyzer.run_analysis()

    character_analyzer = HonkaiCostCharacterMetaAnalyzer()
    character_analyzer.run_analysis()
