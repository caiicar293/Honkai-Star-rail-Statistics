import duckdb
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Character metadata fields extracted from characters.json via enrich_char_data
# ---------------------------------------------------------------------------
CHAR_META_COLS = ["role", "availability", "element", "path", "release_phase"]

# Eidolon breakdown columns present in character_stats
EIDOLON_COLS = [
    "Eidolon_0_pct_pct", "Eidolon_1_pct_pct", "Eidolon_2_pct_pct",
    "Eidolon_3_pct_pct", "Eidolon_4_pct_pct", "Eidolon_5_pct_pct",
    "Eidolon_6_pct_pct",
]


class CharacterMetaAnalyzer:
    """
    Generates two cross-mode summary tables from `character_stats`:

    - character_meta_summary        → all versions (full history)
    - character_recent_meta_summary → last 3 versions per mode

    Each task maps to one distinct (mode, floor, node) slice in the DB.
    The DB has these distinct mode values:
        MOC              floor=12  node=0/1/2
        MOC_LATE_LEGACY  floor=12  node=0/1/2
        MOC_LEGACY       floor=10  node=0/1/2
        PURE_FICTION         floor=4   node=0/1/2
        PURE_FICTION_LEGACY  floor=4   node=0/1/2
        APOC             floor=4   node=0/1/2
        ANOMALY          floor=0/1/2/3/4  node=NULL

    All tasks use node=0 (combined) unless stated otherwise.
    ANOMALY floor=0 is the "all floors combined" aggregate row.
    ANOMALY floor=4 is the hardest individual floor.
    """

    def __init__(self, db_name: str = os.getenv("DB_File")):
        self.db_path = db_name

        self.tasks = [
            # ── Modern MoC (floor 12) ──────────────────────────────────────
            {
                "mode":        "MOC",
                "display":     "MOC",
                "floors":      [12],
                "node_vals":   ["0"],
                "perf":        "MIN",   # lower cycles = better
            },
            # ── Late-legacy MoC (floor 12, older versions) ────────────────
            {
                "mode":        "MOC_LATE_LEGACY",
                "display":     "MOC_LATE_LEGACY",
                "floors":      [12],
                "node_vals":   ["0"],
                "perf":        "MIN",
            },
            # ── Early-legacy MoC (floor 10) ───────────────────────────────
            {
                "mode":        "MOC_LEGACY",
                "display":     "MOC_LEGACY",
                "floors":      [10],
                "node_vals":   ["0"],
                "perf":        "MIN",
            },
            # ── Apocalyptic Shadow ────────────────────────────────────────
            {
                "mode":        "APOC",
                "display":     "APOC",
                "floors":      [4],
                "node_vals":   ["0"],
                "perf":        "MAX",   # higher score = better
            },
            # ── Pure Fiction (modern) ─────────────────────────────────────
            {
                "mode":        "PURE_FICTION",
                "display":     "PURE_FICTION",
                "floors":      [4],
                "node_vals":   ["0"],
                "perf":        "MAX",
            },
            # ── Pure Fiction (legacy) ─────────────────────────────────────
            {
                "mode":        "PURE_FICTION_LEGACY",
                "display":     "PURE_FICTION_LEGACY",
                "floors":      [4],
                "node_vals":   ["0"],
                "perf":        "MAX",
            },
            # ── Anomaly Arbitration — all-floors combined row ─────────────
            {
                "mode":        "ANOMALY",
                "display":     "ANOMALY_ALL",
                "floors":      [0],
                "node_vals":   [None],
                "perf":        "MIN",
            },
            # ── Anomaly Arbitration — hardest floor only ──────────────────
            {
                "mode":        "ANOMALY",
                "display":     "ANOMALY_F4",
                "floors":      [4],
                "node_vals":   [None],
                "perf":        "MIN",
            },
            {
                "mode":        "ANOMALY",
                "display":     "ANOMALY_F5",
                "floors":      [5],
                "node_vals":   [None],
                "perf":        "MIN",
            },
        ]

    # ------------------------------------------------------------------
    # SQL helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _floor_filter(floors: list) -> str:
        if len(floors) == 1:
            return f"AND floor = {floors[0]}"
        return f"AND floor IN ({', '.join(str(f) for f in floors)})"

    @staticmethod
    def _node_filter(node_vals: list) -> str:
        """
        node is stored as VARCHAR in the DB.
        None means the mode has no node column — skip the filter entirely.
        """
        non_null = [v for v in node_vals if v is not None]
        has_null = any(v is None for v in node_vals)

        if has_null and not non_null:
            # ANOMALY — no node column, skip filter
            return ""
        if non_null and not has_null:
            quoted = ", ".join(f"'{v}'" for v in non_null)
            return f"AND node IN ({quoted})"
        # mixed
        quoted = ", ".join(f"'{v}'" for v in non_null)
        return f"AND (node IN ({quoted}) OR node IS NULL)"

    def _generate_query(self, task: dict, limit_recent: bool = False) -> str:
        display      = task["display"]
        mode         = task["mode"]
        floor_filter = self._floor_filter(task["floors"])
        node_filter  = self._node_filter(task["node_vals"])
        perf         = task["perf"]

        recent_filter = ""
        if limit_recent:
            recent_filter = (
                f"AND version IN ("
                f"  SELECT DISTINCT version FROM character_stats"
                f"  WHERE mode = '{mode}'"
                f"  ORDER BY version DESC LIMIT 3"
                f")"
            )

        weighted_eid = "\n                ,".join(
            f"ROUND(SUM({e} * Samples) / NULLIF(SUM(Samples), 0), 4) AS {e}"
            for e in EIDOLON_COLS
        )

        meta_cols = "\n                ,".join(
            f"MAX({col}) AS {col}"
            for col in CHAR_META_COLS
        )

        return f"""
            SELECT
                '{display}'                                                                  AS Game_Mode,
                Character,
                at_eidolon_level,
                up_to_eidolon_level,

                {meta_cols},

                -- Appearance
                ROUND(AVG(Appearance_Rate_pct), 4)                                          AS Simple_Avg_Appearance,
                ROUND(
                    SUM(Appearance_Rate_pct * Samples) / NULLIF(SUM(Samples), 0), 4
                )                                                                            AS Weighted_Avg_Appearance,
                -- Metadata
                ROUND(
                    100.0 * SUM(Sustain_Samples) / NULLIF(SUM(Samples), 0),      
                    2
                )                                                                       AS Sustain_Percentage,                                   
                ROUND(
                    100.0 * SUM(Total_Full_Clears) / NULLIF(SUM(Samples), 0),
                    2
                )                                                                      AS Full_Star_Rate_pct,

                -- Score / Cycles / Points
                ROUND(AVG(Average_Score), 4)                                                AS Simple_Avg_Score,
                ROUND(SUM(Average_Score * Samples) / NULLIF(SUM(Samples), 0), 4)            AS Weighted_Avg_Score,
                ROUND(SUM(Median_Score  * Samples) / NULLIF(SUM(Samples), 0), 4)            AS Weighted_Avg_Median,
                {perf}(Average_Score)                                                        AS Best_Version_Score,

                -- Eidolon breakdown (weighted by Samples)
                {weighted_eid},

                -- Volume / coverage
                SUM(Samples)                                                                 AS Total_Samples,
                COUNT(DISTINCT version)                                                      AS Versions_Count,
                STRING_AGG(DISTINCT version, ', ' ORDER BY version DESC)                    AS Versions_Used

            FROM character_stats
            WHERE Samples > 0
              AND mode = '{mode}'
              {floor_filter}
              {node_filter}
              {recent_filter}
            GROUP BY 1, 2, 3, 4
        """

    # ------------------------------------------------------------------
    # Main execution
    # ------------------------------------------------------------------

    def run_analysis(self):
        con = duckdb.connect(self.db_path)
        all_history: list[pd.DataFrame] = []
        all_recent:  list[pd.DataFrame] = []

        for task in self.tasks:
            label = task["display"]
            try:
                df_h = con.execute(self._generate_query(task, limit_recent=False)).df()
                if not df_h.empty:
                    all_history.append(df_h)
                    print(f"  + History  {label}: {len(df_h):,} rows")
                else:
                    print(f"  - History  {label}: 0 rows (skipped)")

                df_r = con.execute(self._generate_query(task, limit_recent=True)).df()
                if not df_r.empty:
                    all_recent.append(df_r)
                    print(f"  + Recent   {label}: {len(df_r):,} rows")
                else:
                    print(f"  - Recent   {label}: 0 rows (skipped)")

            except Exception as e:
                print(f"  ! Error on {label}: {e}")

        # ---- Write to DB ------------------------------------------------
        con.execute("BEGIN TRANSACTION")
        try:
            if all_history:
                full_df = pd.concat(all_history, ignore_index=True)
                con.execute("DROP TABLE IF EXISTS character_meta_summary")
                con.execute("CREATE TABLE character_meta_summary AS SELECT * FROM full_df")
                print(f"\n  Wrote character_meta_summary        ({len(full_df):,} rows)")

            if all_recent:
                recent_df = pd.concat(all_recent, ignore_index=True)
                con.execute("DROP TABLE IF EXISTS character_recent_meta_summary")
                con.execute("CREATE TABLE character_recent_meta_summary AS SELECT * FROM recent_df")
                print(f"  Wrote character_recent_meta_summary ({len(recent_df):,} rows)")

            con.execute("COMMIT")
            print(
                "\n>>> Analysis complete. "
                "Tables 'character_meta_summary' and 'character_recent_meta_summary' are ready."
            )

        except Exception as e:
            con.execute("ROLLBACK")
            print(f"\n>>> Error during DB write: {e}")
        finally:
            con.close()


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    analyzer = CharacterMetaAnalyzer()
    analyzer.run_analysis()
