import duckdb
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()
class HonkaiMetaAnalyzer:
    def __init__(self, db_name=os.getenv("DB_File")):
        self.db_path = db_name
        self.tasks = [
            {"mode": "MOC", "table": "moc_stats_archetypes", "floor": 12, "perf": "MIN", "node_col": "node", "node_val": "0"},
            {"mode": "APOC", "table": "apoc_stats_archetypes", "floor": 4, "perf": "MAX", "node_col": "node", "node_val": "0"},
            {"mode": "PURE_FICTION", "table": "pure_fiction_stats_archetypes", "floor": 4, "perf": "MAX", "node_col": "node", "node_val": "0"},
            {"mode": "ANOMALY_F0", "table": "anomaly_stats_archetypes", "floor": 0, "perf": "MIN", "node_col": None, "node_val": None},
            {"mode": "ANOMALY_F4", "table": "anomaly_stats_archetypes", "floor": 4, "perf": "MIN", "node_col": None, "node_val": None}
        ]

    def _generate_query(self, task, limit_recent=False):
        node_filter = f"AND {task['node_col']} = {task['node_val']}" if task['node_col'] is not None else ""
        
        recent_filter = ""
        if limit_recent:
            recent_filter = f"AND version IN (SELECT DISTINCT version FROM {task['table']} ORDER BY version DESC LIMIT 3)"

        return f"""
            SELECT 
                '{task['mode']}' as Game_Mode,
                eidolon_level,
                Archetype_Core, 
                -- Appearance Logic
                ROUND(AVG(Usage_pct), 2) as Simple_Avg_Appearance,
                
                -- Scoring Logic
                ROUND(AVG(Average_Score), 2) as Simple_Avg_Score,
                ROUND(SUM(Average_Score * Samples) / SUM(Samples), 2) as Weighted_Avg_Score,
                {task['perf']}(Average_Score) as Best_Version_Avg,
                
                -- Metadata
                SUM(Samples) as Total_Samples,
                ROUND(SUM(Sustain_Samples) * 1.0 / NULLIF(SUM(Samples), 0), 2) as Sustain_Percentage,
                STRING_AGG(DISTINCT version, ', ' ORDER BY version DESC) as Versions_Used
            FROM {task['table']}
            WHERE Samples > 0 
              AND floor = {task['floor']}
              {node_filter}
              {recent_filter}
            GROUP BY 1, 2, 3
        """

    def run_analysis(self):
        con = duckdb.connect(self.db_path)
        all_history = []
        all_recent = []

        for task in self.tasks:
            try:
                # Full History
                df_h = con.execute(self._generate_query(task, limit_recent=False)).df()
                if not df_h.empty: all_history.append(df_h)

                # Recent (Last 3)
                df_r = con.execute(self._generate_query(task, limit_recent=True)).df()
                if not df_r.empty: all_recent.append(df_r)
                
                print(f"  + Successfully aggregated {task['mode']}")
            except Exception as e:
                print(f"  ! Error on {task['mode']}: {e}")

        con.execute("BEGIN TRANSACTION")
        try:
            if all_history:
                full_df = pd.concat(all_history, ignore_index=True)
                con.execute("DROP TABLE IF EXISTS archetype_meta_summary")
                con.execute("CREATE TABLE archetype_meta_summary AS SELECT * FROM full_df")
            
            if all_recent:
                recent_df = pd.concat(all_recent, ignore_index=True)
                con.execute("DROP TABLE IF EXISTS archetype_recent_meta_summary")
                con.execute("CREATE TABLE archetype_recent_meta_summary AS SELECT * FROM recent_df")
            
            con.execute("COMMIT")
            print("\n>>> Analysis complete. Tables 'archetype_meta_summary' and 'archetype_recent_meta_summary' are ready.")
        except Exception as e:
            con.execute("ROLLBACK")
            print(f"\n>>> Error during DB write: {e}")
        finally:
            con.close()

if __name__ == "__main__":
    analyzer = HonkaiMetaAnalyzer()
    analyzer.run_analysis()