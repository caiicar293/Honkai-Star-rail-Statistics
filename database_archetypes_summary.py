import duckdb
import pandas as pd

class HonkaiMetaAnalyzer:
    def __init__(self, db_path="honkai_star_rail_stats2.duckdb"):
        self.db_path = db_path
        # Define the metadata for each mode
        self.tasks = [
            {"mode": "MOC", "table": "moc_stats", "floor": 12, "perf": "MIN", "node_col": "node", "node_val": "0"},
            {"mode": "APOC", "table": "apoc_stats", "floor": 4, "perf": "MAX", "node_col": "node", "node_val": "0"},
            {"mode": "PURE_FICTION", "table": "pure_fiction_stats", "floor": 4, "perf": "MAX", "node_col": "node", "node_val": "0"},
            {"mode": "ANOMALY_F0", "table": "anomaly_stats", "floor": 0, "perf": "MIN", "node_col": None, "node_val": None},
            {"mode": "ANOMALY_F4", "table": "anomaly_stats", "floor": 4, "perf": "MIN", "node_col": None, "node_val": None}
        ]

    def _generate_query(self, task, limit_recent=False):
        """Generates the SQL query for a specific task and version window."""
        node_filter = ""
        if task['node_col'] is not None:
            node_filter = f"AND {task['node_col']} = {task['node_val']}"

        recent_filter = ""
        if limit_recent:
            # Subquery to grab only the top 3 distinct versions for this specific table
            recent_filter = f"AND version IN (SELECT DISTINCT version FROM {task['table']} ORDER BY version DESC LIMIT 3)"

        return f"""
            SELECT 
                '{task['mode']}' as Game_Mode,
                eidolon_level,
                Archetype, 
                ROUND(AVG(Appearance_Rate_pct), 2) as Avg_Appearance_Rate,
                ROUND(SUM(Average_Score * Samples) / SUM(Samples), 2) as Weighted_Avg_Score,
                {task['perf']}(Average_Score) as Best_Version_Avg,
                SUM(Samples) as Total_Samples
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

        print(f">>> Connecting to {self.db_path}...")

        for task in self.tasks:
            try:
                # 1. Process Full History
                df_h = con.execute(self._generate_query(task, limit_recent=False)).df()
                if not df_h.empty: all_history.append(df_h)

                # 2. Process Recent (Top 3 Versions)
                df_r = con.execute(self._generate_query(task, limit_recent=True)).df()
                if not df_r.empty: all_recent.append(df_r)
                
                print(f"  + Processed {task['mode']} (Floor {task['floor']})")
            except Exception as e:
                print(f"  ! Error processing {task['mode']}: {e}")

        # 3. Save to Database with Transactions
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
            print("\n>>> Success! Summary tables refreshed in DuckDB.")
        except Exception as e:
            con.execute("ROLLBACK")
            print(f"\n>>> CRITICAL ERROR: Transaction failed and rolled back. {e}")
        finally:
            con.close()

# --- Execution ---
if __name__ == "__main__":
    analyzer = HonkaiMetaAnalyzer()
    analyzer.run_analysis()