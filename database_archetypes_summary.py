import duckdb
import pandas as pd

# 1. Connect to your persistent DuckDB file
con = duckdb.connect("honkai_star_rail_stats2.duckdb")

# 2. Updated Configuration Task List
# node_val: None means the column doesn't exist (Anomaly Case)
analysis_tasks = [
    {"mode": "MOC", "table": "moc_stats", "floor": 12, "perf": "MIN", "node_val": "0"},
    {"mode": "APOC", "table": "apoc_stats", "floor": 4, "perf": "MAX", "node_val": "0"},
    {"mode": "PURE_FICTION", "table": "pure_fiction_stats", "floor": 4, "perf": "MAX", "node_val": "0"},
    {"mode": "ANOMALY_F0", "table": "anomaly_stats", "floor": 0, "perf": "MIN", "node_val": None},
    {"mode": "ANOMALY_F4", "table": "anomaly_stats", "floor": 4, "perf": "MIN", "node_val": None}
]

all_data = []

print(">>> Analyzing Archetypes for all modes and eidolons...")

for task in analysis_tasks:
    mode = task["mode"]
    
    # Dynamically build the node filter
    # If node_val is None, we just inject a "True" statement (1=1) to keep the SQL valid
    node_filter = f"AND node = {task['node_val']}" if task['node_val'] is not None else ""
    
    print(f"Aggregating {mode} data (Floor {task['floor']})...")
    
    query = f"""
        SELECT 
            '{mode}' as Game_Mode,
            eidolon_level,
            Archetype, 
            ROUND(AVG(Appearance_Rate_pct), 2) as Avg_Appearance_Rate,
            ROUND(AVG(Average_Score), 2) as Simple_Avg_,
            ROUND(SUM(Average_Score * Samples) / SUM(Samples), 2) as Weighted_Avg_Score,
            {task['perf']}(Average_Score) as Best_Version_Avg,
            SUM(Samples) as Total_Samples
        FROM {task['table']}
        WHERE Samples > 0 
          AND floor = {task['floor']}
          {node_filter}
        GROUP BY 1, 2, 3
    """
    
    try:
        df_task = con.execute(query).df()
        if not df_task.empty:
            all_data.append(df_task)
        else:
            print(f"  ! No data found for {mode} Floor {task['floor']}.")
    except Exception as e:
        print(f"  ! Error processing {mode}: {e}")

# 3. Combine and Save back to DuckDB
if all_data:
    final_summary_df = pd.concat(all_data, ignore_index=True)
    
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute("DROP TABLE IF EXISTS archetype_meta_summary")
        con.execute("CREATE TABLE archetype_meta_summary AS SELECT * FROM final_summary_df")
        con.execute("COMMIT")
        print(f"\n>>> Success! Updated 'archetype_meta_summary' with {len(final_summary_df)} rows.")
        
    except Exception as e:
        con.execute("ROLLBACK")
        print(f"\n>>> ERROR: Replacement failed, rolled back to old table. {e}")

    # Display preview
    print(final_summary_df.sort_values(['Game_Mode', 'eidolon_level']).head(15))
else:
    print("\n>>> Warning: No data was aggregated.")

con.close()