import duckdb
import pandas as pd

# 1. Connect to your persistent DuckDB file
con = duckdb.connect("honkai_star_rail_stats.duckdb")

# 2. Updated Configuration
# Note: We specify the 'node' value for each mode to handle the Anomaly edge case.
configs = {
    "MOC": {"table": "moc_stats_teams", "floor": 12, "perf": "MIN", "node_val": "0"},
    "APOC": {"table": "apoc_stats_teams", "floor": 4, "perf": "MAX", "node_val": "0"},
    "PURE_FICTION": {"table": "pure_fiction_stats_teams", "floor": 4, "perf": "MAX", "node_val": "0"},
    # Anomaly usually uses 'Both' because it's a single run, not split nodes.
    "ANOMALY": {"table": "anomaly_stats_teams", "floor": 4, "perf": "MIN", "node_val": "'Both'"}
}

all_data = []

print(">>> Analyzing Archetypes for all modes and eidolons...")

for mode, cfg in configs.items():
    print(f"Aggregating {mode} data (Floor {cfg['floor']} | Node {cfg['node_val']})...")
    
    # The query now uses a dynamic node filter and groups by eidolon_level
    query = f"""
        SELECT 
            '{mode}' as Game_Mode,
            eidolon_level,
            Archetype, 
            ROUND(AVG(Appearance_Rate_pct), 2) as Avg_Appearance_Rate,
            ROUND(SUM(Average_Score * Samples) / SUM(Samples), 2) as Weighted_Avg_Score,
            {cfg['perf']}(Average_Score) as Best_Version_Avg,
            SUM(Samples) as Total_Samples
        FROM {cfg['table']}
        WHERE Samples > 0 
          AND floor = {cfg['floor']}
          AND node = {cfg['node_val']}
        GROUP BY 1, 2, 3
    """
    
    try:
        df_mode = con.execute(query).df()
        if not df_mode.empty:
            all_data.append(df_mode)
        else:
            print(f"  ! No data found for {mode} with these filters.")
    except Exception as e:
        print(f"  ! Error processing {mode}: {e}")

# 3. Combine and Save back to DuckDB
if all_data:
    final_summary_df = pd.concat(all_data, ignore_index=True)
    
    # Use a transaction to ensure data safety
    con.execute("BEGIN TRANSACTION")
    try:
        # 1. DELETE the old table if it exists
        con.execute("DROP TABLE IF EXISTS archetype_meta_summary")
        
        # 2. ADD the new one from your combined DataFrame
        con.execute("CREATE TABLE archetype_meta_summary AS SELECT * FROM final_summary_df")
        
        # 3. COMMIT the changes (makes them permanent)
        con.execute("COMMIT")
        print(f"\n>>> Success! Replaced old table with {len(final_summary_df)} new rows.")
        
    except Exception as e:
        # If anything fails, undo the deletion and go back to the old state
        con.execute("ROLLBACK")
        print(f"\n>>> ERROR: Replacement failed, rolled back to old table. {e}")

    # Show a sample of the results
    print(final_summary_df.sort_values(['Game_Mode', 'eidolon_level']).head(10))
else:
    print("\n>>> Warning: No new data to add.")