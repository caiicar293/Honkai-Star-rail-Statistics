import duckdb
import os
from dotenv import load_dotenv

load_dotenv()
con = duckdb.connect(os.getenv("DB_File"))

def get_weighted_stats_query(table_name, value_col, node_col=None):
    """Generates the CTE for weighted averages and weighted quantiles."""
    return f"""
    WITH base_stats AS (
        SELECT 
            version,
            eidolon_level,
            {f"{node_col}," if node_col else ""}
            {value_col},
            Count,
            SUM(Count) OVER (PARTITION BY version, eidolon_level {f', {node_col}' if node_col else ''} ORDER BY {value_col}) AS cumulative_count,
            SUM(Count) OVER (PARTITION BY version, eidolon_level {f', {node_col}' if node_col else ''}) AS total_count
        FROM {table_name}
    ),
    quantiles AS (
        SELECT 
            version,
            {f"{node_col}," if node_col else ""}
            eidolon_level,
            SUM(Count) OVER (PARTITION BY version, eidolon_level {f', {node_col}' if node_col else ''}) AS Total_Samples,
            SUM({value_col} * Count) OVER (PARTITION BY version, eidolon_level {f', {node_col}' if node_col else ''}) / total_count AS Average_Value,
            MIN({value_col}) OVER (PARTITION BY version, eidolon_level {f', {node_col}' if node_col else ''}) AS Min_Val,
            MIN(CASE WHEN cumulative_count >= total_count * 0.25 THEN {value_col} END) OVER (PARTITION BY version, eidolon_level {f', {node_col}' if node_col else ''}) AS P25,
            MIN(CASE WHEN cumulative_count >= total_count * 0.50 THEN {value_col} END) OVER (PARTITION BY version, eidolon_level {f', {node_col}' if node_col else ''}) AS Median,
            MIN(CASE WHEN cumulative_count >= total_count * 0.75 THEN {value_col} END) OVER (PARTITION BY version, eidolon_level {f', {node_col}' if node_col else ''}) AS P75,
            MAX({value_col}) OVER (PARTITION BY version, eidolon_level {f', {node_col}' if node_col else ''}) AS Max_Val,
            STDDEV_SAMP({value_col}) OVER (PARTITION BY version, eidolon_level {f', {node_col}' if node_col else ''}) AS Std_Dev
        FROM base_stats
    )
    SELECT DISTINCT * FROM quantiles
    """

def process_mode(table_name, value_col, node_col, rank_order, target_table):
    print(f"Saving Standard Summary to Table: {target_table}...")
    stats_query = get_weighted_stats_query(table_name, value_col, node_col)
    
    query = f"""
    CREATE OR REPLACE TABLE {target_table} AS
    WITH stats_calc AS ({stats_query}),
    metadata_snapshot AS (
        SELECT version, {node_col}, eidolon_level,
               E0_pct, E1_pct, E2_pct, E3_pct, E4_pct, E5_pct, E6_pct
        FROM {table_name}
        QUALIFY ROW_NUMBER() OVER(PARTITION BY version, {node_col}, eidolon_level ORDER BY {value_col} {rank_order}) = 1
    )
    SELECT 
        s.version, s.{node_col}, s.eidolon_level, s.Total_Samples, s.Average_Value,
        s.Min_Val, s.P25, s.Median, s.P75, s.Max_Val, s.Std_Dev,
        m.E0_pct, m.E1_pct, m.E2_pct, m.E3_pct, m.E4_pct, m.E5_pct, m.E6_pct
    FROM stats_calc s
    LEFT JOIN metadata_snapshot m 
        ON s.version = m.version AND s.{node_col} = m.{node_col} AND s.eidolon_level = m.eidolon_level
    ORDER BY s.version ASC, s.{node_col} ASC, s.eidolon_level ASC
    """
    con.execute(query)
    con.table(target_table).show(max_rows=5, max_width=10000)

def process_multi_mode(table_name, value_col, rank_order, target_table):
    print(f"Saving Multi-Mode Summary to Table: {target_table}...")
    stats_query = get_weighted_stats_query(table_name, value_col)
    
    query = f"""
    CREATE OR REPLACE TABLE {target_table} AS
    WITH stats_calc AS ({stats_query}),
    metadata_snapshot AS (
        SELECT version, eidolon_level,
               E0_pct, E1_pct, E2_pct, E3_pct, E4_pct, E5_pct, E6_pct
        FROM {table_name}
        QUALIFY ROW_NUMBER() OVER(PARTITION BY version, eidolon_level ORDER BY {value_col} {rank_order}) = 1
    )
    SELECT 
        s.version, s.eidolon_level, s.Total_Samples, s.Average_Value,
        s.Min_Val, s.P25, s.Median, s.P75, s.Max_Val, s.Std_Dev,
        m.E0_pct, m.E1_pct, m.E2_pct, m.E3_pct, m.E4_pct, m.E5_pct, m.E6_pct
    FROM stats_calc s
    LEFT JOIN metadata_snapshot m ON s.version = m.version AND s.eidolon_level = m.eidolon_level
    ORDER BY s.version ASC, s.eidolon_level ASC
    """
    con.execute(query)
    con.table(target_table).show(max_rows=5, max_width=10000)

# --- EXECUTION ---

# Standard Modes 
process_mode("moc_distributions", "Cycles", "node", "DESC", "moc_distributions_summaries")
process_mode("pure_fiction_distributions", "Points", "node", "ASC", "pure_fiction_distributions_summaries")
process_mode("apoc_distributions", "Scores", "node", "ASC", "apoc_distributions_summaries")
process_mode("anomaly_distributions", "Cycles", "floor", "DESC", "anomaly_distributions_summaries")

# Multi-Modes
process_multi_mode("moc_stats_dual_distributions", "Cycles", "DESC", "moc_dual_distributions_summaries")
process_multi_mode("pure_fiction_stats_dual_distributions", "Points", "ASC", "pure_fiction_dual_distributions_summaries")
process_multi_mode("apoc_stats_dual_distributions", "Scores", "ASC", "apoc_dual_distributions_summaries")
process_multi_mode("anomaly_stats_triple_distributions", "Cycles", "DESC", "anomaly_triple_distributions_summaries")

# Always a good habit to close the connection to avoid the "Permission Denied" git error
con.close()