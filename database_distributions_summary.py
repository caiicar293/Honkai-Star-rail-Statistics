import duckdb
import os
from dotenv import load_dotenv

class StarRailStatsProcessor:
    def __init__(self):
        load_dotenv()
        self.db_path = os.getenv("DB_File")
        self.con = duckdb.connect(self.db_path)

    def _get_weighted_stats_query(self, table_name, value_col, node_col=None):
        """Internal helper to generate the weighted statistics CTE."""
        partition_clause = f"PARTITION BY version, up_to_eidolon_level {f', {node_col}' if node_col else ''}"
        
        return f"""
        WITH base_stats AS (
            SELECT 
                version,
                up_to_eidolon_level,
                {f"{node_col}," if node_col else ""}
                {value_col},
                Count,
                SUM(Count) OVER ({partition_clause} ORDER BY {value_col}) AS cumulative_count,
                SUM(Count) OVER ({partition_clause}) AS total_count,
                -- Pre-calculate weighted components for StdDev
                SUM({value_col} * Count) OVER ({partition_clause}) AS sum_x,
                SUM(POWER({value_col}, 2) * Count) OVER ({partition_clause}) AS sum_x2
            FROM {table_name}
        ),
        quantiles AS (
            SELECT 
                version,
                {f"{node_col}," if node_col else ""}
                up_to_eidolon_level,
                total_count AS Total_Samples,
                sum_x / total_count AS Average_Value,
                MIN({value_col}) OVER ({partition_clause}) AS Min_Val,
                MIN(CASE WHEN cumulative_count >= total_count * 0.25 THEN {value_col} END) OVER ({partition_clause}) AS P25,
                MIN(CASE WHEN cumulative_count >= total_count * 0.50 THEN {value_col} END) OVER ({partition_clause}) AS Median,
                MIN(CASE WHEN cumulative_count >= total_count * 0.75 THEN {value_col} END) OVER ({partition_clause}) AS P75,
                MAX({value_col}) OVER ({partition_clause}) AS Max_Val,
                -- Weighted Sample Standard Deviation Calculation:
                -- SQRT( (Sum(f*x^2) - (Sum(f*x)^2 / Sum(f))) / (Sum(f) - 1) )
                SQRT(
                    NULLIF(sum_x2 - (POWER(sum_x, 2) / total_count), 0) / 
                    NULLIF(total_count - 1, 0)
                ) AS Std_Dev
            FROM base_stats
        )
        SELECT DISTINCT * FROM quantiles
        """

    def process_mode(self, table_name, value_col, node_col, rank_order, target_table):
        """Processes standard distributions (grouped by node/floor)."""
        print(f"--- Saving Standard Summary: {target_table} ---")
        stats_query = self._get_weighted_stats_query(table_name, value_col, node_col)
        
        query = f"""
        CREATE OR REPLACE TABLE {target_table} AS
        WITH stats_calc AS ({stats_query}),
        metadata_snapshot AS (
            SELECT version, {node_col}, up_to_eidolon_level,
                   E0_pct, E1_pct, E2_pct, E3_pct, E4_pct, E5_pct, E6_pct
            FROM {table_name}
            QUALIFY ROW_NUMBER() OVER(PARTITION BY version, {node_col}, up_to_eidolon_level ORDER BY {value_col} {rank_order}) = 1
        )
        SELECT 
            s.version, s.{node_col}, s.up_to_eidolon_level, s.Total_Samples, s.Average_Value,
            s.Min_Val, s.P25, s.Median, s.P75, s.Max_Val, s.Std_Dev,
            m.E0_pct, m.E1_pct, m.E2_pct, m.E3_pct, m.E4_pct, m.E5_pct, m.E6_pct
        FROM stats_calc s
        LEFT JOIN metadata_snapshot m 
            ON s.version = m.version AND s.{node_col} = m.{node_col} AND s.up_to_eidolon_level = m.up_to_eidolon_level
        ORDER BY s.version ASC, s.{node_col} ASC, s.up_to_eidolon_level ASC
        """
        self.con.execute(query)
        self.con.table(target_table).show(max_rows=5, max_width=10000)

    def process_multi_mode(self, table_name, value_col, rank_order, target_table):
        """Processes dual/triple distributions (no node/floor grouping)."""
        print(f"--- Saving Multi-Mode Summary: {target_table} ---")
        stats_query = self._get_weighted_stats_query(table_name, value_col)
        
        query = f"""
        CREATE OR REPLACE TABLE {target_table} AS
        WITH stats_calc AS ({stats_query}),
        metadata_snapshot AS (
            SELECT version, up_to_eidolon_level,
                   E0_pct, E1_pct, E2_pct, E3_pct, E4_pct, E5_pct, E6_pct
            FROM {table_name}
            QUALIFY ROW_NUMBER() OVER(PARTITION BY version, up_to_eidolon_level ORDER BY {value_col} {rank_order}) = 1
        )
        SELECT 
            s.version, s.up_to_eidolon_level, s.Total_Samples, s.Average_Value,
            s.Min_Val, s.P25, s.Median, s.P75, s.Max_Val, s.Std_Dev,
            m.E0_pct, m.E1_pct, m.E2_pct, m.E3_pct, m.E4_pct, m.E5_pct, m.E6_pct
        FROM stats_calc s
        LEFT JOIN metadata_snapshot m ON s.version = m.version AND s.up_to_eidolon_level = m.up_to_eidolon_level
        ORDER BY s.version ASC, s.up_to_eidolon_level ASC
        """
        self.con.execute(query)
        self.con.table(target_table).show(max_rows=5, max_width=10000)

    def close(self):
        """Closes the connection to avoid file locks."""
        self.con.close()
        print("Database connection closed.")

# --- EXECUTION ---

if __name__ == "__main__":
    processor = StarRailStatsProcessor()

    try:
        # 1. Standard Modes
        processor.process_mode("moc_stats_distributions", "Cycles", "node", "DESC", "moc_stats_distributions_summaries")
        processor.process_mode("pure_fiction_stats_distributions", "Points", "node", "ASC", "pure_fiction_stats_distributions_summaries")
        processor.process_mode("apoc_stats_distributions", "Scores", "node", "ASC", "apoc_stats_distributions_summaries")
        processor.process_mode("anomaly_stats_distributions", "Cycles", "floor", "DESC", "anomaly_stats_distributions_summaries")

        # 2. Multi-Modes
        processor.process_multi_mode("moc_stats_dual_distributions", "Cycles", "DESC", "moc_stats_dual_distributions_summaries")
        processor.process_multi_mode("pure_fiction_stats_dual_distributions", "Points", "ASC", "pure_fiction_stats_dual_distributions_summaries")
        processor.process_multi_mode("apoc_stats_dual_distributions", "Scores", "ASC", "apoc_dual_stats_distributions_summaries")
        processor.process_multi_mode("anomaly_stats_triple_distributions", "Cycles", "DESC", "anomaly_triple_stats_distributions_summaries")

    finally:
        processor.close()