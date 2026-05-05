from database_main import HonkaiDataPlatform
from database_archetypes_summary import HonkaiMetaAnalyzer
from database_distributions_summary import StarRailStatsProcessor
from database_teams_summary import HonkaiTeamMetaAnalyzer
from database_duos_summary import HonkaiDuosSummaryAnalyzer


platform = HonkaiDataPlatform()
platform.orchestrate_update()

analyzer = HonkaiTeamMetaAnalyzer()
analyzer.run_analysis()


analyzer1 = HonkaiMetaAnalyzer()
analyzer1.run_analysis()

analyzer2 = HonkaiDuosSummaryAnalyzer()
analyzer2.run_analysis()



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
    
    
import duckdb
import os
from dotenv import load_dotenv

load_dotenv()

# Connect to your database
con = duckdb.connect(os.getenv("DB_File"))

# 1. Get a list of all tables ending in 'gear_usage'
tables_to_fix = con.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_name LIKE '%gear_usage'
""").fetchall()

# 2. Loop through each table and apply the regex update
for (table_name,) in tables_to_fix:
    print(f"Cleaning Eidolon strings in table: {table_name}...")
    
    # We target 'column_name'—ensure this matches your actual column name (e.g., 'eidolon_level' or 'Eidolon')
    # If the column name varies, you'd need to fetch that from information_schema.columns too.
    update_query = f"""
    UPDATE {table_name}
    SET Eidolon = regexp_replace(
        Eidolon, 
        '(Eidolon\s+\d+)\.0', 
        '\\1', 
        'g'
    )
    WHERE Eidolon LIKE '%Eidolon%';
    """
    
    try:
        con.execute(update_query)
        print(f"Successfully updated {table_name}.")
    except Exception as e:
        print(f"Could not update {table_name}: {e}")

con.close()