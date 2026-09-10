from database_batch import HonkaiDataPlatform
from database_archetypes_summary import HonkaiMetaAnalyzer
from database_distributions_summary import StarRailStatsProcessor
from database_teams_summary import HonkaiTeamMetaAnalyzer
from database_duos_summary import HonkaiDuosSummaryAnalyzer
from database_by_cost_teams_summary import HonkaiCostArchetypeMetaAnalyzer, HonkaiCostCharacterMetaAnalyzer, HonkaiCostTeamMetaAnalyzer
from database_character_summary import CharacterMetaAnalyzer
from database_network_graph import build_network_tables ,build_raw_duo_network_tables
from database_gear_eidolon_summary import HonkaiGearEidolonSummaryAnalyzer
platform = HonkaiDataPlatform()
platform.orchestrate_update(modern_strategy="per_version", legacy_strategy="all_at_once")

analyzer = HonkaiTeamMetaAnalyzer()
analyzer.run_analysis()

analyzer3 = CharacterMetaAnalyzer()
analyzer3.run_analysis()

analyzer1 = HonkaiMetaAnalyzer()
analyzer1.run_analysis()

analyzer2 = HonkaiDuosSummaryAnalyzer()
analyzer2.run_analysis()

anazlzer3 = HonkaiCostTeamMetaAnalyzer()
anazlzer3.run_analysis()


analyzer4 = HonkaiCostArchetypeMetaAnalyzer()
analyzer4.run_analysis()



analyzer5 = HonkaiCostCharacterMetaAnalyzer()
analyzer5.run_analysis()
processor = StarRailStatsProcessor()

# --- Historical meta ---
df_meta, meta_graphs = build_network_tables(
    source_table="duos_meta_summary",
    dest_table="network_centrality",
)

# --- Recent meta ---
df_recent, recent_graphs = build_network_tables(
    source_table="duos_recent_meta_summary",
    dest_table="network_centrality_recent",
)

# --- Raw per-mode _duos tables (version + node granularity) ---
df_raw, raw_graphs = build_raw_duo_network_tables(
    dest_table="duos_raw_network_centrality",
)

print("\n=== network_centrality (head) ===")
print(df_meta.head(10))

print("\n=== network_centrality_graphs (head) ===")
print(meta_graphs.head(5))

print("\n=== duos_raw_network_centrality (head) ===")
print(df_raw.head(10))

print("\n=== duos_raw_network_centrality_graphs (head) ===")
print(raw_graphs.head(5))
try:
    # 1. Standard Modes
        processor.process_mode("moc_stats_distributions", "Scores", "node", "DESC", "moc_stats_distributions_summaries")
        processor.process_mode("pure_fiction_stats_distributions", "Scores", "node", "ASC", "pure_fiction_stats_distributions_summaries")
        processor.process_mode("apoc_stats_distributions", "Scores", "node", "ASC", "apoc_stats_distributions_summaries")
        processor.process_mode("anomaly_stats_distributions", "Scores", "floor", "DESC", "anomaly_stats_distributions_summaries")

        # 2. Multi-Modes
        processor.process_multi_mode("moc_stats_dual_or_triple_distributions", "Scores", "DESC", "moc_stats_dual_or_triple_distributions_summaries")
        processor.process_multi_mode("pure_fiction_stats_dual_or_triple_distributions", "Scores", "ASC", "pure_fiction_stats_dual_or_triple_distributions_summaries")
        processor.process_multi_mode("apoc_stats_dual_or_triple_distributions", "Scores", "ASC", "apoc_dual_or_triple_stats_distributions_summaries")
        processor.process_multi_mode("anomaly_stats_triple_distributions", "Scores", "DESC", "anomaly_triple_stats_distributions_summaries")

finally:
    processor.close()



analyzer3 = HonkaiGearEidolonSummaryAnalyzer()
analyzer3.run_analysis()