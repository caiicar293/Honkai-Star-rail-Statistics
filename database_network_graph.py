"""
network_graph.py
----------------
Builds a character co-usage network from duos_meta_summary and
duos_recent_meta_summary, computes centrality metrics for every
(Game_Mode, at_eidolon_level, up_to_eidolon_level) slice, and
writes the results back to the DuckDB file as two tables:

    network_centrality        -- from duos_meta_summary
    network_centrality_recent  -- from duos_recent_meta_summary

*NEW: Also stores the raw network adjacency structures as JSON 
      in network_centrality_graphs and network_centrality_recent_graphs.
"""

import os
import json
import duckdb
import polars as pl
import networkx as nx
from dotenv import load_dotenv

load_dotenv()
DB_FILE = os.getenv("DB_File")


# ---------------------------------------------------------------------------
# Core: build graph + compute centrality for one slice of duo data
# ---------------------------------------------------------------------------

def _compute_centrality(slice_df: pl.DataFrame, weight_option: str = "Total_Samples"):
    """
    Given a filtered duo Polars DataFrame (one Game_Mode / eidolon slice),
    build a weighted NetworkX graph and return:
      1. A centrality Polars DataFrame
      2. The raw JSON string of the graph's dictionary structure
    """
    slice_df = slice_df.fill_null(0)

    G = nx.Graph()
    weight_col = weight_option if weight_option in slice_df.columns else "Total_Samples"

    # Fast iteration over Polars rows
    for row in slice_df.select(["Antecedent", "Consequent", weight_col]).iter_rows(named=True):
        a = row["Antecedent"]
        b = row["Consequent"]
        w = float(row[weight_col])
        dist = 1.0 / w if w > 0 else 1e9
        G.add_edge(a, b, weight=w, distance=dist)

    if G.number_of_nodes() == 0:
        return pl.DataFrame(), "{}"

    # Extract the nested dictionary representation and serialize to JSON
    # Format: {"Char_A": {"Char_B": {"weight": 10.0, "distance": 0.1}}}
    graph_dict = nx.to_dict_of_dicts(G)
    graph_json = json.dumps(graph_dict)

    # Graph structural properties
    weighted_degree = dict(G.degree(weight="weight"))
    degree          = dict(G.degree())

    try:
        eigen = nx.eigenvector_centrality(G, weight="weight", max_iter=1000)
    except nx.PowerIterationFailedConvergence:
        eigen = {n: 0.0 for n in G.nodes()}

    betweenness = nx.betweenness_centrality(G, weight="distance")
    closeness   = nx.closeness_centrality(G, distance="distance")

    nodes = list(G.nodes())
    result = pl.DataFrame({
        "Character":       nodes,
        "Weighted_Degree": [weighted_degree[n] for n in nodes],
        "Degree":          [degree[n]          for n in nodes],
        "Eigenvector":     [eigen[n]           for n in nodes],
        "Betweenness":     [betweenness[n]     for n in nodes],
        "Closeness":       [closeness[n]       for n in nodes],
    })

    return result.sort("Weighted_Degree", descending=True), graph_json


# ---------------------------------------------------------------------------
# Main: iterate every (Game_Mode, at_eidolon, up_to_eidolon) slice
# ---------------------------------------------------------------------------

def build_network_tables(
    db_file: str       = DB_FILE,
    weight_option: str = "Total_Samples",
    source_table: str  = "duos_meta_summary",
    dest_table: str    = "network_centrality",
):
    """
    Computes topology metrics and extracts the raw JSON graphs, bulk inserting
    both into separate DuckDB tables.
    """
    conn = duckdb.connect(db_file)

    df_all = pl.from_arrow(conn.execute(f"SELECT * FROM {source_table}").arrow())

    slices = (
        df_all.select(["Game_Mode", "at_eidolon_level", "up_to_eidolon_level"])
        .unique()
        .sort(["Game_Mode", "up_to_eidolon_level", "at_eidolon_level"])
    )

    print(f"[{source_table}] Found {len(slices)} slices using Polars — computing metrics & extracting graphs...")

    all_results = []
    all_graphs = []  # List to store our raw JSON dictionary objects

    for row in slices.iter_rows(named=True):
        gm   = row["Game_Mode"]
        at   = row["at_eidolon_level"]
        upto = row["up_to_eidolon_level"]

        slice_df = df_all.filter(
            (pl.col("Game_Mode") == gm) &
            (pl.col("at_eidolon_level") == at) &
            (pl.col("up_to_eidolon_level") == upto)
        )

        centrality, graph_json = _compute_centrality(slice_df, weight_option)
        if centrality.is_empty():
            continue

        # 1. Format the metrics row
        centrality = centrality.with_columns([
            pl.lit(gm).alias("Game_Mode"),
            pl.lit(int(at)).alias("at_eidolon_level"),
            pl.lit(int(upto)).alias("up_to_eidolon_level")
        ])
        
        centrality = centrality.select([
            "Game_Mode", "at_eidolon_level", "up_to_eidolon_level",
            "Character", "Weighted_Degree", "Degree", 
            "Eigenvector", "Betweenness", "Closeness"
        ])

        all_results.append(centrality)

        # 2. Format the graph storage row
        all_graphs.append({
            "Game_Mode": gm,
            "at_eidolon_level": int(at),
            "up_to_eidolon_level": int(upto),
            "graph_json": graph_json
        })

    if not all_results:
        print("No results generated — check source table.")
        conn.close()
        return pl.DataFrame(), pl.DataFrame()

    # --- Write Centrality Metrics ---
    final_df = pl.concat(all_results)
    conn.execute(f"DROP TABLE IF EXISTS {dest_table}")
    conn.execute(f"CREATE TABLE {dest_table} AS SELECT * FROM final_df")
    print(f"  → Written {len(final_df):,} metrics rows to '{dest_table}'")

    # --- Write Raw Network Graphs ---
    graphs_table = f"{dest_table}_graphs"
    graphs_df = pl.DataFrame(all_graphs)
    
    conn.execute(f"DROP TABLE IF EXISTS {graphs_table}")
    # We cast the string column to DuckDB's native JSON type here for blazing fast SQL querying later
    conn.execute(f"""
        CREATE TABLE {graphs_table} AS 
        SELECT 
            Game_Mode, 
            at_eidolon_level, 
            up_to_eidolon_level, 
            CAST(graph_json AS JSON) AS graph_data 
        FROM graphs_df
    """)
    print(f"  → Written {len(graphs_df):,} network JSON graphs to '{graphs_table}'")
    
    conn.close()
    return final_df, graphs_df


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
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

    print("\n=== network_centrality (head) ===")
    print(df_meta.head(10))

    print("\n=== network_centrality_graphs (head) ===")
    print(meta_graphs.head(5))