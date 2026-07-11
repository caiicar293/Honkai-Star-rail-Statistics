import os
import json
import duckdb
import pandas as pd
import rustworkx as rx
import plotly.graph_objects as go
from dotenv import load_dotenv

load_dotenv()
DB_FILE = os.getenv("DB_File")

def plot_network_slice(
    game_mode: str, 
    at_eidolon: int, 
    up_to_eidolon: int, 
    is_recent: bool = False,
    db_file: str = DB_FILE,
    output_df: bool = True,
    html_output: str = "network_dashboard.html"
):
    conn = duckdb.connect(db_file, read_only=True)
    
    metrics_table = "network_centrality_recent" if is_recent else "network_centrality"
    graphs_table = f"{metrics_table}_graphs"

    json_query = f"""
        SELECT CAST(graph_data AS VARCHAR) 
        FROM {graphs_table} 
        WHERE Game_Mode = ? AND at_eidolon_level = ? AND up_to_eidolon_level = ?
    """
    json_res = conn.execute(json_query, [game_mode, at_eidolon, up_to_eidolon]).fetchone()

    metrics_query = f"""
        SELECT Character, Eigenvector, Weighted_Degree, Betweenness
        FROM {metrics_table}
        WHERE Game_Mode = ? AND at_eidolon_level = ? AND up_to_eidolon_level = ?
    """
    centrality_df = conn.execute(metrics_query, [game_mode, at_eidolon, up_to_eidolon]).df()
    
    conn.close()

    if not json_res or centrality_df.empty:
        print(f"⚠️ No data found for {game_mode} (E{at_eidolon}-E{up_to_eidolon}).")
        return pd.DataFrame()

    # 1. Rebuild the graph from the dict of dicts
    raw_dict = json.loads(json_res[0])
    G = rx.PyGraph()
    node_map = {}
    
    # Track which undirected edges we've already added so we don't duplicate them
    added_edges = set() 
    
    for u, neighbors in raw_dict.items():
        if u not in node_map:
            node_map[u] = G.add_node(u)
        
        u_idx = node_map[u]
        
        for v, edge_data in neighbors.items():
            if v not in node_map:
                node_map[v] = G.add_node(v)
            
            v_idx = node_map[v]
            
            # Since the graph is undirected, 'u->v' and 'v->u' are the same edge
            edge_tuple = tuple(sorted((u_idx, v_idx)))
            if edge_tuple not in added_edges:
                G.add_edge(u_idx, v_idx, edge_data)
                added_edges.add(edge_tuple)

    # Mapping metrics for rich hover data
    eigen_map = dict(zip(centrality_df['Character'], centrality_df['Eigenvector']))
    degree_map = dict(zip(centrality_df['Character'], centrality_df['Weighted_Degree']))
    between_map = dict(zip(centrality_df['Character'], centrality_df['Betweenness']))

    # 2. Layout tuning (using rustworkx spring_layout)
    # k regulates the distance. num_iter replaces NetworkX's iterations.
    pos = rx.spring_layout(
        G, 
        k=0.45, 
        num_iter=80, 
        weight_fn=lambda e: e.get('weight', 1.0)
    )

    # Normalize node sizes safely
    max_eigen = max(eigen_map.values()) if eigen_map.values() else 1
    
    # Cache node names and calculate sizes/texts using the indices
    node_names = [G[idx] for idx in G.node_indices()]
    node_sizes = [15 + (eigen_map.get(name, 0.0) / max_eigen) * 55 for name in node_names]

    # Smart Labels: Only show text for the top 25% of characters to prevent clutter
    threshold = pd.Series(list(eigen_map.values())).quantile(0.75) if not centrality_df.empty else 0
    node_texts = [name if eigen_map.get(name, 0) >= threshold else "" for name in node_names]

    # Build rich hover strings
    hover_texts = []
    for name in node_names:
        e = eigen_map.get(name, 0)
        d = degree_map.get(name, 0)
        b = between_map.get(name, 0)
        hover_texts.append(
            f"<b>{name}</b><br>"
            f"Eigenvector (Meta Value): {e:.4f}<br>"
            f"Weighted Degree (Synergies): {d:.1f}<br>"
            f"Betweenness (Versatility): {b:.4f}"
        )

    # Node positions (pos maps from node_index to [x, y])
    x_nodes = [pos[idx][0] for idx in G.node_indices()]
    y_nodes = [pos[idx][1] for idx in G.node_indices()]

    # 3. Edge positions
    edge_x, edge_y = [], []
    for u_idx, v_idx, edge_data in G.weighted_edge_list():
        x0, y0 = pos[u_idx]
        x1, y1 = pos[v_idx]
        edge_x += [x0, x1, None]
        edge_y += [y0, y1, None]

    edge_trace = go.Scatter(
        x=edge_x,
        y=edge_y,
        mode="lines",
        line=dict(width=0.8, color="rgba(150, 150, 150, 0.2)"), # Made edges softer to emphasize nodes
        hoverinfo="none"
    )

    node_trace = go.Scatter(
        x=x_nodes,
        y=y_nodes,
        mode="markers+text",
        text=node_texts, # Uses smart labels
        textposition="top center",
        hovertext=hover_texts, # Rich HTML hover
        hoverinfo="text",
        textfont=dict(size=11, color="white", family="Arial Black"),
        marker=dict(
            size=node_sizes,
            color=[eigen_map.get(name, 0) for name in node_names],
            colorscale="Plasma", # Better contrast for dark mode
            showscale=True,
            colorbar=dict(title="Meta Relevance", thickness=15, outlinewidth=0),
            line=dict(width=1.5, color="rgba(0,0,0,0.8)")
        )
    )

    fig = go.Figure(data=[edge_trace, node_trace])
    meta_tag = "Recent Meta" if is_recent else "Historical Meta"

    fig.update_layout(
        title=f"<b>HSR Team Network</b><br><sup>{game_mode} (E{at_eidolon}-E{up_to_eidolon}) [{meta_tag}]</sup>",
        title_x=0.5,
        title_font=dict(size=20),
        showlegend=False,
        hovermode="closest",
        margin=dict(l=20, r=20, t=60, b=20),
        paper_bgcolor="#121212", # Slightly deeper dark mode
        plot_bgcolor="#121212",
        font=dict(color="#E0E0E0"),
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
    )

    fig.write_html(html_output)
    print(f"✅ Dashboard saved to: {html_output}")

    if output_df:
        print("\nTop 15 Characters by Eigenvector Centrality:")
        print(centrality_df.sort_values("Eigenvector", ascending=False).head(15).to_string(index=False))

    return centrality_df

if __name__ == "__main__":
    df = plot_network_slice(
        game_mode="MOC",
        at_eidolon=0,
        up_to_eidolon=6,
        is_recent=False,
        output_df=True,
        html_output="moc_network_dashboard.html"
    )