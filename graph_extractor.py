import os
import json
import duckdb
import numpy as np
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
    html_output: str = "network_dashboard.html",
    top_label_pct: float = 0.75,   # label the top (1 - pct) fraction of nodes by default metric
    min_edge_weight_pct: float = 0.0,  # drop the bottom X% of edges by weight to reduce clutter (0 = keep all)
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

    # ------------------------------------------------------------------
    # 1. Rebuild the graph from the dict of dicts
    # ------------------------------------------------------------------
    raw_dict = json.loads(json_res[0])
    G = rx.PyGraph()
    node_map = {}
    added_edges = set()

    for u, neighbors in raw_dict.items():
        if u not in node_map:
            node_map[u] = G.add_node(u)
        u_idx = node_map[u]

        for v, edge_data in neighbors.items():
            if v not in node_map:
                node_map[v] = G.add_node(v)
            v_idx = node_map[v]

            edge_tuple = tuple(sorted((u_idx, v_idx)))
            if edge_tuple not in added_edges:
                G.add_edge(u_idx, v_idx, edge_data)
                added_edges.add(edge_tuple)

    # Optionally prune the weakest edges so the layout isn't a hairball
    if min_edge_weight_pct > 0:
        weights = [e.get("weight", 1.0) for *_, e in G.weighted_edge_list()]
        if weights:
            cutoff = np.percentile(weights, min_edge_weight_pct * 100)
            for u_idx, v_idx, e in list(G.weighted_edge_list()):
                if e.get("weight", 1.0) < cutoff:
                    edge_idx = G.get_edge_data(u_idx, v_idx)
                    try:
                        G.remove_edge(u_idx, v_idx)
                    except Exception:
                        pass

    # ------------------------------------------------------------------
    # Metric maps
    # ------------------------------------------------------------------
    eigen_map = dict(zip(centrality_df["Character"], centrality_df["Eigenvector"]))
    degree_map = dict(zip(centrality_df["Character"], centrality_df["Weighted_Degree"]))
    between_map = dict(zip(centrality_df["Character"], centrality_df["Betweenness"]))

    metric_maps = {
        "Eigenvector": eigen_map,
        "Weighted_Degree": degree_map,
        "Betweenness": between_map,
    }
    metric_labels = {
        "Eigenvector": "Meta Value (Eigenvector)",
        "Weighted_Degree": "Synergy Volume (Weighted Degree)",
        "Betweenness": "Versatility (Betweenness)",
    }

    # ------------------------------------------------------------------
    # 2. Layout (rustworkx spring_layout)
    # ------------------------------------------------------------------
    pos = rx.spring_layout(
        G,
        k=0.45,
        num_iter=120,
        weight_fn=lambda e: e.get("weight", 1.0),
    )

    node_names = [G[idx] for idx in G.node_indices()]
    x_nodes = [pos[idx][0] for idx in G.node_indices()]
    y_nodes = [pos[idx][1] for idx in G.node_indices()]

    # ------------------------------------------------------------------
    # 3. Edges — bucket by weight into a few bands so thickness/opacity
    #    actually communicates synergy strength (a single trace can't
    #    vary width per-segment in Plotly).
    # ------------------------------------------------------------------
    edge_records = []
    for u_idx, v_idx, edge_data in G.weighted_edge_list():
        w = edge_data.get("weight", 1.0)
        edge_records.append((u_idx, v_idx, w))

    edge_traces = []
    if edge_records:
        weights_arr = np.array([w for *_, w in edge_records])
        # 4 strength bands via quartiles (guard against degenerate/constant weights)
        try:
            q = np.quantile(weights_arr, [0.25, 0.5, 0.75])
        except Exception:
            q = [weights_arr.min()] * 3

        bands = [
            ("Weak", lambda w: w <= q[0], 0.6, "rgba(140,140,150,0.15)"),
            ("Light", lambda w: q[0] < w <= q[1], 1.0, "rgba(150,150,160,0.25)"),
            ("Moderate", lambda w: q[1] < w <= q[2], 1.8, "rgba(160,170,200,0.4)"),
            ("Strong", lambda w: w > q[2], 3.0, "rgba(180,200,255,0.65)"),
        ]

        for band_name, pred, width, color in bands:
            ex, ey = [], []
            for u_idx, v_idx, w in edge_records:
                if pred(w):
                    x0, y0 = pos[u_idx]
                    x1, y1 = pos[v_idx]
                    ex += [x0, x1, None]
                    ey += [y0, y1, None]
            if ex:
                edge_traces.append(
                    go.Scatter(
                        x=ex,
                        y=ey,
                        mode="lines",
                        line=dict(width=width, color=color),
                        hoverinfo="none",
                        name=band_name,
                        showlegend=False,
                    )
                )

    # ------------------------------------------------------------------
    # 4. Node trace(s) — build one trace per metric so a dropdown can
    #    toggle which metric drives size/color, instead of baking in
    #    a single fixed encoding.
    # ------------------------------------------------------------------
    def build_node_trace(metric_key, visible):
        m = metric_maps[metric_key]
        vals = [m.get(name, 0.0) for name in node_names]
        max_val = max(vals) if vals else 1.0
        max_val = max_val if max_val > 0 else 1.0

        sizes = [12 + (v / max_val) * 48 for v in vals]

        threshold = pd.Series(vals).quantile(top_label_pct) if vals else 0
        texts = [name if v >= threshold else "" for name, v in zip(node_names, vals)]

        # Rank for hover context
        order = pd.Series(vals).rank(ascending=False, method="min").astype(int)

        hover_texts = []
        for i, name in enumerate(node_names):
            e = eigen_map.get(name, 0)
            d = degree_map.get(name, 0)
            b = between_map.get(name, 0)
            hover_texts.append(
                f"<b>{name}</b><br>"
                f"Rank ({metric_labels[metric_key]}): #{order[i]}<br>"
                "―――――――――――――――<br>"
                f"Eigenvector (Meta Value): {e:.4f}<br>"
                f"Weighted Degree (Synergies): {d:.1f}<br>"
                f"Betweenness (Versatility): {b:.4f}"
            )

        # Gold outline for the top 3 nodes on the active metric
        top3 = set(pd.Series(vals, index=node_names).sort_values(ascending=False).head(3).index)
        line_colors = ["#FFD700" if name in top3 else "rgba(0,0,0,0.8)" for name in node_names]
        line_widths = [2.5 if name in top3 else 1.2 for name in node_names]

        return go.Scatter(
            x=x_nodes,
            y=y_nodes,
            mode="markers+text",
            text=texts,
            textposition="top center",
            textfont=dict(size=11, color="white", family="Arial Black"),
            hovertext=hover_texts,
            hoverinfo="text",
            visible=visible,
            marker=dict(
                size=sizes,
                color=vals,
                colorscale="Plasma",
                showscale=True,
                colorbar=dict(title=metric_labels[metric_key], thickness=15, outlinewidth=0, x=1.02),
                line=dict(width=line_widths, color=line_colors),
            ),
            name=metric_key,
            showlegend=False,
        )

    metric_keys = list(metric_maps.keys())
    node_traces = [
        build_node_trace(key, visible=(i == 0)) for i, key in enumerate(metric_keys)
    ]

    # ------------------------------------------------------------------
    # 5. Assemble figure
    # ------------------------------------------------------------------
    fig = go.Figure(data=edge_traces + node_traces)
    meta_tag = "Recent Meta" if is_recent else "Historical Meta"
    n_nodes = len(node_names)
    n_edges = len(edge_records)

    # Dropdown to switch the metric driving node size/color.
    # Edge traces (fixed count at the front) stay visible; only the
    # node traces toggle.
    n_edge_traces = len(edge_traces)
    buttons = []
    for i, key in enumerate(metric_keys):
        visible_mask = [True] * n_edge_traces + [j == i for j in range(len(metric_keys))]
        buttons.append(
            dict(
                label=metric_labels[key],
                method="update",
                args=[{"visible": visible_mask}],
            )
        )

    fig.update_layout(
        title=dict(
            text=(
                f"<b>HSR Team Network — {game_mode}</b>"
                f"<br><sup>E{at_eidolon}–E{up_to_eidolon} · {meta_tag} · "
                f"{n_nodes} characters · {n_edges} synergy links</sup>"
            ),
            x=0.5,
            font=dict(size=22),
        ),
        showlegend=False,
        hovermode="closest",
        margin=dict(l=20, r=20, t=90, b=20),
        paper_bgcolor="#121212",
        plot_bgcolor="#121212",
        font=dict(color="#E0E0E0"),
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        updatemenus=[
            dict(
                buttons=buttons,
                direction="down",
                showactive=True,
                x=0.0,
                xanchor="left",
                y=1.12,
                yanchor="top",
                bgcolor="#1E1E1E",
                bordercolor="#444",
                font=dict(color="#E0E0E0", size=12),
            )
        ],
        annotations=[
            dict(
                text="Color by:",
                x=0.0,
                xref="paper",
                y=1.17,
                yref="paper",
                showarrow=False,
                font=dict(size=12, color="#AAAAAA"),
                xanchor="left",
            ),
            dict(
                text=(
                    "Node size & color = selected metric · Gold border = top 3 · "
                    "Edge thickness = synergy strength"
                ),
                x=0.5,
                xref="paper",
                y=-0.06,
                yref="paper",
                showarrow=False,
                font=dict(size=11, color="#888888"),
            ),
        ],
    )

    config = {
        "displayModeBar": True,
        "scrollZoom": True,
        "displaylogo": False,
        "modeBarButtonsToRemove": ["lasso2d", "select2d"],
    }

    fig.write_html(html_output, include_plotlyjs='cdn', config=config)
    print(f"✅ Dashboard saved to: {html_output}")

    if output_df:
        print("\nTop 15 Characters by Eigenvector Centrality:")
        print(centrality_df.sort_values("Eigenvector", ascending=False).head(15).to_string(index=False))

    return centrality_df


if __name__ == "__main__":
    df = plot_network_slice(
        game_mode="APOC",
        at_eidolon=0,
        up_to_eidolon=6,
        is_recent=True,
        output_df=True,
        html_output="apoc_network_dashboard.html",
    )