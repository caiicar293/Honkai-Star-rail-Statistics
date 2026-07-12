import os
import json
import duckdb
import numpy as np
import pandas as pd
import rustworkx as rx
from dotenv import load_dotenv

try:
    import brotli
except ImportError:
    brotli = None  # compression becomes a no-op if the package isn't installed

load_dotenv()
DB_FILE = os.getenv("DB_File")


def export_network_slice(
    game_mode: str,
    at_eidolon: int,
    up_to_eidolon: int,
    is_recent: bool = False,
    db_file: str = DB_FILE,
    output_df: bool = True,
    output_dir: str = "docs/network",
    manifest_path: str = "docs/network/network_manifest.json",
    min_edge_weight_pct: float = 0.1,   # drop the bottom X% of edges by weight to reduce clutter (0 = keep all)
    compress: bool = True,
):
    """
    Computes the graph + spring layout for one (game_mode, eidolon_range,
    recency) slice and writes it out as a JSON payload matching the data
    contract expected by network_dashboard_svg.html.j2's client-side JS:

        {
          "nodes": [{"name": str, "x": float, "y": float,
                     "eigenvector": float, "weighted_degree": float,
                     "betweenness": float}, ...],
          "edges": [{"u": str, "v": str, "weight": float}, ...]
        }

    No plotting happens here — rendering is entirely the template's job.
    This function's only output is the compressed data file (plus a
    manifest entry pointing at it).
    """
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

    # ------------------------------------------------------------------
    # 2. Layout (rustworkx spring_layout) — this is the one piece of work
    #    that has to happen in Python; the template just reads x/y.
    # ------------------------------------------------------------------
    pos = rx.spring_layout(
        G,
        k=1.4,
        num_iter=400,
        weight_fn=lambda e: np.sqrt(e.get("weight", 1.0)),
    )

    node_names = [G[idx] for idx in G.node_indices()]
    idx_to_name = {idx: G[idx] for idx in G.node_indices()}

    # ------------------------------------------------------------------
    # 3. Build the JSON-serializable payload
    # ------------------------------------------------------------------
    nodes_payload = []
    for idx in G.node_indices():
        name = idx_to_name[idx]
        x, y = pos[idx]
        nodes_payload.append({
            "name": name,
            "x": float(x),
            "y": float(y),
            "eigenvector": float(eigen_map.get(name, 0.0)),
            "weighted_degree": float(degree_map.get(name, 0.0)),
            "betweenness": float(between_map.get(name, 0.0)),
        })

    edges_payload = []
    for u_idx, v_idx, edge_data in G.weighted_edge_list():
        edges_payload.append({
            "u": idx_to_name[u_idx],
            "v": idx_to_name[v_idx],
            "weight": float(edge_data.get("weight", 1.0)),
        })

    payload = {
        "game_mode": game_mode,
        "at_eidolon": at_eidolon,
        "up_to_eidolon": up_to_eidolon,
        "is_recent": is_recent,
        "nodes": nodes_payload,
        "edges": edges_payload,
    }

    # ------------------------------------------------------------------
    # 4. Write the compressed slice file + update the manifest
    # ------------------------------------------------------------------
    os.makedirs(output_dir, exist_ok=True)
    recency_tag = "recent" if is_recent else "all"
    eidolon_tag = f"e{at_eidolon}-{up_to_eidolon}"
    filename = f"{game_mode.lower()}_{recency_tag}_{eidolon_tag}.json"

    raw_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")

    if compress and brotli is not None:
        out_filename = filename + ".br"
        out_path = os.path.join(output_dir, out_filename)
        with open(out_path, "wb") as f:
            f.write(brotli.compress(raw_bytes, quality=11))
    else:
        if compress and brotli is None:
            print("⚠️ brotli package not installed — writing uncompressed JSON instead.")
        out_filename = filename
        out_path = os.path.join(output_dir, out_filename)
        with open(out_path, "wb") as f:
            f.write(raw_bytes)

    _update_manifest(
        manifest_path=manifest_path,
        game_mode=game_mode,
        eidolon_range_key=f"{at_eidolon}-{up_to_eidolon}",
        recency_tag=recency_tag,
        relative_path=os.path.join(output_dir, out_filename).replace(os.sep, "/"),
    )

    print(f"✅ Slice exported: {out_path}")
    print(f"   {len(nodes_payload)} nodes, {len(edges_payload)} edges "
          f"({'compressed' if compress and brotli else 'uncompressed'})")

    if output_df:
        print("\nTop 15 Characters by Eigenvector Centrality:")
        print(centrality_df.sort_values("Eigenvector", ascending=False).head(15).to_string(index=False))

    return centrality_df


def _update_manifest(manifest_path, game_mode, eidolon_range_key, recency_tag, relative_path):
        """
        Updated Manifest shape:
            { 
            "<GAME_MODE>": { 
                "<at>-<upto>": {
                "recent": "path/to/slice_recent.json.br",
                "all": "path/to/slice_all.json.br"
                }
            }
            }
        """
        manifest = {}
        if os.path.exists(manifest_path):
            with open(manifest_path, "r") as f:
                try:
                    manifest = json.load(f)
                except json.JSONDecodeError:
                    manifest = {}

        # 1. Ensure the game mode dictionary exists
        manifest.setdefault(game_mode, {})
        
        # 2. Ensure the eidolon range dictionary exists under that game mode
        manifest[game_mode].setdefault(eidolon_range_key, {})
        
        # 3. Save the path under the specific recency tag ("recent" or "all")
        manifest[game_mode][eidolon_range_key][recency_tag] = relative_path

        os.makedirs(os.path.dirname(manifest_path) or ".", exist_ok=True)
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)


if __name__ == "__main__":
    for game_mode in ["PURE_FICTION", "ANOMALY_F0", "ANOMALY_F4", "APOC","MOC"]:
        for is_recent in [True, False]:
            eidolons = [0,1,2,6]
            for at_eidolon in range(len(eidolons)):
                for up_to_eidolon in range(at_eidolon, len(eidolons)):
                    print(f"\nExporting {game_mode} (E{eidolons[at_eidolon]}-E{eidolons[up_to_eidolon]}) "
                          f"{'recent' if is_recent else 'all'} slice...")
                    export_network_slice(
                        game_mode=game_mode,
                        at_eidolon=eidolons[at_eidolon],
                        up_to_eidolon=eidolons[up_to_eidolon],
                        is_recent=is_recent,
                        output_df=False,
                    )
            
