"""
generate_archetype_dashboard.py
--------------------------------
Generates a self-contained archetype stats HTML dashboard for any supported game mode.

Supported modes:
    moc       — Memory of Chaos       (node 0/1/2)
    apoc      — Apocalyptic Shadow    (node 0/1/2)
    pf        — Pure Fiction          (node 0/1/2)
    anomaly   — Anomaly Arbitration   (floor 0/1/2/3/4, no node)

Usage:
    python generate_archetype_dashboard.py --mode moc    --version 4.2.3
    python generate_archetype_dashboard.py --mode anomaly --version 4.2.3
    python generate_archetype_dashboard.py --mode apoc   --version 4.2.3 --output ./Dashboards
    python generate_archetype_dashboard.py --mode pf     --version 4.2.3 --db path/to/hsr.duckdb

Dependencies:
    pip install duckdb jinja2
"""

import argparse
import json
import math
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

import duckdb
from jinja2 import Environment, FileSystemLoader, select_autoescape

# ---------------------------------------------------------------------------
# Mode configuration
# ---------------------------------------------------------------------------

MODE_CONFIG = {
    "moc": {
        "table":       "moc_stats_archetypes",
        "mode_label":  "MOC",
        "full_name":   "Memory of Chaos",
        "subtitle":    "FLOOR 12",
        "dim_field":   "node",
        "dim_label":   "Node",
        "dim_values":  [0, 1, 2],
        "dim_group_labels": {
            0: "NODE 0 — Both Halves",
            1: "NODE 1 — First Half",
            2: "NODE 2 — Second Half",
        },
        "dim_btn_label": lambda d: f"N{d}",
    },
    "apoc": {
        "table":       "apoc_stats_archetypes",
        "mode_label":  "APOC",
        "full_name":   "Apocalyptic Shadow",
        "subtitle":    "FLOOR 4",
        "dim_field":   "node",
        "dim_label":   "Node",
        "dim_values":  [0, 1, 2],
        "dim_group_labels": {
            0: "NODE 0 — Both Halves",
            1: "NODE 1 — First Half",
            2: "NODE 2 — Second Half",
        },
        "dim_btn_label": lambda d: f"N{d}",
    },
    "pf": {
        "table":       "pure_fiction_stats_archetypes",
        "mode_label":  "Pure Fiction",
        "full_name":   "Pure Fiction",
        "subtitle":    "FLOOR 4",
        "dim_field":   "node",
        "dim_label":   "Node",
        "dim_values":  [0, 1, 2],
        "dim_group_labels": {
            0: "NODE 0 — Both Halves",
            1: "NODE 1 — First Half",
            2: "NODE 2 — Second Half",
        },
        "dim_btn_label": lambda d: f"N{d}",
    },
    "anomaly": {
        "table":       "anomaly_stats_archetypes",
        "mode_label":  "Anomaly",
        "full_name":   "Anomaly Arbitration",
        "subtitle":    "FLOORS 0–4",
        "dim_field":   "floor",
        "dim_label":   "Floor",
        "dim_values":  [0, 1, 2, 3, 4],
        "dim_group_labels": {
            0: "FLOOR 0",
            1: "FLOOR 1",
            2: "FLOOR 2",
            3: "FLOOR 3",
            4: "FLOOR 4",
        },
        "dim_btn_label": lambda d: f"F{d}",
    },
}

TEMPLATE_NAME = "archetypes_template.html.j2"
DEFAULT_DB_PATH    = os.getenv("DB_File")
DEFAULT_OUTPUT_DIR = "./Other Dashboards"
DEFAULT_ICONS_PATH = "character_icons.json"


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

def fetch_rows(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    version: str,
    dim_field: str,
    dim_values: list[int],
) -> list[dict]:
    placeholders = ", ".join(["?" for _ in dim_values])
    sql = f"""
        SELECT
            Rank,
            version,
            at_eidolon_level,
            up_to_eidolon_level,
            {dim_field},
            Archetype_Core,
            Usage_pct,
            Samples,
            Sustain_Percentage,
            Sustain_Samples,
            Min_Score,
            Percentile_25,
            Median_Score,
            Percentile_75,
            Average_Score,
            Max_Score,
            Std_Dev
        FROM {table}
        WHERE version = ?
          AND {dim_field} IN ({placeholders})
        ORDER BY {dim_field}, at_eidolon_level, up_to_eidolon_level, Rank
    """
    params = [version] + dim_values
    rel    = conn.execute(sql, params)
    cols   = [desc[0] for desc in rel.description]
    rows   = []
    for raw in rel.fetchall():
        row = dict(zip(cols, raw))
        for k, v in row.items():
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                row[k] = None
        rows.append(row)
    return rows


def load_icons(icons_path: Path) -> dict:
    if not icons_path.exists():
        print(f"[WARN] Icons file not found at {icons_path} — icons will be skipped.")
        return {}
    with open(icons_path, encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_version(conn: duckdb.DuckDBPyConnection, table: str, version: str) -> None:
    result = conn.execute(
        f"SELECT COUNT(*) FROM {table} WHERE version = ?", [version]
    ).fetchone()
    if result[0] == 0:
        available = conn.execute(
            f"SELECT DISTINCT version FROM {table} ORDER BY version DESC LIMIT 20"
        ).fetchall()
        versions = [r[0] for r in available]
        print(f"[ERROR] Version '{version}' not found in {table}.")
        print(f"        Available versions: {versions}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Render
# ---------------------------------------------------------------------------

def render_dashboard(
    cfg: dict,
    version: str,
    rows: list[dict],
    icons: dict,
    template_dir: Path,
    output_dir: Path,
) -> Path:
    # Jinja2 doesn't support calling lambdas from templates directly,
    # so we pass a global function and pre-built label list instead.
    def dim_btn_label(d: int) -> str:
        return cfg["dim_btn_label"](d)

    env = Environment(
        loader=FileSystemLoader(str(template_dir)),
        autoescape=select_autoescape(disabled_extensions=["j2", "html"]),
    )
    env.globals["dim_btn_label"] = dim_btn_label

    template = env.get_template(TEMPLATE_NAME)

    html = template.render(
        version=version,
        mode_label=cfg["mode_label"],
        subtitle=cfg["subtitle"],
        dim_field=cfg["dim_field"],
        dim_label=cfg["dim_label"],
        dim_values=cfg["dim_values"],
        dim_group_labels_json=json.dumps(cfg["dim_group_labels"]),
        data_json=json.dumps(rows, ensure_ascii=False),
        icons_json=json.dumps(icons, ensure_ascii=False),
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    safe_version = version.replace(".", "_")
    out_file = output_dir / f"{cfg['mode_label'].lower().replace(' ', '_')}_{safe_version}_archetypes.html"
    out_file.write_text(html, encoding="utf-8")
    return out_file


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate an archetype stats HTML dashboard from DuckDB.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--mode", "-m",
        required=True,
        choices=list(MODE_CONFIG.keys()),
        help="Game mode: moc | apoc | pf | anomaly",
    )
    parser.add_argument(
        "--version", "-v",
        required=True,
        help="Game version, e.g. 4.2.3",
    )
    parser.add_argument(
        "--db",
        default=DEFAULT_DB_PATH,
        help="Path to the DuckDB database file.",
    )
    parser.add_argument(
        "--icons",
        default=DEFAULT_ICONS_PATH,
        help="Path to character_icons.json.",
    )
    parser.add_argument(
        "--output", "-o",
        default=DEFAULT_OUTPUT_DIR,
        help="Output directory for the generated HTML file.",
    )
    parser.add_argument(
        "--template-dir",
        default=str(Path(__file__).parent),
        help="Directory containing the Jinja2 template.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg  = MODE_CONFIG[args.mode]

    db_path      = Path(args.db)
    icons_path   = Path(args.icons)
    output_dir   = Path(args.output)
    template_dir = Path(args.template_dir)

    if not db_path.exists():
        print(f"[ERROR] Database not found: {db_path}")
        sys.exit(1)
    if not (template_dir / TEMPLATE_NAME).exists():
        print(f"[ERROR] Template not found: {template_dir / TEMPLATE_NAME}")
        sys.exit(1)

    print(f"[INFO] Mode: {cfg['full_name']} ({args.mode})")
    print(f"[INFO] Connecting to {db_path} ...")
    conn = duckdb.connect(str(db_path), read_only=True)

    print(f"[INFO] Validating version '{args.version}' in {cfg['table']} ...")
    validate_version(conn, cfg["table"], args.version)

    print(f"[INFO] Fetching rows ...")
    rows = fetch_rows(conn, cfg["table"], args.version, cfg["dim_field"], cfg["dim_values"])
    conn.close()
    print(f"[INFO] {len(rows)} rows fetched.")

    icons = load_icons(icons_path)
    print(f"[INFO] {len(icons)} icons loaded.")

    print(f"[INFO] Rendering dashboard ...")
    out_file = render_dashboard(cfg, args.version, rows, icons, template_dir, output_dir)

    size_kb = out_file.stat().st_size / 1024
    print(f"[DONE] {out_file}  ({size_kb:.0f} KB)")


if __name__ == "__main__":
    main()
