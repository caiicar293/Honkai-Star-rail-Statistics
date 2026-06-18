"""
generate_character_dashboard.py
---------------------------------
Generates a self-contained character stats HTML dashboard from the unified
character_stats table for any supported game mode.

All modes share the same table. Mode controls which dim field (node/floor),
which dim values, whether eidolon distribution columns are shown, and
whether the eidolon range filter is rendered (legacy modes skip it).

Supported modes:
    moc              — Memory of Chaos          (node 0/1/2,  floor 12)
    apoc             — Apocalyptic Shadow       (node 0/1/2,  floor 4)
    pf               — Pure Fiction             (node 0/1/2,  floor 4)
    anomaly          — Anomaly Arbitration      (floor 0-4,   node NULL)
    moc_legacy       — MOC Legacy               (node 0/1/2,  floor 10, no eid dist)
    moc_late_legacy  — MOC Late Legacy          (node 0/1/2,  floor 12, no eid dist)
    pf_legacy        — Pure Fiction Legacy      (node 0/1/2,  floor 4,  no eid dist)

Usage:
    python generate_character_dashboard.py --mode moc           --version 4.2.3
    python generate_character_dashboard.py --mode anomaly       --version 4.2.3
    python generate_character_dashboard.py --mode moc_legacy    --version 3.0.0
    python generate_character_dashboard.py --mode apoc          --version 4.2.3 --output ./Dashboards

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
        "db_mode":      "MOC",
        "mode_label":   "MOC",
        "full_name":    "Memory of Chaos",
        "subtitle":     "FLOOR 12",
        "dim_field":    "node",
        "dim_label":    "Node",
        "dim_values":   [0, 1, 2],
        "dim_group_labels": {0: "NODE 0 — Both Halves", 1: "NODE 1 — First Half", 2: "NODE 2 — Second Half"},
        "dim_btn_label": lambda d: f"N{d}",
        "is_legacy":    False,
    },
    "apoc": {
        "db_mode":      "APOC",
        "mode_label":   "APOC",
        "full_name":    "Apocalyptic Shadow",
        "subtitle":     "FLOOR 4",
        "dim_field":    "node",
        "dim_label":    "Node",
        "dim_values":   [0, 1, 2 ,3],
        "dim_group_labels": {0: "NODE 0 — Both Halves", 1: "NODE 1 — First Half", 2: "NODE 2 — Second Half", 3: "NODE 3 — Third Half"},
        "dim_btn_label": lambda d: f"N{d}",
        "is_legacy":    False,
    },
    "pf": {
        "db_mode":      "PURE_FICTION",
        "mode_label":   "Pure Fiction",
        "full_name":    "Pure Fiction",
        "subtitle":     "FLOOR 4",
        "dim_field":    "node",
        "dim_label":    "Node",
        "dim_values":   [0, 1, 2],
        "dim_group_labels": {0: "NODE 0 — Both Halves", 1: "NODE 1 — First Half", 2: "NODE 2 — Second Half"},
        "dim_btn_label": lambda d: f"N{d}",
        "is_legacy":    False,
    },
    "anomaly": {
        "db_mode":      "ANOMALY",
        "mode_label":   "Anomaly",
        "full_name":    "Anomaly Arbitration",
        "subtitle":     "FLOORS 0–4",
        "dim_field":    "floor",
        "dim_label":    "Floor",
        "dim_values":   [0, 1, 2, 3, 4],
        "dim_group_labels": {0: "FLOOR 0", 1: "FLOOR 1", 2: "FLOOR 2", 3: "FLOOR 3", 4: "FLOOR 4"},
        "dim_btn_label": lambda d: f"F{d}",
        "is_legacy":    False,
    },
    "moc_legacy": {
        "db_mode":      "MOC_LEGACY",
        "mode_label":   "MOC Legacy",
        "full_name":    "Memory of Chaos (Legacy)",
        "subtitle":     "FLOOR 10",
        "dim_field":    "node",
        "dim_label":    "Node",
        "dim_values":   [0, 1, 2],
        "dim_group_labels": {0: "NODE 0 — Both Halves", 1: "NODE 1 — First Half", 2: "NODE 2 — Second Half"},
        "dim_btn_label": lambda d: f"N{d}",
        "is_legacy":    True,
    },
    "moc_late_legacy": {
        "db_mode":      "MOC_LATE_LEGACY",
        "mode_label":   "MOC Late Legacy",
        "full_name":    "Memory of Chaos (Late Legacy)",
        "subtitle":     "FLOOR 12",
        "dim_field":    "node",
        "dim_label":    "Node",
        "dim_values":   [0, 1, 2],
        "dim_group_labels": {0: "NODE 0 — Both Halves", 1: "NODE 1 — First Half", 2: "NODE 2 — Second Half"},
        "dim_btn_label": lambda d: f"N{d}",
        "is_legacy":    True,
    },
    "pf_legacy": {
        "db_mode":      "PURE_FICTION_LEGACY",
        "mode_label":   "Pure Fiction Legacy",
        "full_name":    "Pure Fiction (Legacy)",
        "subtitle":     "FLOOR 4",
        "dim_field":    "node",
        "dim_label":    "Node",
        "dim_values":   [0, 1, 2],
        "dim_group_labels": {0: "NODE 0 — Both Halves", 1: "NODE 1 — First Half", 2: "NODE 2 — Second Half"},
        "dim_btn_label": lambda d: f"N{d}",
        "is_legacy":    True,
    },
}

TABLE_NAME         = "character_stats"
TEMPLATE_NAME      = "character_stats_template.html.j2"
DEFAULT_DB_PATH    = os.getenv("DB_File")
DEFAULT_OUTPUT_DIR = "./Other Dashboards"
DEFAULT_ICONS_PATH = "character_icons.json"


# Eidolon dist columns — only fetched for non-legacy modes
EID_DIST_COLS = ", ".join(
    f'"Eidolon_{e}_pct_pct" AS "Eidolon_{e}_pct_pct"' for e in range(7)
)


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

def fetch_rows(
    conn: duckdb.DuckDBPyConnection,
    cfg: dict,
    version: str,
) -> list[dict]:
    dim_field  = cfg["dim_field"]
    db_mode    = cfg["db_mode"]
    dim_values = cfg["dim_values"]
    is_legacy  = cfg["is_legacy"]

    placeholders = ", ".join(["?" for _ in dim_values])

    # Eidolon dist columns: include for modern, NULL placeholders for legacy
    if is_legacy:
        eid_select = ", ".join(f"NULL AS \"Eidolon_{e}_pct_pct\"" for e in range(7))
    else:
        eid_select = EID_DIST_COLS

    sql = f"""
        SELECT
            "Rank",
            "version",
            "at_eidolon_level",
            "up_to_eidolon_level",
            "{dim_field}",
            "Character",
            "Appearance_Rate_pct",
            "Samples",
            "Min_Score",
            "Percentile_25",
            "Median_Score",
            "Percentile_75",
            "Average_Score",
            "Std_Dev",
            "Max_Score",
            "Sustain_Samples",
            "Sustain_Percentage",
            "rarity",
            "path",
            "element",
            "role",
            {eid_select}
        FROM {TABLE_NAME}
        WHERE "version" = ?
          AND "mode"    = ?
          AND "{dim_field}" IN ({placeholders})
        ORDER BY "{dim_field}", "at_eidolon_level", "up_to_eidolon_level", "Rank"
    """
    params = [version, db_mode] + dim_values
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

def validate_version(conn: duckdb.DuckDBPyConnection, db_mode: str, version: str) -> None:
    result = conn.execute(
        f'SELECT COUNT(*) FROM {TABLE_NAME} WHERE "version" = ? AND "mode" = ?',
        [version, db_mode]
    ).fetchone()
    if result[0] == 0:
        available = conn.execute(
            f'SELECT DISTINCT "version" FROM {TABLE_NAME} WHERE "mode" = ? ORDER BY "version" DESC LIMIT 20',
            [db_mode]
        ).fetchall()
        versions = [r[0] for r in available]
        print(f"[ERROR] Version '{version}' not found for mode '{db_mode}' in {TABLE_NAME}.")
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
        path_prefix="../",
        mode_label=cfg["mode_label"],
        subtitle=cfg["subtitle"],
        dim_field=cfg["dim_field"],
        dim_label=cfg["dim_label"],
        dim_values=cfg["dim_values"],
        dim_group_labels_json=json.dumps(cfg["dim_group_labels"]),
        is_legacy=cfg["is_legacy"],
        data_json=json.dumps(rows, ensure_ascii=False),
        icons_json=json.dumps(icons, ensure_ascii=False),
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    safe_version = version.replace(".", "_")
    safe_mode    = cfg["mode_label"].lower().replace(" ", "_")
    out_file     = output_dir / f"{safe_mode}_{safe_version}_characters.html"
    out_file.write_text(html, encoding="utf-8")
    return out_file


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a character stats HTML dashboard from DuckDB.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--mode", "-m",
        required=True,
        choices=list(MODE_CONFIG.keys()),
        help="Game mode: " + " | ".join(MODE_CONFIG.keys()),
    )
    parser.add_argument("--version", "-v", required=True, help="Game version, e.g. 4.2.3")
    parser.add_argument("--db",       default=DEFAULT_DB_PATH,    help="Path to DuckDB file.")
    parser.add_argument("--icons",    default=DEFAULT_ICONS_PATH, help="Path to character_icons.json.")
    parser.add_argument("--output", "-o", default=DEFAULT_OUTPUT_DIR, help="Output directory.")
    parser.add_argument("--template-dir", default=str(Path(__file__).parent), help="Directory with the Jinja2 template.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg  = MODE_CONFIG[args.mode]

    db_path      = Path(args.db)
    icons_path   = Path(args.icons)
    output_dir   = Path(args.output)
    template_dir = Path(args.template_dir)

    if not db_path.exists():
        print(f"[ERROR] Database not found: {db_path}"); sys.exit(1)
    if not (template_dir / TEMPLATE_NAME).exists():
        print(f"[ERROR] Template not found: {template_dir / TEMPLATE_NAME}"); sys.exit(1)

    print(f"[INFO] Mode     : {cfg['full_name']} ({args.mode})")
    print(f"[INFO] Legacy   : {cfg['is_legacy']}")
    print(f"[INFO] Dim field: {cfg['dim_field']} {cfg['dim_values']}")
    print(f"[INFO] Connecting to {db_path} ...")
    conn = duckdb.connect(str(db_path), read_only=True)

    print(f"[INFO] Validating version '{args.version}' for mode '{cfg['db_mode']}' ...")
    validate_version(conn, cfg["db_mode"], args.version)

    print(f"[INFO] Fetching rows ...")
    rows = fetch_rows(conn, cfg, args.version)
    conn.close()
    print(f"[INFO] {len(rows)} rows fetched.")

    icons = load_icons(icons_path)
    print(f"[INFO] {len(icons)} icons loaded.")

    print(f"[INFO] Rendering dashboard ...")
    out_file = render_dashboard(cfg, args.version, rows, icons, template_dir, output_dir)
    print(f"[DONE] {out_file}  ({out_file.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
