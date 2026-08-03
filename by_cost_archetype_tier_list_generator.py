"""
by_cost_archetype_tier_list_generator.py
-----------------------------------------
Builds the Dynamic By-Cost Archetype Tier List page.

Unlike the older static archetype_tier_list_e0_generator.py (which bakes a
fixed 'Tier' column per-row in Python via assign_tier()), this generator
ships the RAW performance numbers straight from
`by_cost_archetype_rolling_meta_summary` to the browser and lets the Jinja
template's JS compute T0 / T0.5 / T1 / T1.5 / T2 live, against thresholds
that live in the template (DEFAULT_TIER_CONFIG) and are editable on-page
via the "Tier Settings" panel. That's what makes it "dynamic" -- tiers can
be re-tuned without touching the database or re-running this script.

Source table: by_cost_archetype_rolling_meta_summary
  (built by HonkaiCostArchetypeMetaAnalyzer in database_by_cost_teams_summary.py)

The "Latest" view (and the default page load) is Version_Group_Num == 1 --
the rolling table already carries the most recent trailing-3-version window
per game mode, so there's no need for a separate _recent_meta_summary table
anymore. Version_Group_Num is a PER-MODE rank (1 = each mode's own most
recent As_Of_Version snapshot, 2 = its second most recent, etc), which is
also what history browsing groups by -- see the module docstring further
down for why that matters.

Usage:
    python by_cost_archetype_tier_list_generator.py
    python by_cost_archetype_tier_list_generator.py --db path/to/hsr.duckdb --output docs/tier_list/by_cost_archetype_tier_list.html
"""

import argparse
import json
import math
import os
from decimal import Decimal
from pathlib import Path

import duckdb
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader, select_autoescape

try:
    import brotli
except ImportError:
    brotli = None

load_dotenv()

ROLLING_TABLE = "by_cost_archetype_rolling_meta_summary"
CHARACTER_ROLLING_TABLE = "by_cost_character_rolling_meta_summary"
CHARACTERS_JSON_PATH = "characters.json"

# Roles pulled from characters.json and included as their own tier-list
# sections alongside archetypes. 'dps' and 'specialist' are intentionally
# excluded -- this page is about team-building pieces (who to slot in
# alongside your DPS), not the DPS itself.
CHARACTER_ROLES_INCLUDED = ("sustain", "amplifier")

# Columns pulled from the rolling table. Version_Group_Num is a PER-MODE
# rank (1 = that mode's own most recent As_Of_Version snapshot, 2 = its
# second most recent, etc). Grouping history by this instead of the raw
# As_Of_Version string is what keeps every game mode's tab present at a
# given "snapshot position": different modes can land on different
# calendar versions for the same rank (e.g. MOC's rank-2 might be 4.2.2
# while Pure Fiction's rank-2 is 4.3.1), so filtering on a literal version
# string silently dropped whichever modes didn't happen to share it.
_COMMON_COLUMNS = """
    Game_Mode, at_eidolon_level, up_to_eidolon_level, Archetype_Core,
    estimated_min_cost, estimated_max_cost, max_eidolon,
    Simple_Avg_Appearance, Simple_Avg_Score, Weighted_Avg_Score,
    Weighted_Avg_Median, Best_Version_Avg, Min_Score, Max_Score, Total_Full_Clears,
    Total_Samples, Full_Star_Rate_pct, Total_Sustain_Samples,
    Sustain_Rate_pct, Version_Count, Versions_Used,
    As_Of_Version, Version_Group_Num
"""

# ---------------------------------------------------------------------
# Default tier thresholds, mirrored into the template's DEFAULT_TIER_CONFIG.
# These are just the STARTING POINT baked into the generated HTML; edit
# them here (or directly in the .j2 template / in-browser Tier Settings
# panel) to retune what counts as T0..T2 per game mode.
#
# direction:
#   'asc'  -> lower score is better (cycles-based modes)
#   'desc' -> higher score is better (points-based modes)
# thresholds.T0 / T0_5 / T1 / T1_5 are the boundary between that tier and
# the next-worse one; anything past T1_5 falls into T2.
# ---------------------------------------------------------------------
DEFAULT_TIER_CONFIG = {
    "MOC": {
        "direction": "asc",
        "thresholds": {"T0": 2.0, "T0_5": 3.5, "T1": 5.0, "T1_5": 7.0},
    },
    "ANOMALY_F0": {
        "direction": "asc",
        "thresholds": {"T0": 1.0, "T0_5": 2.0, "T1": 3.0, "T1_5": 4.5},
    },
    "ANOMALY_F4": {
        "direction": "asc",
        "thresholds": {"T0": 1.0, "T0_5": 2.0, "T1": 3.0, "T1_5": 4.5},
    },
    "ANOMALY_F5": {
        "direction": "asc",
        "thresholds": {"T0": 0.3, "T0_5": 0.8, "T1": 1.3, "T1_5": 1.8},
    },
    "APOC": {
        "direction": "desc",
        "thresholds": {"T0": 3800, "T0_5": 3650, "T1": 3450, "T1_5": 3200},
    },
    "PURE_FICTION": {
        "direction": "desc",
        "thresholds": {"T0": 39500, "T0_5": 37000, "T1": 33000, "T1_5": 28000},
    },
}

# ---------------------------------------------------------------------
# Default cost-bracket presets shown as Quick Preset buttons in the cost
# panel. Purely a UI convenience -- editable live in-browser via the
# "Cost Tier Settings" editor in the Tier Settings panel, same pattern as
# DEFAULT_TIER_CONFIG. min/max are in the same estimated_min_cost /
# estimated_max_cost units as the DB columns.
# ---------------------------------------------------------------------
DEFAULT_COST_TIERS = [
    {"key": "f2p",    "label": "F2P",    "min": 0,  "max": 4},
    {"key": "budget", "label": "Budget", "min": 0,  "max": 8},
    {"key": "mid",    "label": "Mid",    "min": 8,  "max": 16},
    {"key": "high",   "label": "High",   "min": 16, "max": 24},
    {"key": "whale",  "label": "Whale",  "min": 24, "max": 32},
]

# Default "reach at least this score" target that seeds the Cost
# Efficiency ranking mode's per-mode threshold input. Mirrors each mode's
# T1 boundary from DEFAULT_TIER_CONFIG above -- i.e. the out-of-the-box
# question is "what's the cheapest way to be at least T1-tier?". Fully
# adjustable live in-browser, same as the tier thresholds.
DEFAULT_EFFICIENCY_TARGET = {"MOC": 0.0 , "ANOMALY_F0": 0.0, "ANOMALY_F4": 0.0, "ANOMALY_F5": 0.0, "APOC": 3850, "PURE_FICTION": 40000}

MODE_META = {
    "MOC":          {"label": "MOC",         "full": "Memory of Chaos"},
    "APOC":         {"label": "APOC",        "full": "Apocalyptic Shadow"},
    "PURE_FICTION": {"label": "Pure Fiction","full": "Pure Fiction"},
    "ANOMALY_F0":   {"label": "Anomaly F0",  "full": "Anomaly Arbitration — Floor 0"},
    "ANOMALY_F4":   {"label": "Anomaly F4",  "full": "Anomaly Arbitration — Floor 4"},
    "ANOMALY_F5":   {"label": "Anomaly F5*", "full": "Anomaly Arbitration — Floor 5 (Hard)"},
}

# Default hard floor for Simple_Avg_Appearance (%) below which an archetype
# is excluded from tiering entirely (too few samples to trust its score).
DEFAULT_MIN_APPEARANCE = 0.1


def clean_rows(cursor) -> list[dict]:
    cols = [desc[0] for desc in cursor.description]
    rows = []
    for raw in cursor.fetchall():
        row = dict(zip(cols, raw))
        for k, v in row.items():
            if isinstance(v, Decimal):
                v = float(v)
                row[k] = v
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                row[k] = None
        rows.append(row)
    return rows


def fetch_rolling_group_nums(db_path: str) -> list[int]:
    """All distinct Version_Group_Num snapshots, 1 (newest) first, ascending."""
    conn = duckdb.connect(db_path, read_only=True)
    try:
        rows = conn.execute(f"""
            SELECT DISTINCT Version_Group_Num
            FROM {ROLLING_TABLE}
            ORDER BY Version_Group_Num
        """).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


def fetch_rolling_data_by_group(db_path: str, group_num: int) -> list[dict]:
    """
    Rows for a single Version_Group_Num snapshot, across ALL game modes.
    Because Version_Group_Num is ranked per-mode, this is what guarantees
    every mode that HAS a snapshot at this rank shows up together, even
    though their underlying As_Of_Version strings can differ.
    """
    conn = duckdb.connect(db_path, read_only=True)
    try:
        cur = conn.execute(f"""
            SELECT {_COMMON_COLUMNS}
            FROM {ROLLING_TABLE}
            WHERE Version_Group_Num = ? AND Total_Samples > 0
            ORDER BY Game_Mode, Weighted_Avg_Score
        """, [group_num])
        rows = clean_rows(cur)
        for r in rows:
            r["Category"] = "archetype"
        return rows
    finally:
        conn.close()


def load_character_roles(path: str) -> dict:
    """name -> list of roles, e.g. {'Aventurine': ['sustain'], 'Robin': ['amplifier']}."""
    p = Path(path)
    if not p.exists():
        print(f"[WARN] {path} not found; the Sustains/Amplifiers sections will be empty.")
        return {}
    with open(p, encoding="utf-8") as f:
        raw = json.load(f)
    return {name: info.get("role", []) for name, info in raw.items()}


def fetch_character_rolling_data_by_group(db_path: str, group_num: int, character_roles: dict) -> list[dict]:
    """
    Sustain/Amplifier character rows for a single Version_Group_Num snapshot,
    normalized to the same shape as fetch_rolling_data_by_group() so both
    can be concatenated into one dataset. 
    
    This is executed purely via DuckDB SQL which handles stripping Eidolon
    strings e.g., 'Robin(E1)' or 'Tribbie (e5)' -> 'Robin'/'Tribbie'
    matching the base name against our characters.json list.
    """
    # Build a VALUES list to load character roles into DuckDB as an inline CTE
    values_clauses = []
    for name, roles in character_roles.items():
        for role in roles:
            if role in CHARACTER_ROLES_INCLUDED:
                safe_name = name.replace("'", "''")  # escape quotes just in case
                values_clauses.append(f"('{safe_name}', '{role}')")
                
    if not values_clauses:
        return []

    values_sql = ",\n".join(values_clauses)
    
    # regex \s*\([Ee]\d+\)$ strips eidolon suffixes efficiently prior to join matching
    query = rf"""
        WITH char_roles(Base_Character, Category) AS (
            SELECT * FROM (VALUES
                {values_sql}
            )
        )
        SELECT 
            Game_Mode, 
            at_eidolon_level, 
            up_to_eidolon_level, 
            db.Character AS Archetype_Core,
            estimated_min_cost, 
            estimated_max_cost, 
            max_eidolon,
            Simple_Avg_Appearance, 
            Simple_Avg_Score, 
            Weighted_Avg_Score,
            Weighted_Avg_Median, 
            Best_Version_Avg, 
            Min_Score,
            Max_Score,
            Total_Full_Clears,
            Total_Samples, 
            Full_Star_Rate_pct, 
            Total_Sustain_Samples,
            Sustain_Rate_pct, 
            Version_Count, 
            Versions_Used,
            As_Of_Version, 
            Version_Group_Num,
            cr.Category
        FROM {CHARACTER_ROLLING_TABLE} db
        JOIN char_roles cr 
          ON regexp_replace(db.Character, '\s*\([Ee]\d+\)$', '') = cr.Base_Character
        WHERE Version_Group_Num = ? AND Total_Samples > 0
        ORDER BY Game_Mode, Weighted_Avg_Score
    """

    conn = duckdb.connect(db_path, read_only=True)
    try:
        cur = conn.execute(query, [group_num])
        rows = clean_rows(cur)
    finally:
        conn.close()

    return rows


def write_brotli_json(out_path: Path, data) -> None:
    payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
    if brotli is None:
        raise RuntimeError("The 'brotli' package is required. pip install brotli --break-system-packages")
    out_path.write_bytes(brotli.compress(payload, quality=8))


def load_icons(icons_path: Path) -> dict:
    if not icons_path.exists():
        print(f"[WARN] Icons file not found at {icons_path}; icons will be skipped.")
        return {}
    with open(icons_path, encoding="utf-8") as f:
        return json.load(f)


def build(args):
    db_path = args.db or os.getenv("DB_File")
    if not db_path:
        raise ValueError("No DB path provided and DB_File is not set in .env")

    character_roles = load_character_roles(args.characters)

    print(f"[INFO] Reading {ROLLING_TABLE} + {CHARACTER_ROLLING_TABLE} (Version_Group_Num = 1, i.e. Latest) from {db_path} ...")
    data = fetch_rolling_data_by_group(db_path, 1) + fetch_character_rolling_data_by_group(db_path, 1, character_roles)
    cat_counts = {}
    for r in data:
        cat_counts[r["Category"]] = cat_counts.get(r["Category"], 0) + 1
    print(f"[INFO] Fetched {len(data):,} rows across {len({r['Game_Mode'] for r in data})} game modes. By category: {cat_counts}")

    mode_versions_latest = {}
    for r in data:
        mode_versions_latest.setdefault(r["Game_Mode"], r["As_Of_Version"])
    versions_seen = sorted(set(mode_versions_latest.values()), reverse=True)
    version_label = ", ".join(versions_seen[:4]) + ("…" if len(versions_seen) > 4 else "") if versions_seen else "recent"

    out_html = Path(args.output)
    out_dir = out_html.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    data_filename = args.data_filename
    write_brotli_json(out_dir / data_filename, data)
    print(f"  [DONE] {data_filename} ({(out_dir / data_filename).stat().st_size / 1024:.1f} KB)")

    # -------------------------------------------------------------
    # Rolling-history snapshots: one Brotli file per Version_Group_Num,
    # combining archetype + sustain/amplifier character rows, so the tier
    # list page can let the person browse past rankings across all three
    # sections with every game mode's tab present at each snapshot position.
    # -------------------------------------------------------------
    history_manifest = []
    if not args.skip_history:
        history_dir = out_dir / "history"
        history_dir.mkdir(parents=True, exist_ok=True)

        group_nums = fetch_rolling_group_nums(db_path)
        print(f"[INFO] Found {len(group_nums):,} rolling Version_Group_Num snapshots.")

        for g in group_nums:
            rows = fetch_rolling_data_by_group(db_path, g) + fetch_character_rolling_data_by_group(db_path, g, character_roles)
            if not rows:
                continue
            fname = f"by_cost_archetype_history_g{g}.json.br"
            write_brotli_json(history_dir / fname, rows)

            mode_versions = {}
            for r in rows:
                mode_versions.setdefault(r["Game_Mode"], r["As_Of_Version"])
            versions_used = sorted({
                u.strip() for r in rows for u in (r.get("Versions_Used") or "").split(",") if u.strip()
            })
            history_manifest.append({
                "group_num": g,
                "file": f"history/{fname}",
                "mode_versions": mode_versions,
                "versions_used": versions_used,
                "row_count": len(rows),
                "is_latest": (g == 1),
            })
            print(f"  [DONE] history/{fname} ({(history_dir / fname).stat().st_size / 1024:.1f} KB, {len(rows):,} rows, modes={sorted(mode_versions.keys())})")

    icons = load_icons(Path(args.icons))

    env = Environment(
        loader=FileSystemLoader(str(Path(args.template_dir))),
        autoescape=select_autoescape(disabled_extensions=["j2", "html"]),
    )
    template = env.get_template(args.template)

    context = {
        "version_label": version_label,
        "subtitle": "ROLLING LAST-3-VERSION WINDOW · ARCHETYPES, SUSTAINS & AMPLIFIERS · ALL GAME MODES",
        "path_prefix": "../",
        "data_filename": data_filename,
        "icons_json": json.dumps(icons, ensure_ascii=False),
        "tier_config_json": json.dumps(DEFAULT_TIER_CONFIG, ensure_ascii=False),
        "mode_meta_json": json.dumps(MODE_META, ensure_ascii=False),
        "default_min_appearance": DEFAULT_MIN_APPEARANCE,
        "cost_tiers_json": json.dumps(DEFAULT_COST_TIERS, ensure_ascii=False),
        "efficiency_target_json": json.dumps(DEFAULT_EFFICIENCY_TARGET, ensure_ascii=False),
        "history_manifest_json": json.dumps(history_manifest, ensure_ascii=False),
    }

    html = template.render(**context)
    out_html.write_text(html, encoding="utf-8")
    print(f"[SUCCESS] Wrote {out_html} ({out_html.stat().st_size / 1024:.0f} KB)")


def main():
    parser = argparse.ArgumentParser(description="Generate the Dynamic By-Cost Archetype Tier List.")
    parser.add_argument("--db", default=None, help="Path to DuckDB file (defaults to DB_File in .env)")
    parser.add_argument("--icons", default="character_icons.json", help="Path to character icons JSON")
    parser.add_argument("--characters", default=CHARACTERS_JSON_PATH, help="Path to characters.json (name -> role mapping, for the Sustains/Amplifiers sections)")
    parser.add_argument("--template-dir", default=str(Path(__file__).parent), help="Dir containing .j2 templates")
    parser.add_argument("--template", default="by_cost_archetype_tier_list_template.html.j2", help="Template filename")
    parser.add_argument("--output", default="docs/tier_list/by_cost_archetype_tier_list.html", help="Output HTML path")
    parser.add_argument("--data-filename", default="by_cost_archetype_tier_list_data.json.br", help="Output data filename (written alongside --output)")
    parser.add_argument("--skip-history", action="store_true", help="Skip generating per-version rolling history snapshots (faster iteration)")
    args = parser.parse_args()
    build(args)


if __name__ == "__main__":
    main()