"""
database_trends_export.py
--------------------------------------------------------------------------
Exports version-over-version trend data from archetype_meta_summary and
character_meta_summary into per-slice Brotli-compressed JSON files, using
the exact same manifest-driven convention as network_export.py:

    docs/trends/archetypes/<mode>_e<at>-<upto>.json.br
    docs/trends/characters/<mode>_e<at>-<upto>.json.br

    docs/trends/archetype_trends_manifest.json
    docs/trends/character_trends_manifest.json

Both summary tables already carry, per (Game_Mode, at_eidolon_level,
up_to_eidolon_level, entity) row, a set of "_List" columns — one value per
version the entity appeared in — plus a Versions_Used string listing those
versions in the *same row-iteration order* as the list columns (they're all
produced by aggregate functions in one GROUP BY query, so they share
positional alignment). That order is NOT chronological, so this script
re-derives a chronologically-sorted version axis per entity and reorders
every trend list to match before writing it out.

This script is a pure exporter — it does not decide chart types, colors,
or which metric to plot; that's the template's job (trends_template.html.j2),
matching the network_export.py / network_dashboard_svg.html.j2 split.

Design notes:
    - "All columns are used": columns are discovered dynamically via
      DESCRIBE, not hardcoded, so both tables (which have slightly
      different schemas — character rows carry role/element/path/etc.,
      archetype rows don't) are handled by the same generic code path.
    - Any column ending in "_List" is treated as a trend series; every
      other column (except the identity column and Versions_Used) is
      treated as a static "summary" scalar.
    - Trend lists are intentionally kept OUT of the page's visible data
      table — the template only renders `summary` columns in the grid,
      and only reads `trend` + `versions` when a user opens a chart for
      one entity. That satisfies "lists hidden from the browser [table]".

Usage:
    python database_trends_export.py
    python database_trends_export.py --db path/to/hsr.duckdb --output-root docs/trends

Dependencies:
    pip install duckdb pandas brotli python-dotenv
"""

import argparse
import json
import math
import os
import re
from pathlib import Path

import duckdb
import pandas as pd
from dotenv import load_dotenv

try:
    import brotli
except ImportError:
    brotli = None  # compression becomes a no-op if the package isn't installed

load_dotenv()
DB_FILE = os.getenv("DB_File")


# ---------------------------------------------------------------------------
# Table configuration — this is the only per-table hardcoding; everything
# else (which columns exist, which are lists) is introspected at runtime.
# ---------------------------------------------------------------------------

TABLE_CONFIGS = [
    {
        "table":        "archetype_meta_summary",
        "identity_col": "Archetype_Core",
        "entity_type":  "archetype",
        "output_dir":   "docs/trends/archetypes",
        "manifest_path": "docs/trends/archetype_trends_manifest.json",
    },
    {
        "table":        "character_meta_summary",
        "identity_col": "Character",
        "entity_type":  "character",
        "output_dir":   "docs/trends/characters",
        "manifest_path": "docs/trends/character_trends_manifest.json",
    },
    {
        "table":        "team_meta_summary",
        "identity_col": "Team",
        "entity_type":  "team",
        "output_dir":   "docs/trends/teams",
        "manifest_path": "docs/trends/team_trends_manifest.json",
    },
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_VERSION_RE = re.compile(r"\d+")


def _version_sort_key(v: str):
    """Chronological sort key for version strings like '4.2.2' or '3.10.1'.

    Falls back to the raw string if it doesn't look like a dotted version,
    so unexpected values don't crash the export — they just sort last.
    """
    parts = _VERSION_RE.findall(v or "")
    if not parts:
        return (float("inf"), v)
    return tuple(int(p) for p in parts)


def _clean(v):
    """Convert numpy/pandas scalars to plain JSON-safe Python values."""
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if hasattr(v, "item"):  # numpy scalar (int64, float64, etc.)
        v = v.item()
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        return v
    return v


def _clean_list(lst):
    if lst is None:
        return None
    return [_clean(v) for v in lst]


def get_columns(conn: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    return [row[0] for row in conn.execute(f"DESCRIBE {table}").fetchall()]


# Slice-key columns are already carried at the top level of each exported
# payload (game_mode / at_eidolon_level / up_to_eidolon_level), so they're
# constant across every entity in a slice and dropped here to avoid a
# redundant, always-identical column in the table.
_SLICE_KEY_COLS = ("Game_Mode", "at_eidolon_level", "up_to_eidolon_level")


def split_columns(columns: list[str], identity_col: str) -> tuple[list[str], list[str]]:
    """Return (list_columns, summary_columns), excluding identity + Versions_Used
    + the slice-key columns (which are redundant once you're inside a slice)."""
    list_cols = [c for c in columns if c.endswith("_List")]
    summary_cols = [
        c for c in columns
        if c not in list_cols
        and c not in (identity_col, "Versions_Used")
        and c not in _SLICE_KEY_COLS
    ]
    return list_cols, summary_cols


def get_slices(conn: duckdb.DuckDBPyConnection, table: str) -> list[tuple]:
    return conn.execute(f"""
        SELECT DISTINCT Game_Mode, at_eidolon_level, up_to_eidolon_level
        FROM {table}
        ORDER BY Game_Mode, at_eidolon_level, up_to_eidolon_level
    """).fetchall()


# ---------------------------------------------------------------------------
# Per-slice export
# ---------------------------------------------------------------------------

def export_slice(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    identity_col: str,
    entity_type: str,
    list_cols: list[str],
    summary_cols: list[str],
    game_mode: str,
    at_eidolon: int,
    up_to_eidolon: int,
    output_dir: Path,
    manifest_path: Path,
    compress: bool = True,
) -> int:
    select_cols = ", ".join([identity_col] + summary_cols + list_cols + ["Versions_Used"])
    query = f"""
        SELECT {select_cols}
        FROM {table}
        WHERE Game_Mode = ? AND at_eidolon_level = ? AND up_to_eidolon_level = ?
    """
    df = conn.execute(query, [game_mode, at_eidolon, up_to_eidolon]).df()
    if df.empty:
        return 0

    trend_metric_names = [c[:-len("_List")] for c in list_cols]

    entities = []
    mismatches = 0
    for _, row in df.iterrows():
        versions_raw = [v.strip() for v in str(row["Versions_Used"] or "").split(",") if v.strip()]
        order = sorted(range(len(versions_raw)), key=lambda i: _version_sort_key(versions_raw[i]))
        sorted_versions = [versions_raw[i] for i in order]

        trend = {}
        for col in list_cols:
            metric_name = col[:-len("_List")]
            raw_list = row[col]
            raw_list = list(raw_list) if raw_list is not None else None
            if raw_list is None or len(raw_list) != len(versions_raw):
                # Alignment guard: if a list column's length doesn't match
                # Versions_Used, don't silently misalign values to versions —
                # emit null and let it surface as a visible gap instead.
                trend[metric_name] = None
                mismatches += 1
            else:
                trend[metric_name] = [_clean(raw_list[i]) for i in order]

        summary = {col: _clean(row[col]) for col in summary_cols}

        entities.append({
            "id": row[identity_col],
            "summary": summary,
            "versions": sorted_versions,
            "trend": trend,
        })

    if mismatches:
        print(f"  [WARN] {mismatches} entity/metric list(s) had a length mismatch "
              f"against Versions_Used in {game_mode} E{at_eidolon}-E{up_to_eidolon} "
              f"— written as null rather than misaligned.")

    payload = {
        "entity_type": entity_type,
        "game_mode": game_mode,
        "at_eidolon_level": int(at_eidolon),
        "up_to_eidolon_level": int(up_to_eidolon),
        "trend_metrics": trend_metric_names,
        "summary_columns": summary_cols,
        "identity_field": identity_col,
        "entities": entities,
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{game_mode.lower()}_e{at_eidolon}-{up_to_eidolon}.json"
    raw_bytes = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

    if compress and brotli is not None:
        out_filename = filename + ".br"
        out_path = output_dir / out_filename
        out_path.write_bytes(brotli.compress(raw_bytes, quality=11))
    else:
        if compress and brotli is None:
            print("  [WARN] brotli package not installed — writing uncompressed JSON instead.")
        out_filename = filename
        out_path = output_dir / out_filename
        out_path.write_bytes(raw_bytes)

    _update_manifest(
        manifest_path=manifest_path,
        game_mode=game_mode,
        eidolon_range_key=f"{at_eidolon}-{up_to_eidolon}",
        # Store a path RELATIVE TO THE MANIFEST'S OWN DIRECTORY (docs/trends/),
        # not an absolute/repo-root path — the page fetches manifest entries
        # directly, and only "./archetypes/foo.json.br"-style relative paths
        # resolve correctly from docs/trends/index.html. An absolute-looking
        # path (e.g. "docs/trends/archetypes/foo.json.br") 404s from there.
        relative_path="./" + os.path.relpath(out_path, manifest_path.parent).replace(os.sep, "/"),
    )

    print(f"  + {out_path}  ({len(entities)} entities, {out_path.stat().st_size / 1024:.1f} KB)")
    return len(entities)


def _update_manifest(manifest_path: Path, game_mode: str, eidolon_range_key: str, relative_path: str):
    """
    Manifest shape:
        { "<GAME_MODE>": { "<at>-<upto>": "path/to/slice.json.br" } }
    """
    manifest = {}
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            manifest = {}

    manifest.setdefault(game_mode, {})
    manifest[game_mode][eidolon_range_key] = relative_path

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_export(db_file: str, output_root: str = None, compress: bool = True):
    conn = duckdb.connect(db_file, read_only=True)

    for cfg in TABLE_CONFIGS:
        table = cfg["table"]
        identity_col = cfg["identity_col"]
        entity_type = cfg["entity_type"]
        output_dir = Path(output_root) / Path(cfg["output_dir"]).name if output_root else Path(cfg["output_dir"])
        manifest_path = Path(output_root) / Path(cfg["manifest_path"]).name if output_root else Path(cfg["manifest_path"])

        print(f"\n=== {table} -> {output_dir} ===")

        columns = get_columns(conn, table)
        list_cols, summary_cols = split_columns(columns, identity_col)
        print(f"  {len(list_cols)} trend columns, {len(summary_cols)} summary columns")

        slices = get_slices(conn, table)
        print(f"  {len(slices)} (Game_Mode, eidolon) slices to export")

        total_entities = 0
        for game_mode, at_eidolon, up_to_eidolon in slices:
            n = export_slice(
                conn=conn,
                table=table,
                identity_col=identity_col,
                entity_type=entity_type,
                list_cols=list_cols,
                summary_cols=summary_cols,
                game_mode=game_mode,
                at_eidolon=at_eidolon,
                up_to_eidolon=up_to_eidolon,
                output_dir=output_dir,
                manifest_path=manifest_path,
                compress=compress,
            )
            total_entities += n

        print(f"  >>> {table}: {total_entities} entity-rows exported across {len(slices)} slices")

    conn.close()
    print("\n[DONE] Trend export complete.")


def main():
    parser = argparse.ArgumentParser(description="Export archetype/character trend data as Brotli JSON slices.")
    parser.add_argument("--db", default=DB_FILE, help="Path to the DuckDB database file.")
    parser.add_argument("--output-root", default=None,
                        help="If set, overrides the output directory root (dirs/manifests keep their basenames "
                             "under this root). Defaults to the docs/trends/... paths baked into TABLE_CONFIGS.")
    parser.add_argument("--no-compress", action="store_true", help="Write raw JSON instead of Brotli.")
    args = parser.parse_args()

    if not args.db:
        raise SystemExit("[ERROR] No DB path given and DB_File is not set in the environment.")
    if not Path(args.db).exists():
        raise SystemExit(f"[ERROR] Database not found: {args.db}")

    run_export(db_file=args.db, output_root=args.output_root, compress=not args.no_compress)


if __name__ == "__main__":
    main()
