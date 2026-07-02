"""
generate_dashboards.py
-----------------------
Unified generator for HSR endgame telemetry dashboards.
Builds Character, Archetype, and Team stats across all modes,
plus the root index.html routing hub.

Usage:
    python generate_dashboards.py --version 4.3.1
    python generate_dashboards.py --version 4.3.1 --db path/to/hsr.duckdb

Outputs to:
    docs/
    ├── index.html
    ├── moc/
    ├── apoc/
    ├── pure_fiction/
    └── anomaly_arbitration/
"""

import argparse
import json
import math
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import duckdb
import gzip
from jinja2 import Environment, FileSystemLoader, select_autoescape

load_dotenv()

class DashboardGenerator:
    # Core mode configurations mapped to their specific database tables and output routes
    MODE_CONFIG = {
        "moc": {
            "folder": "moc",
            "file_prefix": "moc",
            "char_db_mode": "MOC",
            "arch_table": "moc_stats_archetypes",
            "team_table": "moc_stats_teams",
            "cost_team_table": "moc_by_cost_teams",
            "duo_table": "moc_stats_duos",
            "mode_label": "MOC",
            "full_name": "Memory of Chaos",
            "subtitle": "FLOOR 12",
            "dim_field": "node",
            "dim_label": "Node",
            "dim_values": [0, 1, 2],
            "dim_group_labels": {0: "NODE 0 — All Nodes", 1: "NODE 1 — First Half", 2: "NODE 2 — Second Half"},
            "dim_btn_label": lambda d: f"N{d}",
            "is_legacy": False,
        },
        "apoc": {
            "folder": "apoc",
            "file_prefix": "apoc",
            "char_db_mode": "APOC",
            "arch_table": "apoc_stats_archetypes",
            "team_table": "apoc_stats_teams",
            "cost_team_table": "apoc_by_cost_teams",
            "duo_table": "apoc_stats_duos",
            "mode_label": "APOC",
            "full_name": "Apocalyptic Shadow",
            "subtitle": "FLOOR 4",
            "dim_field": "node",
            "dim_label": "Node",
            "dim_values": [0, 1, 2, 3],
            "dim_group_labels": {0: "NODE 0 — All Nodes", 1: "NODE 1 — First Half", 2: "NODE 2 — Second Half", 3: "NODE 3 — Third Half"},
            "dim_btn_label": lambda d: f"N{d}",
            "is_legacy": False,
        },
        "pf": {
            "folder": "pure_fiction",
            "file_prefix": "pure_fiction",
            "char_db_mode": "PURE_FICTION",
            "arch_table": "pure_fiction_stats_archetypes",
            "team_table": "pure_fiction_stats_teams",
            "cost_team_table": "pure_fiction_by_cost_teams",
            "duo_table": "pure_fiction_stats_duos",
            "mode_label": "Pure Fiction",
            "full_name": "Pure Fiction",
            "subtitle": "FLOOR 4",
            "dim_field": "node",
            "dim_label": "Node",
            "dim_values": [0, 1, 2, 3],
            "dim_group_labels": {0: "NODE 0 — All Nodes", 1: "NODE 1 — First Half", 2: "NODE 2 — Second Half", 3: "NODE 3 — Third Half"},
            "dim_btn_label": lambda d: f"N{d}",
            "is_legacy": False,
        },
        "anomaly": {
            "folder": "anomaly_arbitration",
            "file_prefix": "anomaly",
            "char_db_mode": "ANOMALY",
            "arch_table": "anomaly_stats_archetypes",
            "team_table": "anomaly_stats_teams",
            "cost_team_table": "anomaly_by_cost_teams",
            "duo_table": "anomaly_stats_duos",
            "mode_label": "Anomaly",
            "full_name": "Anomaly Arbitration",
            "subtitle": "FLOORS 0–4",
            "dim_field": "floor",
            "dim_label": "Floor",
            "dim_values": [0, 1, 2, 3, 4],
            "dim_group_labels": {0: "FLOOR 0", 1: "FLOOR 1", 2: "FLOOR 2", 3: "FLOOR 3", 4: "FLOOR 4"},
            "dim_btn_label": lambda d: f"F{d}",
            "is_legacy": False,
        }
    }

    CHAR_TABLE = "character_stats"

    def __init__(self, db_path: Path, icons_path: Path, template_dir: Path, output_base: Path):
        self.db_path = db_path
        self.output_base = output_base
        self.template_dir = template_dir
        
        # Initialize dependencies
        self._check_paths()
        self.conn = duckdb.connect(str(self.db_path), read_only=True)
        self.icons = self._load_icons(icons_path)
        self.env = Environment(
            loader=FileSystemLoader(str(self.template_dir)),
            autoescape=select_autoescape(disabled_extensions=["j2", "html"])
        )

    def _check_paths(self):
        if not self.db_path.exists():
            sys.exit(f"[ERROR] Database not found: {self.db_path}")
        if not self.template_dir.exists():
            sys.exit(f"[ERROR] Template directory not found: {self.template_dir}")

    def _load_icons(self, icons_path: Path) -> dict:
        if not icons_path.exists():
            print(f"[WARN] Icons file skipped. Not found at {icons_path}")
            return {}
        with open(icons_path, encoding="utf-8") as f:
            return json.load(f)

    def _clean_rows(self, cursor) -> list[dict]:
        cols = [desc[0] for desc in cursor.description]
        rows = []
        for raw in cursor.fetchall():
            row = dict(zip(cols, raw))
            for k, v in row.items():
                if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                    row[k] = None
            rows.append(row)
        return rows

    def _has_data(self, table: str, version: str, mode_col: str = None, mode_val: str = None) -> bool:
        if mode_col:
            res = self.conn.execute(f'SELECT COUNT(*) FROM {table} WHERE "version" = ? AND "{mode_col}" = ?', [version, mode_val]).fetchone()
        else:
            res = self.conn.execute(f"SELECT COUNT(*) FROM {table} WHERE version = ?", [version]).fetchone()
        return res[0] > 0

    # -- Data Fetching --------------------------------------------------------

    def fetch_characters(self, cfg: dict, version: str) -> list[dict]:
        dim_field = cfg["dim_field"]
        placeholders = ", ".join(["?" for _ in cfg["dim_values"]])
        eid_select = ", ".join(f'"{e}" AS "{e}"' for e in [f"Eidolon_{i}_pct_pct" for i in range(7)]) if not cfg["is_legacy"] else ", ".join(f'NULL AS "Eidolon_{i}_pct_pct"' for i in range(7))

        sql = f"""
            SELECT
                "Rank", "version", "at_eidolon_level", "up_to_eidolon_level",
                "{dim_field}", "Character", "Appearance_Rate_pct", "Samples",
                "Min_Score", "Percentile_25", "Median_Score", "Percentile_75",
                "Average_Score", "Std_Dev", "Max_Score", "Sustain_Samples",
                "Sustain_Percentage", "Total_Full_Clears", "Full_Clear_Rate_pct",
                "rarity", "path", "element", "role",
                {eid_select}
            FROM {self.CHAR_TABLE}
            WHERE "version" = ? AND "mode" = ? AND "{dim_field}" IN ({placeholders})
            ORDER BY "{dim_field}", "at_eidolon_level", "up_to_eidolon_level", "Rank"
        """
        params = [version, cfg["char_db_mode"]] + cfg["dim_values"]
        return self._clean_rows(self.conn.execute(sql, params))

    def fetch_archetypes(self, cfg: dict, version: str) -> list[dict]:
        dim_field = cfg["dim_field"]
        placeholders = ", ".join(["?" for _ in cfg["dim_values"]])
        sql = f"""
            SELECT
                Rank, version, at_eidolon_level, up_to_eidolon_level,
                {dim_field}, Archetype_Core, Usage_pct, Samples,
                Sustain_Percentage, Sustain_Samples, Full_Clear_Rate_pct, Total_Full_Clears,
                Min_Score, Percentile_25,
                Median_Score, Percentile_75, Average_Score, Max_Score, Std_Dev
            FROM {cfg['arch_table']}
            WHERE version = ? AND {dim_field} IN ({placeholders})
            ORDER BY {dim_field}, at_eidolon_level, up_to_eidolon_level, Rank
        """
        params = [version] + cfg["dim_values"]
        return self._clean_rows(self.conn.execute(sql, params))

    def fetch_cost_teams(self, cfg: dict, version: str) -> list[dict]:
        dim_field = cfg["dim_field"]
        placeholders = ", ".join(["?" for _ in cfg["dim_values"]])
        sql = f"""
            SELECT
                Rank, version, estimated_min_cost, estimated_max_cost,
                {dim_field}, max_eidolon, Team, Archetype_Core, has_sustain,
                Sustain_Count, Appearance_Rate_pct, Samples, Full_Star_Clears,
                Full_Star_Rate_pct, Min_Score, Percentile_25, Median_Score,
                Percentile_75, Average_Score, Std_Dev, Max_Score
            FROM {cfg['cost_team_table']}
            WHERE version = ? AND {dim_field} IN ({placeholders})
            ORDER BY {dim_field}, estimated_min_cost, max_eidolon, Rank
        """
        params = [version] + cfg["dim_values"]
        rows = self._clean_rows(self.conn.execute(sql, params))
        for row in rows:
            row["has_sustain"] = bool(row.get("has_sustain") or False)
        return rows

    def fetch_duos(self, cfg: dict, version: str) -> list[dict]:
        dim_field = cfg["dim_field"]
        placeholders = ", ".join(["?" for _ in cfg["dim_values"]])
        sql = f"""
            SELECT
                version, at_eidolon_level, up_to_eidolon_level,
                {dim_field}, Antecedent, Consequent, Samples,
                Appearance_Rate_pct, Confidence, Lift, Leverage, Conviction,
                Total_Sustains, Sustain_Percentage, Total_Full_Clears, Full_Clear_Rate_pct,
                Percentile_25, Median_Score, Percentile_75, Std_Dev,
                Min_Score, Average_Score, Max_Score
            FROM {cfg['duo_table']}
            WHERE version = ? AND {dim_field} IN ({placeholders})
            ORDER BY {dim_field}, at_eidolon_level, up_to_eidolon_level, Appearance_Rate_pct DESC
        """
        params = [version] + cfg["dim_values"]
        return self._clean_rows(self.conn.execute(sql, params))

    def fetch_teams(self, cfg: dict, version: str) -> list[dict]:
        dim_field = cfg["dim_field"]
        placeholders = ", ".join(["?" for _ in cfg["dim_values"]])
        sql = f"""
            SELECT
                Rank, version, at_eidolon_level, up_to_eidolon_level,
                {dim_field}, Team, Archetype_Core, Appearance_Rate_pct, Samples, Min_Score,
                Percentile_25, Median_Score, Percentile_75, Average_Score,
                Std_Dev, Max_Score, Full_Clear_Rate_pct, Total_Full_Clears, "Sustain?" AS sustain
            FROM {cfg['team_table']}
            WHERE version = ? AND {dim_field} IN ({placeholders})
            ORDER BY {dim_field}, at_eidolon_level, up_to_eidolon_level, Rank
        """
        params = [version] + cfg["dim_values"]
        rows = self._clean_rows(self.conn.execute(sql, params))
        for row in rows:
            row["Sustain?"] = bool(row.pop("sustain", False) or False)
        return rows

    # -- Rendering ------------------------------------------------------------

    def _write_gz_json(self, out_dir: Path, filename: str, data) -> None:
        with gzip.open(out_dir / filename, 'wb', compresslevel=6) as f:
            f.write(json.dumps(data, ensure_ascii=False).encode('utf-8'))

    def _write_json(self, out_dir: Path, filename: str, data) -> None:
        with open(out_dir / filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)

    def _discover_versions(self, mode_dir: Path, prefix: str, page: str) -> list[dict]:
        """Scans mode_dir for every already-generated {prefix}_{ver}_{page}_data.json.gz
        file and returns [{"label": "4.3.2", "safe": "4_3_2"}, ...] sorted newest-first.
        This lets the in-page version switcher discover every version ever generated,
        not just the one being rendered in this run."""
        if not mode_dir.exists():
            return []
        front = f"{prefix}_"
        back = f"_{page}_data.json.gz"
        versions = []
        for f in mode_dir.glob(f"{prefix}_*{back}"):
            name = f.name
            if not name.startswith(front) or not name.endswith(back):
                continue
            safe = name[len(front):-len(back)]
            if not safe:
                continue
            label = safe.replace("_", ".")
            versions.append((label, safe))

        def vkey(item):
            parts = []
            for p in item[0].split("."):
                try:
                    parts.append(int(p))
                except ValueError:
                    parts.append(0)
            return tuple(parts)

        uniq = sorted(set(versions), key=vkey, reverse=True)
        return [{"label": label, "safe": safe} for label, safe in uniq]

    def _write_versions_manifest(self, mode_dir: Path, prefix: str, page: str) -> None:
        """Writes {prefix}_{page}_versions.json — a small uncompressed manifest the
        in-browser version switcher fetches at runtime. Regenerated every run so that
        even older, previously-generated pages pick up newly added versions."""
        versions = self._discover_versions(mode_dir, prefix, page)
        if not versions:
            return
        self._write_json(mode_dir, f"{prefix}_{page}_versions.json", versions)
        print(f"  [DONE] {prefix}_{page}_versions.json ({len(versions)} version(s))")

    def render_file(self, template_name: str, out_path: Path, context: dict):
        template = self.env.get_template(template_name)
        html = template.render(**context)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(html, encoding="utf-8")
        print(f"  [DONE] {out_path.name} ({out_path.stat().st_size / 1024:.0f} KB)")

    def generate_mode(self, mode_key: str, version: str):
        cfg = self.MODE_CONFIG[mode_key]
        print(f"\n[INFO] Processing {cfg['full_name']} ({version})...")

        safe_version = version.replace(".", "_")
        mode_dir = self.output_base / cfg["folder"]
        
        base_context = {
            "version": version,
            "path_prefix": "../",
            "mode_label": cfg["mode_label"],
            "subtitle": cfg["subtitle"],
            "dim_field": cfg["dim_field"],
            "dim_label": cfg["dim_label"],
            "dim_values": cfg["dim_values"],
            "dim_group_labels_json": json.dumps(cfg["dim_group_labels"]),
            "icons_json": json.dumps(self.icons, ensure_ascii=False),
            "safe_version": safe_version,
            "file_prefix": cfg["file_prefix"],
        }
        
        # Register the lambda function for Jinja context dynamically
        self.env.globals["dim_btn_label"] = cfg["dim_btn_label"]

        # 1. Characters
        if self._has_data(self.CHAR_TABLE, version, "mode", cfg["char_db_mode"]):
            data = self.fetch_characters(cfg, version)
            out_file = mode_dir / f"{cfg['file_prefix']}_{safe_version}_characters.html"
            filename = f"{cfg['file_prefix']}_{safe_version}_characters_data.json.gz"
            self._write_gz_json(mode_dir, filename, data)
            self.render_file("character_stats_template.html.j2", out_file, {
                **base_context,
                "is_legacy": cfg["is_legacy"],
                "data_filename": filename,
                "page_suffix": "characters"
            })
        else:
            print(f"  [SKIP] No character data found for {cfg['char_db_mode']}.")
        self._write_versions_manifest(mode_dir, cfg["file_prefix"], "characters")

        # 2. Archetypes
        if self._has_data(cfg["arch_table"], version):
            data = self.fetch_archetypes(cfg, version)
            out_file = mode_dir / f"{cfg['file_prefix']}_{safe_version}_archetypes.html"
            filename = f"{cfg['file_prefix']}_{safe_version}_archetypes_data.json.gz"
            self._write_gz_json(mode_dir, filename, data)
            self.render_file("archetypes_template.html.j2", out_file, {
                **base_context,
                "data_filename": filename,
                "page_suffix": "archetypes"
            })
        else:
            print(f"  [SKIP] No archetype data found in {cfg['arch_table']}.")
        self._write_versions_manifest(mode_dir, cfg["file_prefix"], "archetypes")

        # 3. Teams
        if self._has_data(cfg["team_table"], version):
            data = self.fetch_teams(cfg, version)
            out_file = mode_dir / f"{cfg['file_prefix']}_{safe_version}_teams.html"
            filename = f"{cfg['file_prefix']}_{safe_version}_teams_data.json.gz"
            self._write_gz_json(mode_dir, filename, data)
            self.render_file("teams_template.html.j2", out_file, {
                **base_context,
                "data_filename": filename,
                "page_suffix": "teams"
            })
        else:
            print(f"  [SKIP] No team data found in {cfg['team_table']}.")
        self._write_versions_manifest(mode_dir, cfg["file_prefix"], "teams")

        # 4. Duos
        duo_table = cfg.get("duo_table")
        if duo_table and self._has_data(duo_table, version):
            data = self.fetch_duos(cfg, version)
            out_file = mode_dir / f"{cfg['file_prefix']}_{safe_version}_duos.html"
            filename = f"{cfg['file_prefix']}_{safe_version}_duos_data.json.gz"
            self._write_gz_json(mode_dir, filename, data)
            self.render_file("duos_template.html.j2", out_file, {
                **base_context,
                "data_filename": filename,
                "page_suffix": "duos"
            })
        else:
            print(f"  [SKIP] No duo data found for {mode_key}.")
        if duo_table:
            self._write_versions_manifest(mode_dir, cfg["file_prefix"], "duos")

        cost_table = cfg.get("cost_team_table")
        if cost_table and self._has_data(cost_table, version):
            data = self.fetch_cost_teams(cfg, version)
            out_file = mode_dir / f"{cfg['file_prefix']}_{safe_version}_by_cost_teams.html"
            
            # 1. Update extension to .json.gz
            filename = f"{cfg['file_prefix']}_{safe_version}_by_cost_teams_data.json.gz"
            
            # 2. Write the file locally using gzip compression
            self._write_gz_json(mode_dir, filename, data)
                
            # 3. Pass just the filename to your Jinja template context
            self.render_file("by_cost_teams_template.html.j2", out_file, {
                **base_context,
                "data_filename": filename,  # Pass it as a clean template variable
                "page_suffix": "by_cost_teams"
            })
        else:
            print(f"   [SKIP] No by-cost team data found for {mode_key}.")
        if cost_table:
            self._write_versions_manifest(mode_dir, cfg["file_prefix"], "by_cost_teams")

    def generate_index(self, version: str):
        print(f"\n[INFO] Generating Hub Index for {version}...")
        safe_version = version.replace(".", "_")
        out_file = self.output_base / "index.html"
        self.render_file("index_template.html.j2", out_file, {
            "version": version,
            "path_prefix": "./",
            "version_safe": safe_version
        })

    CHARACTERS_JSON = "characters.json"
    CHARACTERS_TEMPLATE = "characters_index_template.html.j2"
    DASHBOARD_SUFFIX = "_Dashboard.html"

    def generate_characters_index(self, version: str, characters_json_path: Path = None):
        """Rebuilds docs/characters/index.html (the Character Database browser)
        from characters.json + character_icons.json + whatever
        *_Dashboard.html files currently exist in docs/characters/.
        Safe to call even if no character sheets have been generated yet.
        """
        print(f"\n[INFO] Generating Character Database for {version}...")
        characters_dir = self.output_base / "characters"
        if not characters_dir.exists():
            print(f"  [SKIP] {characters_dir} does not exist yet.")
            return

        meta_path = characters_json_path or Path(self.CHARACTERS_JSON)
        if not meta_path.exists():
            print(f"  [WARN] {meta_path} not found — character badges will be skipped.")
            meta = {}
        else:
            with open(meta_path, encoding="utf-8") as f:
                meta = json.load(f)

        names = sorted(
            f.name[: -len(self.DASHBOARD_SUFFIX)]
            for f in characters_dir.glob(f"*{self.DASHBOARD_SUFFIX}")
        )
        if not names:
            print(f"  [SKIP] No *{self.DASHBOARD_SUFFIX} files found in {characters_dir}.")
            return

        records, missing_meta, missing_icon = [], [], []
        for name in names:
            m = meta.get(name)
            icon = self.icons.get(name)
            if m is None:
                missing_meta.append(name)
            if icon is None:
                missing_icon.append(name)
            records.append({
                "name": name,
                "file": f"{name}{self.DASHBOARD_SUFFIX}",
                "icon": icon,
                "path": m.get("path") if m else None,
                "element": m.get("element") if m else None,
                "rarity": m.get("rarity") if m else None,
                "role": m.get("role") if m else [],
                "release_phase": m.get("release_phase") if m else None,
                "id": m.get("id") if m else None,
            })

        if missing_meta:
            print(f"  [WARN] Missing characters.json entry for: {', '.join(missing_meta)}")
        if missing_icon:
            print(f"  [WARN] Missing character_icons.json entry for: {', '.join(missing_icon)}")

        records.sort(key=lambda r: r["name"])
        out_file = characters_dir / "index.html"
        self.render_file(self.CHARACTERS_TEMPLATE, out_file, {
            "version": version,
            "characters": records,
            "characters_json": json.dumps(records, ensure_ascii=False),
        })

    def close(self):
        self.conn.close()

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate comprehensive HSR dashboards.")
    parser.add_argument("--version", "-v", required=True, help="Target game version (e.g., 4.3.1)")
    parser.add_argument("--db", default=os.getenv("DB_File", "hsr.duckdb"), help="Path to DuckDB file")
    parser.add_argument("--icons", default="character_icons.json", help="Path to character icons JSON")
    parser.add_argument("--template-dir", default=str(Path(__file__).parent), help="Dir containing .j2 templates")
    parser.add_argument("--output", "-o", default="./docs", help="Root output directory (docs/)")
    
    args = parser.parse_args()

    generator = DashboardGenerator(
        db_path=Path(args.db),
        icons_path=Path(args.icons),
        template_dir=Path(args.template_dir),
        output_base=Path(args.output)
    )

    try:
        # Generate all modes iteratively
        for mode in generator.MODE_CONFIG.keys():
            generator.generate_mode(mode, args.version)
        
        # Generate the root routing hub
        generator.generate_index(args.version)

        # Generate the Character Database browser (docs/characters/index.html)
        generator.generate_characters_index(args.version)

        print("\n[SUCCESS] Pipeline complete.")
    finally:
        generator.close()

if __name__ == "__main__":
    main()