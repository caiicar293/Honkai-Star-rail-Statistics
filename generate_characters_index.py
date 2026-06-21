"""
generate_characters_index.py
-------------------------------
Builds docs/characters/index.html — the searchable/filterable Character
Database hub — from three sources:

    1. characters.json         path / element / rarity / role / release_phase
    2. character_icons.json    relative icon path (assets/icons/x.webp)
    3. docs/characters/*.html  the actual "{Name}_Dashboard.html" files

This keeps the browse page self-maintaining. When a new character's
dashboard is added to the site, you do NOT hand-edit index.html or its
embedded JSON. Instead:

    1. Add a "{Name}" entry to characters.json
       (rarity, path, element, role, release_phase, id)
    2. Add a "{Name}": "assets/icons/{slug}.webp" entry to character_icons.json
       and drop the matching .webp into docs/assets/icons/
    3. Generate (or copy) the dashboard file itself to
       docs/characters/{Name}_Dashboard.html
    4. Run this script:
           python generate_characters_index.py --version 4.3.2

The script scans docs/characters/ for every "*_Dashboard.html" file,
matches each one by name against characters.json + character_icons.json,
and re-renders the page. If a new character is missing from either JSON
file, it still gets a card (just without path/element badges or an icon)
and a [WARN] is printed so nothing is silently incomplete.

Usage:
    python generate_characters_index.py --version 4.3.1
    python generate_characters_index.py --version 4.3.1 --characters-dir "docs/characters"

Dependencies:
    pip install jinja2
"""

import argparse
import json
import sys
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

DASHBOARD_SUFFIX = "_Dashboard.html"
TEMPLATE_NAME = "characters_index_template.html.j2"


def load_json(path: Path) -> dict:
    if not path.exists():
        sys.exit(f"[ERROR] Required file not found: {path}")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def discover_characters(characters_dir: Path) -> list[str]:
    """Return character names derived from every *_Dashboard.html file present."""
    if not characters_dir.exists():
        sys.exit(f"[ERROR] Characters directory not found: {characters_dir}")
    names = [f.name[: -len(DASHBOARD_SUFFIX)] for f in characters_dir.glob(f"*{DASHBOARD_SUFFIX}")]
    return sorted(names)


def build_records(names: list[str], meta: dict, icons: dict) -> list[dict]:
    records = []
    missing_meta, missing_icon = [], []

    for name in names:
        m = meta.get(name)
        icon = icons.get(name)
        if m is None:
            missing_meta.append(name)
        if icon is None:
            missing_icon.append(name)

        records.append({
            "name": name,
            "file": f"{name}{DASHBOARD_SUFFIX}",
            "icon": icon,
            "path": m.get("path") if m else None,
            "element": m.get("element") if m else None,
            "rarity": m.get("rarity") if m else None,
            "role": m.get("role") if m else [],
            "release_phase": m.get("release_phase") if m else None,
            "id": m.get("id") if m else None,
        })

    if missing_meta:
        print(f"[WARN] {len(missing_meta)} character(s) missing from characters.json "
              f"(no path/element/rarity badges will show):")
        for n in missing_meta:
            print(f"         - {n}")
    if missing_icon:
        print(f"[WARN] {len(missing_icon)} character(s) missing from character_icons.json "
              f"(no icon will show):")
        for n in missing_icon:
            print(f"         - {n}")

    return sorted(records, key=lambda r: r["name"])


def render(records: list[dict], version: str, template_dir: Path, output_file: Path):
    if not (template_dir / TEMPLATE_NAME).exists():
        sys.exit(f"[ERROR] Template not found: {template_dir / TEMPLATE_NAME}")

    env = Environment(
        loader=FileSystemLoader(str(template_dir)),
        autoescape=select_autoescape(disabled_extensions=["j2", "html"]),
    )
    template = env.get_template(TEMPLATE_NAME)
    html = template.render(
        version=version,
        characters=records,
        characters_json=json.dumps(records, ensure_ascii=False),
    )
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(html, encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(
        description="Generate docs/characters/index.html from characters.json + icons + existing dashboards.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--version", "-v", required=True, help="Game version label shown on the page, e.g. 4.3.1")
    parser.add_argument("--characters-json", default="characters.json", help="Path to characters.json")
    parser.add_argument("--icons-json", default="character_icons.json", help="Path to character_icons.json")
    parser.add_argument("--characters-dir", default="docs/characters", help="Folder containing *_Dashboard.html files")
    parser.add_argument("--template-dir", default=str(Path(__file__).parent), help="Folder containing characters_index_template.html.j2")
    parser.add_argument("--output", "-o", default=None, help="Output file (default: <characters-dir>/index.html)")
    args = parser.parse_args()

    characters_dir = Path(args.characters_dir)
    output_file = Path(args.output) if args.output else characters_dir / "index.html"

    meta = load_json(Path(args.characters_json))
    icons = load_json(Path(args.icons_json))
    names = discover_characters(characters_dir)

    if not names:
        sys.exit(f"[ERROR] No *{DASHBOARD_SUFFIX} files found in {characters_dir}")

    print(f"[INFO] Found {len(names)} dashboard file(s) in {characters_dir}")

    records = build_records(names, meta, icons)
    render(records, args.version, Path(args.template_dir), output_file)

    print(f"[DONE] {output_file}  ({len(records)} characters, {output_file.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
