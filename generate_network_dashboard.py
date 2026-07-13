"""
generate_network_dashboard.py
------------------------------
Renders network_dashboard_svg.html.j2 to docs/network/index.html.

Unlike the other pages (Generate_Dashboards.py), this page has no
per-version data baked in at render time — mode/eidolon-range/recency
selection and the actual slice data are all resolved client-side from
docs/network/network_manifest.json (written by network_export.py).

What this generator DOES inject at render time, same pattern as
DashboardGenerator in Generate_Dashboards.py:
    - icons_json  — character_icons.json loaded and dumped, so the page's
                    ICONS lookup table (see lookupIconUrl() in the template)
                    can render character portraits on each node instead of
                    a plain colored circle.
    - path_prefix — "../" since docs/network/index.html sits one directory
                    below docs/, same depth as docs/moc/*.html etc., so
                    relative icon paths like "assets/icons/foo.webp"
                    resolve correctly from here.

Usage:
    python generate_network_dashboard.py
    python generate_network_dashboard.py --template-dir . --output docs/network/index.html --icons character_icons.json

Requires docs/network/network_manifest.json and its *.json.br slices to
already exist (run network_export.py first if they don't).
"""

import argparse
import json
from pathlib import Path
from jinja2 import Environment, FileSystemLoader, select_autoescape


def _load_icons(icons_path: Path) -> dict:
    if not icons_path.exists():
        print(f"[WARN] Icons file not found at {icons_path} — nodes will render as "
              f"plain colored circles instead of character portraits.")
        return {}
    with open(icons_path, encoding="utf-8") as f:
        return json.load(f)


def generate_network_dashboard(
    template_dir: Path,
    output_path: Path,
    manifest_path: Path = None,
    icons_path: Path = None,
    path_prefix: str = "../",
):
    if not template_dir.exists():
        raise SystemExit(f"[ERROR] Template directory not found: {template_dir}")

    manifest_path = manifest_path or (output_path.parent / "network_manifest.json")
    if not manifest_path.exists():
        print(f"[WARN] {manifest_path} not found yet — the page will render, "
              f"but will show 'Failed to load network manifest' until you run "
              f"network_export.py.")

    icons_path = icons_path or Path("character_icons.json")
    icons = _load_icons(icons_path)

    env = Environment(
        loader=FileSystemLoader(str(template_dir)),
        autoescape=select_autoescape(disabled_extensions=["j2", "html"]),
    )
    template = env.get_template("network_dashboard_svg.html.j2")
    html = template.render(
        icons_json=json.dumps(icons, ensure_ascii=False),
        path_prefix=path_prefix,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    print(f"[DONE] {output_path} ({output_path.stat().st_size / 1024:.0f} KB, "
          f"{len(icons)} icons injected)")


def main():
    parser = argparse.ArgumentParser(description="Render the network graph dashboard page.")
    parser.add_argument("--template-dir", default=".", help="Dir containing network_dashboard_svg.html.j2")
    parser.add_argument("--output", "-o", default="docs/network/index.html", help="Output HTML path")
    parser.add_argument("--icons", default="character_icons.json", help="Path to character icons JSON")
    parser.add_argument("--path-prefix", default="../", help="Relative prefix from output page to docs/ root")
    args = parser.parse_args()

    generate_network_dashboard(
        template_dir=Path(args.template_dir),
        output_path=Path(args.output),
        icons_path=Path(args.icons),
        path_prefix=args.path_prefix,
    )


if __name__ == "__main__":
    main()
