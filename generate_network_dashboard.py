"""
generate_network_dashboard.py
------------------------------
Renders network_dashboard_svg.html.j2 to docs/network/index.html.

Unlike the other pages (Generate_Dashboards.py), this page has no
per-version data baked in at render time — mode/eidolon-range/recency
selection and the actual slice data are all resolved client-side from
docs/network/network_manifest.json (written by network_export.py).
So this generator's job is just: render the template once, same as
DashboardGenerator.render_file() does for every other page.

Usage:
    python generate_network_dashboard.py
    python generate_network_dashboard.py --template-dir . --output docs/network/index.html

Requires docs/network/network_manifest.json and its *.json.br slices to
already exist (run network_export.py first if they don't).
"""

import argparse
from pathlib import Path
from jinja2 import Environment, FileSystemLoader, select_autoescape


def generate_network_dashboard(template_dir: Path, output_path: Path, manifest_path: Path = None):
    if not template_dir.exists():
        raise SystemExit(f"[ERROR] Template directory not found: {template_dir}")

    manifest_path = manifest_path or (output_path.parent / "network_manifest.json")
    if not manifest_path.exists():
        print(f"[WARN] {manifest_path} not found yet — the page will render, "
              f"but will show 'Failed to load network manifest' until you run "
              f"network_export.py.")

    env = Environment(
        loader=FileSystemLoader(str(template_dir)),
        autoescape=select_autoescape(disabled_extensions=["j2", "html"]),
    )
    template = env.get_template("network_dashboard_svg.html.j2")
    html = template.render()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    print(f"[DONE] {output_path} ({output_path.stat().st_size / 1024:.0f} KB)")


def main():
    parser = argparse.ArgumentParser(description="Render the network graph dashboard page.")
    parser.add_argument("--template-dir", default=".", help="Dir containing network_dashboard_svg.html.j2")
    parser.add_argument("--output", "-o", default="docs/network/index.html", help="Output HTML path")
    args = parser.parse_args()

    generate_network_dashboard(
        template_dir=Path(args.template_dir),
        output_path=Path(args.output),
    )


if __name__ == "__main__":
    main()
