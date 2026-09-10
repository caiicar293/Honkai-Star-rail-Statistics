"""
frontend_orchestrator.py
------------------------
Orchestrates the frontend (docs/) regeneration pipeline documented in
maintainence.md:

    1. delete/replace data in the docs for moc, pure, anomaly and apoc
    2. Generate Dashboards
    3. character dashboards orchestrator
    4. network export - generate network graphs
    5. by_cost_archetype_tier_list_generator
    6. archetype_tier_list_e0_generator.py (manually add new entries)
    7. database_trends_export then generate_trends_dashboard

Step 1 honors the per-mode strategy keywords in .env:

    MOC= replace
    Pure_fiction = replace
    anomaly = add
    apoc = add

A mode set to 'replace' has its NEWEST generated version removed from
docs/ so the following steps regenerate it fresh from the database.
Modes set to 'add' (or any non-'replace' value) are left untouched.
That mirrors the manual workflow: when a mode is on 'replace', you delete
that gamemode's most recent version, and the rest of the pipeline rebuilds
it from the (already-updated) DuckDB file.

The pipeline lives in the FrontendOrchestrator class so it can be driven
both from the CLI and programmatically:

    from frontend_orchestrator import FrontendOrchestrator
    orch = FrontendOrchestrator(version="4.5.1", dry_run=True)
    orch.run_pipeline()

CLI usage:
    python frontend_orchestrator.py
    python frontend_orchestrator.py --version 4.5.1
    python frontend_orchestrator.py --dry-run
    python frontend_orchestrator.py --skip-e0 --no-delete
"""

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).resolve().parent
DOCS = ROOT / "docs"

# (env strategy key, docs subfolder, file prefix)
MODE_SLOTS = [
    ("MOC", "moc", "moc"),
    ("Pure_fiction", "pure_fiction", "pure_fiction"),
    ("anomaly", "anomaly_arbitration", "anomaly"),
    ("apoc", "apoc", "apoc"),
]

# .env version lists that track the modern (non-legacy) versions per mode.
# Used to derive the newest version to feed Generate_Dashboards.
VERSION_ENV_KEYS = ["MOC_VERSIONS", "PF_VERSIONS", "APOC_VERSIONS", "ANOMALY_VERSIONS"]

# Every generated version produces at least a {prefix}_{safe}_characters page
# (both the .html and the _data.json.br), so it's a reliable version scanner.
_DATA_SUFFIX = "_characters_data.json.br"


class FrontendOrchestrator:
    """Runs the frontend (docs/) regeneration pipeline as ordered steps.

    Parameters
    ----------
    version : str | None
        Game version fed to Generate_Dashboards / e0 tier list. Defaults to
        the newest version across the mode version lists in .env.
    dry_run : bool
        Print the pipeline steps without running anything (step 1 included).
    no_delete : bool
        Skip step 1 (delete/replace of the newest version for modes set to
        'replace' in .env).
    skip_e0 : bool
        Skip archetype_tier_list_e0_generator.py (maintainence.md notes new
        entries are added manually).
    root : str | Path | None
        Project root; defaults to this file's directory. Overridable so tests
        can point the pipeline at a temp docs/ tree.
    runner : callable | None
        Callable invoked as ``runner(cmd)`` for each pipeline step instead of
        the default ``subprocess.run([sys.executable, *cmd])``. Tests inject a
        recorder here; ``dry_run`` short-circuits before the runner is called.
    """

    def __init__(
        self,
        version: str | None = None,
        dry_run: bool = False,
        no_delete: bool = False,
        skip_e0: bool = False,
        root: str | Path | None = None,
        runner=None,
    ) -> None:
        self.root = Path(root) if root is not None else ROOT
        self.docs = self.root / "docs"
        self.dry_run = dry_run
        self.no_delete = no_delete
        self.skip_e0 = skip_e0
        self.runner = runner if runner is not None else self._subprocess_run
        self.version = version if version is not None else self.newest_version()

    # ------------------------------------------------------------------ utils

    @staticmethod
    def _vkey(token: str) -> tuple:
        """'4_5_1' (or '4.5.1') -> (4, 5, 1) for numeric ordering."""
        return tuple(int(p) for p in re.split(r"[._]", token) if p.isdigit())

    def _newest_safe_version(self, folder: Path, prefix: str) -> str | None:
        """Newest generated version (safe form, e.g. '4_5_1') in a mode folder."""
        if not folder.exists():
            return None
        front = f"{prefix}_"
        versions = []
        for f in folder.glob(f"{prefix}_*_characters_data.json.br"):
            name = f.name
            if not (name.startswith(front) and name.endswith(_DATA_SUFFIX)):
                continue
            safe = name[len(front):-len(_DATA_SUFFIX)]
            if safe and all(p.isdigit() for p in safe.split("_")):
                versions.append(safe)
        if not versions:
            return None
        return max(versions, key=self._vkey)

    # ------------------------------------------------------------------- step 1

    def replace_newest_versions(self) -> None:
        """Delete the newest generated version for every mode whose .env
        strategy keyword is 'replace'."""
        for env_key, sub, prefix in MODE_SLOTS:
            strategy = (os.getenv(env_key) or "").strip().lower()
            if strategy != "replace":
                print(f"[1] {env_key}: strategy={strategy!r} -> leaving docs/{sub} untouched")
                continue

            folder = self.docs / sub
            safe = self._newest_safe_version(folder, prefix)
            if safe is None:
                print(f"[1] {env_key}: no generated versions found in docs/{sub} -> nothing to delete")
                continue

            label = safe.replace("_", ".")
            targets = [f for f in folder.glob(f"{prefix}_{safe}_*") if f.is_file()]
            if self.dry_run:
                print(f"[dry-run] {env_key}: would remove {len(targets)} file(s) for v{label} "
                      f"from docs/{sub}")
                continue

            for f in targets:
                f.unlink()

            print(f"[1] {env_key}: removed {len(targets)} file(s) for v{label} from docs/{sub}")
            for f in sorted(targets):
                print(f"      - {f.name}")

    # ------------------------------------------------------------- version logic

    def newest_version(self) -> str:
        """Newest version across the modern version lists in .env."""
        best = None
        for key in VERSION_ENV_KEYS:
            raw = os.getenv(key) or ""
            for v in raw.split(","):
                v = v.strip()
                if not v:
                    continue
                if best is None or self._vkey(v) > self._vkey(best):
                    best = v
        if best is None:
            raise SystemExit(
                "[ERROR] No version lists found in .env "
                "(expected MOC_VERSIONS / PF_VERSIONS / APOC_VERSIONS / ANOMALY_VERSIONS)."
            )
        return best

    # ------------------------------------------------------------- step runners

    def _subprocess_run(self, cmd: list[str]) -> None:
        """Default runner: execute ``cmd`` with this interpreter, fail fast."""
        proc = subprocess.run([sys.executable, *cmd], cwd=str(self.root))
        if proc.returncode != 0:
            raise SystemExit(f"[ERROR] step failed (exit {proc.returncode}): python {' '.join(cmd)}")

    def run(self, cmd: list[str]) -> None:
        """Run one pipeline step (respecting dry_run), then the configured runner."""
        label = " ".join(cmd)
        if self.dry_run:
            print(f"[dry-run] python {label}")
            return
        print(f"\n>>> python {label}")
        self.runner(cmd)

    # ----------------------------------------------------------------- pipeline

    def run_pipeline(self) -> None:
        """Execute all 7 steps in order. Steps fail fast (SystemExit on error)."""
        print("=" * 70)
        print(f"Frontend regeneration orchestrator  (version {self.version})")
        print("=" * 70)

        # ---------------------------------------------------------------- 1
        if self.no_delete:
            print("[1/7] delete/replace data in docs/ -- SKIPPED (--no-delete)")
        else:
            print("[1/7] delete/replace data in docs/ for moc, pure, anomaly and apoc")
            self.replace_newest_versions()

        # ---------------------------------------------------------------- 2
        print("[2/7] Generate Dashboards")
        self.run(["Generate_Dashboards.py", "--version", self.version])

        # ---------------------------------------------------------------- 3
        print("[3/7] character dashboards orchestrator")
        self.run(["character_dashboards_orchestrator.py"])

        # ---------------------------------------------------------------- 4
        print("[4/7] network export - generate network graphs")
        self.run(["network_export.py"])
        self.run(["generate_network_dashboard.py"])

        # ---------------------------------------------------------------- 5
        print("[5/7] by_cost_archetype_tier_list_generator")
        self.run(["by_cost_archetype_tier_list_generator.py"])

        # ---------------------------------------------------------------- 6
        if self.skip_e0:
            print("[6/7] archetype_tier_list_e0_generator.py -- SKIPPED (--skip-e0)")
        else:
            print("[6/7] archetype_tier_list_e0_generator.py "
                  "(remember: manually add new entries first)")
            self.run(["archetype_tier_list_e0_generator.py", "--version", self.version])

        # ---------------------------------------------------------------- 7
        print("[7/7] database_trends export then generate_trends")
        self.run(["database_trends_export.py"])
        self.run(["generate_trends_dashboard.py"])

        print("\n[DONE] Frontend regeneration complete.")

    # -------------------------------------------------------------------- CLI

    @classmethod
    def from_cli(cls, argv: list[str] | None = None) -> "FrontendOrchestrator":
        """Parse CLI args, build the orchestrator and run the pipeline."""
        parser = argparse.ArgumentParser(
            description="Regenerate the frontend (docs/) pipeline from maintainence.md.",
        )
        parser.add_argument(
            "--version",
            help="Game version fed to Generate_Dashboards / e0 tier list "
                 "(default: newest across the mode version lists in .env)",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Print the pipeline steps without running anything",
        )
        parser.add_argument(
            "--no-delete",
            action="store_true",
            help="Skip step 1 (delete/replace of the newest version for modes set "
                 "to 'replace' in .env)",
        )
        parser.add_argument(
            "--skip-e0",
            action="store_true",
            help="Skip archetype_tier_list_e0_generator.py (maintainence.md notes "
                 "new entries are added manually)",
        )
        args = parser.parse_args(argv)

        orch = cls(
            version=args.version,
            dry_run=args.dry_run,
            no_delete=args.no_delete,
            skip_e0=args.skip_e0,
        )
        orch.run_pipeline()
        return orch


if __name__ == "__main__":
    from frontend_orchestrator import FrontendOrchestrator

    FrontendOrchestrator.from_cli()