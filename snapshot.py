"""
snapshot.py — cheap file-level backups of the DuckDB database.

Why: database_batch.py autocommits every INSERT, and DuckDB has no native
point-in-time recovery. The only way to undo a bad run is to restore a copy
of the file taken *before* the run. This script makes that copy safe:

  1. CHECKPOINT merges the write-ahead log (WAL) into the main .duckdb file,
     so a plain file copy doesn't silently drop the most recent committed rows.
  2. The file is copied to backups/<name>.<YYYY-MM-DD_HHMMSS>.
  3. Old snapshots are pruned, keeping the newest `--keep` (default 5).

database_batch.py calls snapshot() automatically at the start of
incremental_update(), so every incremental run leaves a restore point.

Usage:
    python snapshot.py                       # snapshot DB_File from .env
    python snapshot.py --restore <file>      # copy a snapshot back over DB_File
    python snapshot.py --keep 10             # retain 10 snapshots instead of 5
"""

import argparse
import datetime
import os
import shutil
import sys
from pathlib import Path

import duckdb
from dotenv import load_dotenv

load_dotenv()

DEFAULT_KEEP = 5
DUCK_MAGIC = b"DUCK"
# Current DuckDB formats put the magic at byte offset 8 (after an 8-byte
# random salt); older formats placed it at offset 0. Check both.
DUCK_MAGIC_OFFSETS = (0, 8)


def db_path():
    """Resolve DB_File from .env (relative paths resolve against CWD, matching
    how database_batch.py uses it)."""
    p = os.getenv("DB_File")
    if not p:
        raise SystemExit("DB_File not set in .env — nothing to snapshot/restore.")
    return Path(p)


def _backup_dir():
    return Path("backups")


def snapshot(db_file=None, keep=DEFAULT_KEEP):
    """Checkpoint the DB, copy it to backups/, and prune old snapshots.

    Returns the backup path, or None if there was nothing to snapshot.
    Safe to call even when the DB doesn't exist yet (fresh setup).
    """
    src = Path(db_file) if db_file else db_path()
    if not src.exists():
        print(f"  [snapshot] no DB yet at {src} — skipping")
        return None

    # Flush the WAL into the main file so the copy is complete and consistent.
    try:
        conn = duckdb.connect(str(src))
        try:
            conn.execute("CHECKPOINT")
        finally:
            conn.close()
    except Exception as ex:
        print(f"  [snapshot] could not checkpoint {src}: {ex}")
        return None

    bak_dir = _backup_dir()
    bak_dir.mkdir(exist_ok=True)
    stamp = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    dest = bak_dir / f"{src.name}.{stamp}"
    shutil.copy2(src, dest)

    # Belt-and-suspenders: copy any leftover WAL alongside the snapshot too.
    for wal in sorted(Path(".").glob(str(src) + ".wal*")):
        shutil.copy2(wal, bak_dir / f"{dest.name}{wal.suffix}")

    pruned = _prune(bak_dir, src.name, keep)
    note = f"  (pruned {len(pruned)} old)" if pruned else ""
    print(f"  [snapshot] -> {dest}{note}")
    return dest


def restore(backup_file, db_file=None):
    """Copy a snapshot back over the live DB file, dropping any stale WAL.

    The stale-WAL step matters: after CHECKPOINT the live DB can still carry a
    .wal with newer (unwanted) transactions. If left in place, DuckDB would
    replay it on next open and resurrect the data we just rolled back.
    """
    src = Path(backup_file)
    dest = Path(db_file) if db_file else db_path()

    if not src.exists():
        raise SystemExit(f"snapshot not found: {src}")
    with open(src, "rb") as fh:
        head = fh.read(16)
    if not any(head[off:off + 4] == DUCK_MAGIC for off in DUCK_MAGIC_OFFSETS):
        raise SystemExit(f"{src} doesn't look like a duckdb file — refusing to restore.")

    # Sanity: can we actually open the snapshot?
    try:
        with duckdb.connect(str(src), read_only=True) as c:
            tables = [r[0] for r in c.execute("SHOW TABLES").fetchall()]
    except Exception as ex:
        raise SystemExit(f"snapshot {src} can't be opened as a duckdb file: {ex}")

    shutil.copy2(src, dest)
    removed_wal = 0
    for wal in sorted(Path(".").glob(str(dest) + ".wal*")):
        wal.unlink()
        removed_wal += 1

    print(f"  [restore] {dest} <- {src}  ({len(tables)} tables"
          + (f", removed {removed_wal} stale wal" if removed_wal else "") + ")")
    return dest


def _prune(bak_dir, name, keep):
    """Delete the oldest snapshots of `name` beyond the newest `keep`.

    Only main snapshot files are counted; each pruned snapshot also removes
    its sidecar .wal copy (named <snapshot>.wal).
    """
    snaps = sorted(
        p for p in bak_dir.glob(f"{name}.*") if not p.name.endswith(".wal")
    )
    removed = []
    for old in snaps[:-keep]:
        old.unlink()
        for wal in bak_dir.glob(old.name + ".wal*"):
            wal.unlink()
        removed.append(old.name)
    return removed


def main():
    parser = argparse.ArgumentParser(
        description="Snapshot or restore the DuckDB database (DB_File from .env)."
    )
    parser.add_argument("--restore", metavar="FILE",
                        help="restore the live DB from this snapshot file")
    parser.add_argument("--keep", type=int, default=DEFAULT_KEEP,
                        help="how many snapshots to retain (default %(default)s)")
    parser.add_argument("--db", default=None,
                        help="override DB_File from .env")
    args = parser.parse_args()

    if args.restore:
        restore(args.restore, args.db)
    else:
        snapshot(args.db, keep=args.keep)


if __name__ == "__main__":
    main()