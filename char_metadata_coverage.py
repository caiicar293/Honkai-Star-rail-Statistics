"""Coverage rule for the characters.json metadata join.

`character_stats` is built by joining the per-character rows onto
characters.json with `how="left"` (see `_standardize(..., is_char=True)` in
database_main.py / database_batch.py). That join does NOT fail when it has
nothing to match: role, availability, element, path and release_phase simply
come back NULL for every row of that character, so a name typo or a brand new
character is invisible until something downstream groups by role.

Keeping the rule here means database_main.py, database_batch.py and the test
suite all agree on what counts as "missing". Stdlib only -- deliberately no
polars/duckdb, so it stays importable from anywhere.
"""

import json

# Intentional filler for empty team slots (written by the pipelines, and
# expected to have no characters.json entry).
PLACEHOLDER_CHARACTERS = frozenset({"Empty Slot"})


def load_character_names(path="characters.json"):
    """The names defined in characters.json (the join's right-hand side)."""
    with open(path, encoding="utf-8") as f:
        return set(json.load(f))


def missing_char_metadata(known_names, data_names, ignore=PLACEHOLDER_CHARACTERS):
    """Dataset character names that have no characters.json entry.

    known_names : names defined in characters.json
    data_names  : names seen in the dataset's Character column
    ignore      : names that are expected to be absent (see PLACEHOLDER_CHARACTERS)

    Returns a sorted list. Blanks and non-strings are skipped -- NULL slots and
    filler values are not something you can add metadata for.
    """
    known = {n for n in known_names if isinstance(n, str)}
    missing = {
        n for n in data_names
        if isinstance(n, str) and n.strip() and n not in known and n not in ignore
    }
    return sorted(missing)


def describe_missing_char_metadata(missing):
    """Warning text for a non-empty list from missing_char_metadata()."""
    names = ", ".join(missing)
    return (
        f"  [WARN] {len(missing)} character name(s) appear in the dataset but have no "
        f"characters.json entry -- role/availability/element/path/release_phase will be "
        f"NULL for every row of: {names}"
    )
