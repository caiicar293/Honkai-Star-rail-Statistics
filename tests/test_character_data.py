"""Validation tests for the two hand-maintained character data files.

    characters.json   -> DB-critical. Joined into `character_stats` by
                         database_main.py / database_batch.py (role,
                         availability, element, path, release_phase) and read
                         by every Appearance_rate* script and the archetype
                         calculations. A bad value here silently corrupts
                         aggregates, so the checks are strict.
    char_config.json  -> dashboard-only. Drives character_dashboards_orchestrator
                         (theme colours, stat panels, which pages exist).

These files are edited by hand, so the point of this suite is to catch the
mistakes hand-editing produces: a role typed as one comma-separated string
instead of a list, a misspelled path, a non-ASCII or camelCase slug, a name
that no longer matches the other file, a theme colour that isn't a colour.

Run it:

    python -m unittest discover -s tests -t .
    python tests/test_character_data.py            # equivalent

No third-party dependencies -- stdlib unittest only. The module also runs
under pytest if you ever add it.

Every failure names the offending character and field, so you can fix the
JSON directly.
"""

import json
import re
import sys
import unicodedata
import unittest
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
# Works for `python tests/test_character_data.py` too (sys.path[0] is then tests/).
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from char_metadata_coverage import (  # noqa: E402  (needs ROOT on sys.path first)
    PLACEHOLDER_CHARACTERS,
    describe_missing_char_metadata,
    load_character_names,
    missing_char_metadata,
)
CHARACTERS_PATH = ROOT / "characters.json"
CHAR_CONFIG_PATH = ROOT / "char_config.json"
CHARACTER_ICONS_PATH = ROOT / "character_icons.json"
# The fill-in-the-blanks template used when a new character's theme is added.
# It carries the per-Element colour palette that char_config.json must follow.
CHARACTER_PROMPT_PATH = ROOT / "Dashboards" / "character_dashboard_prompt_v2.md"

# ---------------------------------------------------------------------------
# Vocabularies.
#
# These are deliberately hard-coded whitelists: they are what turns a typo
# into a failing test. When the game legitimately adds a new path / element /
# role, add it here in the same commit as the data.
# ---------------------------------------------------------------------------

# Consumed as a *list* everywhere (`'sustain' in v['role']`,
# `set(v['role']).intersection({'dps','specialist'})`), then joined with ", "
# into the DB `role` column. Roles are always lowercase.
ALLOWED_ROLES = ("dps", "specialist", "amplifier", "sustain")
ROLE_ORDER = {role: i for i, role in enumerate(ALLOWED_ROLES)}

ALLOWED_RARITIES = (4, 5)
ALLOWED_AVAILABILITIES = ("4*", "Standard 5*", "Limited 5*")

# characters.json speaks the dataset's (CSV) vocabulary.
CHARACTERS_PATHS = (
    "Abundance", "Destruction", "Elation", "Erudition", "Harmony", "Hunt",
    "Nihility", "Preservation", "Remembrance",
)
CHARACTERS_ELEMENTS = (
    "Fire", "Ice", "Imaginary", "Physical", "Quantum", "Thunder", "Wind",
)

# char_config.json speaks the out-of-game display vocabulary. It is only used
# for labels on the sheet (e.g. `f"{element} DMG"`), so "Lightning" is correct
# here even though the dataset calls the same element "Thunder".
CONFIG_ELEMENTS = (
    "Fire", "Ice", "Imaginary", "Lightning", "Physical", "Quantum", "Wind",
)
# char_config spelling -> characters.json spelling, for cross-file agreement.
ELEMENT_ALIASES = {"lightning": "thunder"}

# Stat panel vocabulary (character_dashboard_generator._generate_stat_cells_html
# special-cases "DMG Boost" into "<element> DMG").
STAT_NAMES = (
    "ATK", "HP", "DEF", "SPD",
    "CRIT Rate", "CRIT DMG", "DMG Boost",
    "Break Effect", "Effect Hit Rate", "Effect RES",
    "Energy Regeneration Rate", "Outgoing Healing Boost",
)

# Every sheet currently shows exactly these three, in this order.
SIDEBAR_METADATA = ("Total Samples", "Dataset Versions", "Latest Version")

REQUIRED_CHARACTER_FIELDS = (
    "id", "rarity", "path", "element", "availability", "slug",
    "release_phase", "role",
)
OPTIONAL_CHARACTER_FIELDS = ("trailblazer_ids",)

REQUIRED_CONFIG_FIELDS = (
    "character_name", "element", "path", "subtitle",
    "theme_c1", "theme_c2", "theme_c3", "theme_p1", "theme_p2", "theme_glow",
    "defining_stats", "supporting_stats", "sidebar_spread_stats",
    "sidebar_metadata",
)
# Overrides the generator honours when present (falls back to the DB / defaults).
OPTIONAL_CONFIG_FIELDS = ("role", "availability")

THEME_HEX_FIELDS = ("theme_c1", "theme_c2", "theme_c3", "theme_p1", "theme_p2")
# All six theme values, in the order the prompt's palette table lists them.
THEME_PALETTE_FIELDS = THEME_HEX_FIELDS + ("theme_glow",)

HEX_RE = re.compile(r"^#[0-9a-fA-F]{6}$")
RGBA_RE = re.compile(
    r"^rgba\(\s*(\d{1,3})\s*,\s*(\d{1,3})\s*,\s*(\d{1,3})\s*,\s*([0-9]*\.?[0-9]+)\s*\)$"
)
SLUG_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
RELEASE_PHASE_RE = re.compile(r"^\d+\.\d+\.\d+$")
ID_RE = re.compile(r"^\d{4}$")

# Separator used inside variant names, e.g. "Dan Heng • Imbibitor Lunae".
NAME_BULLET = "\u2022"


def _load(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _parse_element_theme_palette(path=CHARACTER_PROMPT_PATH):
    """Read the ``| Element | theme_c1 | ... | theme_glow |`` table from the prompt.

    Parsed rather than duplicated so the prompt stays the single source of
    truth: edit the palette there and this test tells you which characters now
    disagree, instead of the two silently drifting apart.

    Rows are keyed by the *header* column names, so reordering the columns
    keeps working while renaming, duplicating, or dropping one is a hard error
    rather than a silent mis-mapping. Returns ``{element: {field: value}}``,
    with inline backticks stripped.
    """
    palette = {}
    fields = None
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line.startswith("|"):
            continue
        cells = [c.strip().strip("`") for c in line.strip("|").split("|")]
        if fields is None:
            # Header row: | Element | theme_c1 | ... | theme_glow |
            if not cells or cells[0] != "Element":
                continue
            fields = tuple(cells[1:])
            if sorted(fields) != sorted(THEME_PALETTE_FIELDS):
                raise ValueError(
                    f"{Path(path).name}: expected the palette table columns "
                    f"{list(THEME_PALETTE_FIELDS)} after 'Element', found {list(fields)}"
                )
            continue
        element = cells[0]
        # Skips the |---| separator row and anything mis-shaped.
        if len(cells) != 1 + len(fields) or not element or set(element) <= set("-: "):
            continue
        palette[element] = dict(zip(fields, cells[1:]))
    return palette


# ---------------------------------------------------------------------------
# characters.json
# ---------------------------------------------------------------------------

class TestCharactersJson(unittest.TestCase):
    """DB-critical file: characters.json."""

    @classmethod
    def setUpClass(cls):
        cls.data = _load(CHARACTERS_PATH)

    def test_top_level_shape(self):
        self.assertIsInstance(self.data, dict, "characters.json must be a JSON object keyed by character name")
        self.assertTrue(self.data, "characters.json is empty")
        for name in self.data:
            self.assertIsInstance(name, str, f"non-string key in characters.json: {name!r}")
            self.assertTrue(name.strip(), "blank character name in characters.json")

    def test_fields_present_and_no_unknown_keys(self):
        known = set(REQUIRED_CHARACTER_FIELDS) | set(OPTIONAL_CHARACTER_FIELDS)
        for name, info in self.data.items():
            with self.subTest(character=name):
                self.assertIsInstance(info, dict, f"{name}: entry must be an object")
                missing = [k for k in REQUIRED_CHARACTER_FIELDS if k not in info]
                self.assertEqual(missing, [], f"{name}: missing required field(s) {missing}")
                # A mistyped key ("paths", "Role", "release-phase") would silently
                # drop the value -- the join just never sees it.
                unknown = sorted(set(info) - known)
                self.assertEqual(unknown, [], f"{name}: unknown field(s) {unknown} (typo?)")

    def test_role_is_a_list_of_valid_tokens(self):
        """The headline check: role must be a list, never a comma-joined string.

        `'sustain' in 'dps, specialist'` is a substring test and
        `set('dps, specialist')` is a set of *characters*, so both the
        sustain filter and the dps/specialist archetype filter silently stop
        matching.
        """
        for name, info in self.data.items():
            with self.subTest(character=name):
                role = info.get("role")
                if isinstance(role, str):
                    self.fail(
                        f"{name}: 'role' must be a list, got the string {role!r}. "
                        f'Write ["dps", "specialist"] -- a comma-joined string breaks '
                        f"the sustain/dps filters and archetype classification."
                    )
                self.assertIsInstance(role, list, f"{name}: 'role' must be a list, got {type(role).__name__}")
                self.assertTrue(role, f"{name}: 'role' is empty")
                for token in role:
                    self.assertIsInstance(token, str, f"{name}: role token {token!r} is not a string")
                    self.assertNotIn(
                        ",", token,
                        f"{name}: role token {token!r} contains a comma -- one string "
                        f'was pasted instead of separate list items, e.g. ["dps", "specialist"]',
                    )
                    self.assertEqual(token, token.strip(), f"{name}: role token {token!r} has stray whitespace")
                    self.assertEqual(token, token.lower(), f"{name}: role token {token!r} must be lowercase")
                    self.assertIn(
                        token, ALLOWED_ROLES,
                        f"{name}: unknown role {token!r}; allowed: {list(ALLOWED_ROLES)}",
                    )
                self.assertEqual(
                    len(set(role)), len(role),
                    f"{name}: duplicate role(s) in {role!r}",
                )

    def test_role_order_is_canonical(self):
        """Roles are joined with ", " into the DB, so keep the order stable."""
        for name, info in self.data.items():
            role = info.get("role")
            if not isinstance(role, list):
                continue
            with self.subTest(character=name):
                self.assertEqual(
                    role, sorted(role, key=lambda r: ROLE_ORDER.get(r, 99)),
                    f"{name}: role {role!r} is out of canonical order {list(ALLOWED_ROLES)}",
                )

    def test_id(self):
        seen = {}
        for name, info in self.data.items():
            with self.subTest(character=name):
                char_id = info.get("id")
                self.assertIsInstance(char_id, str, f"{name}: 'id' must be a string")
                self.assertRegex(char_id, ID_RE, f"{name}: 'id' {char_id!r} should be 4 digits")
                self.assertNotIn(char_id, seen, f"{name}: duplicate id {char_id} (also {seen.get(char_id)})")
                seen[char_id] = name

    def test_slug_ascii_kebab_case_and_unique(self):
        seen = {}
        for name, info in self.data.items():
            with self.subTest(character=name):
                slug = info.get("slug")
                self.assertIsInstance(slug, str, f"{name}: 'slug' must be a string")
                # A non-ASCII lookalike is almost invisible by eye: U+0435
                # (Cyrillic e) and U+0065 (Latin e) render identically.
                self.assertTrue(
                    slug.isascii(),
                    f"{name}: slug {slug!r} contains non-ASCII characters "
                    f"{[hex(ord(c)) for c in slug if not c.isascii()]} -- looks like a "
                    f"lookalike character; retype it in plain ASCII",
                )
                self.assertRegex(
                    slug, SLUG_RE,
                    f"{name}: slug {slug!r} must be lowercase kebab-case "
                    f"(no camelCase, no trailing/leading dashes)",
                )
                self.assertNotIn(slug, seen, f"{name}: duplicate slug {slug!r} (also {seen.get(slug)})")
                seen[slug] = name

    def test_rarity_path_element_availability(self):
        for name, info in self.data.items():
            with self.subTest(character=name):
                self.assertIn(info.get("rarity"), ALLOWED_RARITIES, f"{name}: rarity must be 4 or 5")
                self.assertIn(
                    info.get("path"), CHARACTERS_PATHS,
                    f"{name}: unknown path {info.get('path')!r}; allowed: {list(CHARACTERS_PATHS)}",
                )
                self.assertIn(
                    info.get("element"), CHARACTERS_ELEMENTS,
                    f"{name}: unknown element {info.get('element')!r}; allowed: {list(CHARACTERS_ELEMENTS)}",
                )
                self.assertIn(
                    info.get("availability"), ALLOWED_AVAILABILITIES,
                    f"{name}: unknown availability {info.get('availability')!r}; "
                    f"allowed: {list(ALLOWED_AVAILABILITIES)}",
                )

    def test_availability_matches_rarity(self):
        for name, info in self.data.items():
            with self.subTest(character=name):
                availability, rarity = info.get("availability"), info.get("rarity")
                if availability == "4*":
                    self.assertEqual(rarity, 4, f"{name}: availability '4*' but rarity is {rarity}")
                elif availability in ("Limited 5*", "Standard 5*"):
                    self.assertEqual(rarity, 5, f"{name}: availability {availability!r} but rarity is {rarity}")

    def test_release_phase_format(self):
        for name, info in self.data.items():
            with self.subTest(character=name):
                phase = info.get("release_phase")
                self.assertIsInstance(phase, str, f"{name}: 'release_phase' must be a string")
                self.assertRegex(phase, RELEASE_PHASE_RE, f"{name}: release_phase {phase!r} should look like '4.5.2'")

    def test_trailblazer_ids(self):
        for name, info in self.data.items():
            if "trailblazer_ids" not in info:
                continue
            ids = info["trailblazer_ids"]
            with self.subTest(character=name):
                self.assertIsInstance(ids, list, f"{name}: 'trailblazer_ids' must be a list")
                self.assertTrue(ids, f"{name}: 'trailblazer_ids' is empty")
                for value in ids:
                    self.assertIsInstance(value, str, f"{name}: trailblazer id {value!r} must be a string")
                    self.assertRegex(value, ID_RE, f"{name}: trailblazer id {value!r} should be 4 digits")
                self.assertEqual(len(set(ids)), len(ids), f"{name}: duplicate trailblazer ids in {ids!r}")
                self.assertIn(
                    info.get("id"), ids,
                    f"{name}: own id {info.get('id')!r} is missing from trailblazer_ids {ids!r}",
                )

    def test_names_are_clean(self):
        for name in self.data:
            with self.subTest(character=name):
                self.assertEqual(name, name.strip(), f"{name!r}: leading/trailing whitespace")
                self.assertNotIn("  ", name, f"{name!r}: double space")
                self.assertEqual(
                    name, unicodedata.normalize("NFC", name),
                    f"{name!r}: not NFC-normalised -- retype the name",
                )
                if NAME_BULLET in name:
                    self.assertEqual(
                        name.count(NAME_BULLET), 1,
                        f"{name!r}: variant names use exactly one {NAME_BULLET!r} separator",
                    )
                    self.assertIn(
                        f" {NAME_BULLET} ", name,
                        f"{name!r}: the {NAME_BULLET!r} separator must be surrounded by single spaces",
                    )
                for ch in name:
                    if ord(ch) > 127:
                        self.assertIn(
                            ch, (NAME_BULLET,),
                            f"{name!r}: unexpected non-ASCII character {ch!r} "
                            f"({unicodedata.name(ch, '?')}, U+{ord(ch):04X})",
                        )

    def test_no_case_insensitive_duplicate_names(self):
        dupes = [n for n, count in Counter(name.lower() for name in self.data).items() if count > 1]
        self.assertEqual(dupes, [], f"names differing only by case: {dupes}")


# ---------------------------------------------------------------------------
# char_config.json
# ---------------------------------------------------------------------------

class TestCharConfigJson(unittest.TestCase):
    """Dashboard-only file: char_config.json."""

    @classmethod
    def setUpClass(cls):
        cls.data = _load(CHAR_CONFIG_PATH)
        cls.characters = _load(CHARACTERS_PATH)

    def test_top_level_shape(self):
        self.assertIsInstance(self.data, list, "char_config.json must be a JSON array")
        self.assertTrue(self.data, "char_config.json is empty")
        for index, entry in enumerate(self.data):
            self.assertIsInstance(entry, dict, f"entry #{index} is not an object")

    def test_character_name_unique_and_clean(self):
        seen = set()
        for entry in self.data:
            name = entry.get("character_name")
            with self.subTest(character=name):
                self.assertIsInstance(name, str, f"entry {entry!r}: 'character_name' must be a string")
                self.assertTrue(name.strip(), "blank 'character_name'")
                self.assertEqual(name, name.strip(), f"{name!r}: leading/trailing whitespace")
                self.assertEqual(name, unicodedata.normalize("NFC", name), f"{name!r}: not NFC-normalised")
                self.assertNotIn(name, seen, f"{name!r}: duplicate character_name")
            seen.add(name)

    def test_fields_present_and_no_unknown_keys(self):
        known = set(REQUIRED_CONFIG_FIELDS) | set(OPTIONAL_CONFIG_FIELDS)
        for entry in self.data:
            name = entry.get("character_name")
            with self.subTest(character=name):
                missing = [k for k in REQUIRED_CONFIG_FIELDS if k not in entry]
                self.assertEqual(missing, [], f"{name}: missing required field(s) {missing}")
                unknown = sorted(set(entry) - known)
                self.assertEqual(unknown, [], f"{name}: unknown field(s) {unknown} (typo?)")

    def test_subtitle(self):
        for entry in self.data:
            name = entry.get("character_name")
            with self.subTest(character=name):
                subtitle = entry.get("subtitle")
                self.assertIsInstance(subtitle, str, f"{name}: 'subtitle' must be a string")
                self.assertTrue(subtitle.strip(), f"{name}: 'subtitle' is blank")

    def test_colours_are_valid(self):
        for entry in self.data:
            name = entry.get("character_name")
            with self.subTest(character=name):
                for field in THEME_HEX_FIELDS:
                    self.assertRegex(
                        str(entry.get(field)), HEX_RE,
                        f"{name}: {field} {entry.get(field)!r} must be a #rrggbb hex colour",
                    )
                glow = entry.get("theme_glow")
                match = RGBA_RE.match(str(glow))
                self.assertIsNotNone(
                    match, f"{name}: theme_glow {glow!r} must look like 'rgba(232,212,77,0.4)'",
                )
                if match:
                    r, g, b, alpha = match.groups()
                    for channel, value in (("r", r), ("g", g), ("b", b)):
                        self.assertLessEqual(int(value), 255, f"{name}: theme_glow {channel} channel out of range")
                    self.assertLessEqual(float(alpha), 1.0, f"{name}: theme_glow alpha must be <= 1.0")

    def test_theme_colours_match_the_element_palette(self):
        """Test the six theme values against the prompt's per-Element palette.

        This is the check that the fill-in template is meant to guarantee: a
        wrong element colour is invisible in review -- both values are valid
        hex -- but it paints Fire colours on an Ice character's sheet, or
        leaves two characters of the same element looking unrelated.
        """
        palette = _parse_element_theme_palette()
        for entry in self.data:
            name = entry.get("character_name")
            element = entry.get("element")
            with self.subTest(character=name, element=element):
                expected = palette.get(element)
                if expected is None:
                    self.fail(
                        f"{name}: element {element!r} has no row in the palette table of "
                        f"{CHARACTER_PROMPT_PATH.name}; rows found: {sorted(palette)}"
                    )
                for field in THEME_PALETTE_FIELDS:
                    self.assertEqual(
                        entry.get(field), expected[field],
                        f"{name} ({element}): {field} is {entry.get(field)!r} but the "
                        f"prompt's {element} palette says {expected[field]!r} -- copy the "
                        f"palette row for this element verbatim",
                    )

    def test_prompt_palette_is_complete_and_valid(self):
        """Guard the parse above, so a reformatted table can't pass vacuously.

        If the markdown stops matching, the test above would see an empty
        palette and every character would fail for the wrong reason.
        """
        palette = _parse_element_theme_palette()
        self.assertNotEqual(
            palette, {},
            f"could not parse a palette table from {CHARACTER_PROMPT_PATH.name}; expected a "
            f"'| Element | theme_c1 | ... | theme_glow |' header row",
        )
        self.assertEqual(
            sorted(palette), sorted(CONFIG_ELEMENTS),
            f"the palette table in {CHARACTER_PROMPT_PATH.name} should define exactly the "
            f"config elements {list(CONFIG_ELEMENTS)}",
        )
        for element, values in palette.items():
            with self.subTest(element=element):
                for field in THEME_HEX_FIELDS:
                    self.assertRegex(
                        values[field], HEX_RE,
                        f"{element}: {field} {values[field]!r} should be a #rrggbb hex colour",
                    )
                self.assertRegex(
                    values["theme_glow"], RGBA_RE,
                    f"{element}: theme_glow {values['theme_glow']!r} should look like 'rgba(232,212,77,0.4)'",
                )

    def test_glow_matches_theme_c1(self):
        """theme_glow is theme_c1 at low alpha; a mismatched pair is a copy-paste slip."""
        for entry in self.data:
            name = entry.get("character_name")
            match = RGBA_RE.match(str(entry.get("theme_glow")))
            c1 = str(entry.get("theme_c1"))
            if not match or not HEX_RE.match(c1):
                continue
            with self.subTest(character=name):
                glow_rgb = [int(match.group(1)), int(match.group(2)), int(match.group(3))]
                c1_rgb = [int(c1[1:3], 16), int(c1[3:5], 16), int(c1[5:7], 16)]
                self.assertEqual(
                    glow_rgb, c1_rgb,
                    f"{name}: theme_glow rgb {glow_rgb} != theme_c1 {c1} rgb {c1_rgb}",
                )

    def test_stat_lists(self):
        for entry in self.data:
            name = entry.get("character_name")
            with self.subTest(character=name):
                lists = {}
                for field in ("defining_stats", "supporting_stats", "sidebar_spread_stats"):
                    values = entry.get(field)
                    self.assertIsInstance(values, list, f"{name}: {field} must be a list")
                    self.assertTrue(values, f"{name}: {field} is empty")
                    lists[field] = values
                    self.assertEqual(
                        len(set(values)), len(values),
                        f"{name}: {field} has duplicates ({values!r})",
                    )
                    for stat in values:
                        self.assertIn(
                            stat, STAT_NAMES,
                            f"{name}: unknown stat {stat!r} in {field}; allowed: {list(STAT_NAMES)}",
                        )
                extra = sorted(set(lists["sidebar_spread_stats"]) - set(lists["defining_stats"]) - set(lists["supporting_stats"]))
                self.assertEqual(
                    extra, [],
                    f"{name}: sidebar_spread_stats entries {extra} are not in defining_stats "
                    f"or supporting_stats (typo?)",
                )

    def test_sidebar_metadata(self):
        for entry in self.data:
            name = entry.get("character_name")
            with self.subTest(character=name):
                metadata = entry.get("sidebar_metadata")
                self.assertIsInstance(metadata, list, f"{name}: sidebar_metadata must be a list")
                self.assertEqual(
                    len(set(metadata)), len(metadata),
                    f"{name}: sidebar_metadata has duplicates ({metadata!r})",
                )
                self.assertEqual(
                    set(metadata), set(SIDEBAR_METADATA),
                    f"{name}: sidebar_metadata {metadata!r} should be {list(SIDEBAR_METADATA)}",
                )

    def test_element_and_path_vocabulary(self):
        for entry in self.data:
            name = entry.get("character_name")
            with self.subTest(character=name):
                self.assertIn(
                    entry.get("element"), CONFIG_ELEMENTS,
                    f"{name}: unknown element {entry.get('element')!r}; allowed: {list(CONFIG_ELEMENTS)}",
                )
                self.assertIn(
                    entry.get("path"), CHARACTERS_PATHS,
                    f"{name}: unknown path {entry.get('path')!r}; allowed: {list(CHARACTERS_PATHS)}",
                )

    def test_optional_overrides(self):
        """role / availability may be set here to override the DB value."""
        for entry in self.data:
            name = entry.get("character_name")
            with self.subTest(character=name):
                if "role" in entry:
                    role = entry["role"]
                    self.assertIsInstance(role, list, f"{name}: override 'role' must be a list, got {type(role).__name__}")
                    for token in role:
                        self.assertIn(token, ALLOWED_ROLES, f"{name}: unknown override role {token!r}")
                if "availability" in entry:
                    self.assertIn(
                        entry["availability"], ALLOWED_AVAILABILITIES,
                        f"{name}: unknown override availability {entry['availability']!r}",
                    )


# ---------------------------------------------------------------------------
# Cross-file agreement
# ---------------------------------------------------------------------------

class TestCrossFileConsistency(unittest.TestCase):
    """The three files must describe the same roster, the same way."""

    @classmethod
    def setUpClass(cls):
        cls.characters = _load(CHARACTERS_PATH)
        cls.config = _load(CHAR_CONFIG_PATH)
        try:
            cls.icons = _load(CHARACTER_ICONS_PATH)
        except FileNotFoundError:
            cls.icons = None
        cls.config_by_name = {e.get("character_name"): e for e in cls.config}

    def test_rosters_match(self):
        """A character in only one file either gets no data or no dashboard."""
        characters = set(self.characters)
        configured = set(self.config_by_name)
        self.assertEqual(
            sorted(characters - configured), [],
            "in characters.json but not in char_config.json -> no dashboard page is generated",
        )
        self.assertEqual(
            sorted(configured - characters), [],
            "in char_config.json but not in characters.json -> dashboard has no DB data behind it",
        )

    def test_icons_cover_the_roster(self):
        if self.icons is None:
            self.skipTest("character_icons.json not found")
        characters = set(self.characters)
        icons = set(self.icons)
        self.assertEqual(
            sorted(characters - icons), [],
            "in characters.json but not in character_icons.json -> index shows a missing icon",
        )
        self.assertEqual(
            sorted(icons - characters), [],
            "in character_icons.json but not in characters.json -> orphan icon entry",
        )

    def test_element_and_path_agree_between_files(self):
        for name, info in self.characters.items():
            entry = self.config_by_name.get(name)
            if entry is None:
                continue
            with self.subTest(character=name):
                config_element = str(entry.get("element", "")).lower()
                config_element = ELEMENT_ALIASES.get(config_element, config_element)
                self.assertEqual(
                    config_element, str(info.get("element", "")).lower(),
                    f"{name}: element differs -- char_config={entry.get('element')!r}, "
                    f"characters.json={info.get('element')!r}",
                )
                self.assertEqual(
                    str(entry.get("path", "")).lower(), str(info.get("path", "")).lower(),
                    f"{name}: path differs -- char_config={entry.get('path')!r}, "
                    f"characters.json={info.get('path')!r}",
                )


# ---------------------------------------------------------------------------
# characters.json vs the dataset -- the silent metadata join
# ---------------------------------------------------------------------------

class TestCharMetadataCoverage(unittest.TestCase):
    """The rule database_main/database_batch use around the metadata join.

    `character_stats` is built with `join(char_metadata_pl, on="Character",
    how="left")`, so a dataset name with no characters.json entry does not
    raise -- role/availability/element/path/release_phase just come back NULL
    for every row of that character.
    """

    def test_missing_names_are_reported(self):
        self.assertEqual(
            missing_char_metadata({"Acheron", "Kafka"}, ["Acheron", "Kafka", "Aventurine", "Cyrene"]),
            ["Aventurine", "Cyrene"],
        )

    def test_known_and_placeholder_names_do_not_warn(self):
        self.assertEqual(missing_char_metadata({"Acheron"}, ["Acheron"]), [])
        self.assertEqual(missing_char_metadata({"Acheron"}, ["Acheron", "Empty Slot"]), [])
        for placeholder in PLACEHOLDER_CHARACTERS:
            self.assertEqual(missing_char_metadata(set(), [placeholder]), [])

    def test_blank_and_non_string_names_are_ignored(self):
        """NULL slots and filler values are not something you can add metadata for."""
        self.assertEqual(missing_char_metadata({"Acheron"}, ["Acheron", None, "", "   ", 7]), [])

    def test_near_miss_names_are_reported(self):
        """A separator/spelling difference is exactly the silent failure this catches."""
        known = {"Dan Heng \u2022 Imbibitor Lunae"}
        self.assertEqual(
            missing_char_metadata(known, ["Dan Heng and Imbibitor Lunae"]),
            ["Dan Heng and Imbibitor Lunae"],
        )

    def test_warning_message_names_what_is_missing(self):
        message = describe_missing_char_metadata(["Aventurine", "Cyrene"])
        self.assertIn("2 character name(s)", message)
        self.assertIn("Aventurine, Cyrene", message)
        self.assertIn("characters.json", message)

    def test_real_characters_json_names_load(self):
        names = load_character_names(CHARACTERS_PATH)
        self.assertEqual(names, set(self.__class__._characters().keys()))

    @staticmethod
    def _characters():
        with open(CHARACTERS_PATH, encoding="utf-8") as f:
            return json.load(f)


if __name__ == "__main__":
    unittest.main(verbosity=2)
