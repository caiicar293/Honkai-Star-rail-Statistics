# HSR Character Dashboard — JSON Configuration Prompt

Use this prompt when asking an AI to generate a configuration file for the automated `CharacterDashboard` pipeline.

---

## PROMPT

You are generating a JSON configuration file for the `CharacterDashboard` class in the Honkai: Star Rail Statistics pipeline. 

**Character:** `<CHARACTER_NAME>`

---

### STEP 1 — Query the Database
Run this query against the `character_builds_all_versions` table from the hsr-statistics-duckdb MCP server:

```sql
SELECT * FROM character_builds_all_versions WHERE character = '<CHARACTER_NAME>';

```

You must use the returned data to inform your decisions in Step 3. Do not invent or estimate any values.

---

### STEP 2 — Core Identity & Theme

Determine the character's Element, Path, and a lore-accurate subtitle (e.g., "The Fiery Pharmacist", "Emanator of Nihility").

Using the character's Element, assign the exact 6 hex/rgba values from this table:

| Element | theme_c1 | theme_c2 | theme_c3 | theme_p1 | theme_p2 | theme_glow |
| --- | --- | --- | --- | --- | --- | --- |
| Lightning | `#e8d44d` | `#c080ff` | `#f0e880` | `#e8d44d` | `#c080ff` | `rgba(232,212,77,0.4)` |
| Quantum | `#8b6eff` | `#c8a8ff` | `#6040cc` | `#8b6eff` | `#c8a8ff` | `rgba(139,110,255,0.4)` |
| Ice | `#6eb4f7` | `#a8d4ff` | `#3a7acc` | `#6eb4f7` | `#a8d4ff` | `rgba(110,180,247,0.4)` |
| Fire | `#e05a30` | `#f5a060` | `#b03010` | `#e05a30` | `#f5a060` | `rgba(224,90,48,0.4)` |
| Wind | `#4ecfa0` | `#a0f0d0` | `#20a070` | `#4ecfa0` | `#a0f0d0` | `rgba(78,207,160,0.4)` |
| Imaginary | `#e8c97a` | `#f0dfa0` | `#a8883a` | `#e8c97a` | `#c8a060` | `rgba(232,201,122,0.4)` |
| Physical | `#c0cce0` | `#e8eef8` | `#8090a8` | `#c0cce0` | `#e8eef8` | `rgba(192,204,224,0.4)` |

---

### STEP 3 — Stat Selection Logic

Select the stats that best define this character's optimal build. Use exact schema names (e.g., "CRIT DMG", "SPD", "Effect Hit Rate", "DMG Boost", "Break Effect").

1. **`defining_stats`**: Choose exactly **2** primary optimization targets.
2. **`supporting_stats`**: Choose exactly **6** secondary stats.
3. **`sidebar_spread_stats`**: Choose **3** stats to show p25–p75 spread ranges for.
4. **`sidebar_metadata`**: Use exactly `["Total Samples", "Dataset Versions", "Latest Version"]`.

*CRITICAL RULE:* The total number of items in `sidebar_spread_stats` + `sidebar_metadata` must equal exactly 6.

---

### STEP 4 — Output Format

Output ONLY a valid JSON block. Do not include any conversational filler, explanations, or python code.

**Expected Format:**

```json
{
  "character_name": "<CHARACTER_NAME>",
  "element": "...",
  "path": "...",
  "subtitle": "...",
  "theme_c1": "...",
  "theme_c2": "...",
  "theme_c3": "...",
  "theme_p1": "...",
  "theme_p2": "...",
  "theme_glow": "...",
  "defining_stats": ["...", "..."],
  "supporting_stats": ["...", "...", "...", "...", "...", "..."],
  "sidebar_spread_stats": ["...", "...", "..."],
  "sidebar_metadata": ["Total Samples", "Dataset Versions", "Latest Version"]
}

```



