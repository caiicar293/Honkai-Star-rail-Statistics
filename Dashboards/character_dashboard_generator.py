import os
import json
import duckdb
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
from jinja2 import Environment, FileSystemLoader

# 1. Dynamically locate where the .env file actually lives on this machine
dotenv_path = find_dotenv()
load_dotenv(dotenv_path)

class CharacterDashboard:
    # ── BASE QUERIES ──
    DUOS_QUERY = """
    WITH RankedDuos AS (
        SELECT 
            Game_Mode,
            Consequent AS partner, 
            Total_Samples AS samples,
            Simple_Avg_Appearance_Rate AS appearance, 
            Simple_Avg_Confidence AS confidence, 
            Simple_Avg_Score AS score, 
            Weighted_Avg_Score AS weighted, 
            Best_Version_Avg AS best,
            ROW_NUMBER() OVER(PARTITION BY Game_Mode ORDER BY Total_Samples DESC) as rn
        FROM 
            duos_meta_summary
        WHERE 
            Antecedent = ? AND at_eidolon_level = 0 AND up_to_eidolon_level = 6
    )
    SELECT Game_Mode, partner, samples, appearance, confidence, score, weighted, best
    FROM RankedDuos WHERE rn <= 8 ORDER BY Game_Mode, samples DESC;
    """

    TEAMS_QUERY = """
    WITH RankedTeams AS (
        SELECT 
            Game_Mode, Team, 
            Simple_Avg_Appearance AS appearance, 
            Simple_Avg_Score AS score, 
            Weighted_Avg_Score AS weighted, 
            Best_Version_Avg AS best, 
            Total_Samples AS samples,
            ROW_NUMBER() OVER(PARTITION BY Game_Mode ORDER BY Total_Samples DESC) as rn
        FROM 
            team_meta_summary
        WHERE 
            Team LIKE ? AND at_eidolon_level = 0 AND up_to_eidolon_level = 6
    )
    SELECT Game_Mode, Team, appearance, score, weighted, best, samples
    FROM RankedTeams WHERE rn <= 5 ORDER BY Game_Mode, samples DESC;
    """

    ARCHETYPES_QUERY = """
    SELECT Game_Mode, Archetype_Core, Simple_Avg_Appearance, Simple_Avg_Score,
           Weighted_Avg_Score, Best_Version_Avg, Total_Samples
    FROM archetype_meta_summary
    WHERE Archetype_Core LIKE ? AND at_eidolon_level = 0 AND up_to_eidolon_level = 6
    ORDER BY Game_Mode, Total_Samples DESC;
    """

    GEAR_QUERY = """
    WITH ranked AS (
        SELECT
            Game_Mode,
            Eidolon,
            Category,
            Gear_Name,
            SUM(Total_Usage)                                        AS total_usage,
            SUM(Total_Usage * Weighted_Avg_Score)
                / NULLIF(SUM(Total_Usage), 0)                      AS avg_score,
            SUM(Usage_Rate)                                         AS summed_rate,
            ROW_NUMBER() OVER (
                PARTITION BY Game_Mode, Eidolon, Category
                ORDER BY SUM(Total_Usage) DESC
            ) AS rn
        FROM gear_meta_summary
        WHERE Character = ?
        AND at_eidolon_level = 0
        AND up_to_eidolon_level = 6
        AND Eidolon IN ('Eidolon 0','Eidolon 1','Eidolon 2','Eidolon 6')
        AND Category IN ('Relics','Planar_Set','Lightcones')
        GROUP BY Game_Mode, Eidolon, Category, Gear_Name
    )
    SELECT Game_Mode, Eidolon, Category, Gear_Name,
        summed_rate AS usage_rate, avg_score
    FROM ranked
    WHERE rn <= 5
    ORDER BY Game_Mode, Eidolon, Category, total_usage DESC;
    """

    EIDOLON_MAP = {
        'Eidolon 0': 'E0',
        'Eidolon 1': 'E1',
        'Eidolon 2': 'E2',
        'Eidolon 6': 'E6',
    }

    DESIRED_MODES = ['MOC', 'PURE_FICTION', 'APOC', 'ANOMALY_F0', 'ANOMALY_F4']

    def __init__(self, character_name, db_path=None, icons_path=None, custom_build_stats=None, template_dir=".", template_name="dashboard_template_python.html"):
        """
        Initializes the dashboard generator.
        
        :param character_name: Name of the character (e.g., "Lingsha")
        :param db_path: Path to duckdb file. Defaults to .env "DB_File"
        :param icons_path: Path to character_icons.json. Defaults to project root fallback path.
        :param custom_build_stats: Dictionary of stats/HTML blocks to manually override database values.
        """
        # Get the directory of this script file to resolve template directories safely
        SCRIPT_DIR = Path(__file__).parent.resolve()
        
        # Get the directory of the .env file (e.g., "C:/Users/.../Honkai-Star-rail-Statistics")
        PROJECT_ROOT = Path(dotenv_path).parent.resolve()

        # 2. Grab the raw value from the environment variable or argument
        raw_db_path = db_path or os.getenv("DB_File")

        if raw_db_path:
            raw_path_obj = Path(raw_db_path)
            # If it's a relative path, resolve it relative to the PROJECT ROOT (.env location)
            if not raw_path_obj.is_absolute():
                self.db_path = str((PROJECT_ROOT / raw_path_obj).resolve())
            else:
                self.db_path = str(raw_path_obj.resolve())
        else:
            raise ValueError("❌ No database path provided in arguments or .env configuration.")
        
        # 3. Resolve the Icons Path dynamically
        raw_icons_path = icons_path or "../character_icons.json"
        raw_icons_obj = Path(raw_icons_path)
        if not raw_icons_obj.is_absolute():
            self.icons_path = str((PROJECT_ROOT / raw_icons_obj).resolve())
        else:
            self.icons_path = str(raw_icons_obj.resolve())
  
        self.character_name = character_name
        
        # If the template directory is default ".", make it absolute to prevent file location bugs
        if template_dir == ".":
            self.template_dir = str(SCRIPT_DIR)
        else:
            self.template_dir = template_dir
            
        self.template_name = template_name
        
        # User-inputted variables for build stats and themes
        self.custom_build_stats = custom_build_stats or {}

    def _get_character_version_and_node(self, con):
        v_res = con.execute("SELECT DISTINCT version FROM moc_stats_gear_usage WHERE Character = ? ORDER BY version DESC LIMIT 1", [self.character_name]).fetchone()
        version = v_res[0] if v_res else "4.2.2"
        
        n_res = con.execute("SELECT DISTINCT node FROM moc_stats_gear_usage WHERE Character = ? LIMIT 1", [self.character_name]).fetchone()
        node_value = n_res[0] if n_res else 0
        return version, node_value

    def _extract_builds_data(self, con):
        try:
            query = "SELECT * FROM character_builds_all_versions WHERE character = ?"
            result = con.execute(query, [self.character_name]).fetchone()
            if result:
                columns = [desc[0].lower() for desc in con.description]
                return dict(zip(columns, result))
        except Exception as e:
            print(f"Error fetching builds data: {e}")
        return {}

    def _extract_gear_usage(self, con):
        """Pulls from gear_meta_summary (cross-version pre-aggregated)."""
        gear_data = {m: {} for m in self.DESIRED_MODES}

        try:
            results = con.execute(self.GEAR_QUERY, [self.character_name]).fetchall()
        except Exception as e:
            print(f"Error fetching gear_meta_summary: {e}")
            return gear_data

        for row in results:
            game_mode, raw_eidolon, category, name, usage_rate, score = row
            e_key = self.EIDOLON_MAP.get(raw_eidolon)
            if not e_key or game_mode not in gear_data:
                continue

            if e_key not in gear_data[game_mode]:
                gear_data[game_mode][e_key] = {'Relics': [], 'Planar_Set': [], 'Lightcones': []}

            if len(gear_data[game_mode][e_key][category]) < 5:
                gear_data[game_mode][e_key][category].append({
                    "name": name,
                    "rate": float(usage_rate or 0),
                    "score": float(score or 0),
                })

        return gear_data

    def _extract_duos(self, con):
        try:
            results = con.execute(self.DUOS_QUERY, [self.character_name]).fetchall()
            desired_order = ['MOC', 'PURE_FICTION', 'APOC', 'ANOMALY_F0', 'ANOMALY_F4']
            output = {mode: [] for mode in desired_order}
            columns = ['game_mode', 'partner', 'samples', 'appearance', 'confidence', 'score', 'weighted', 'best']
            
            for row in results:
                data = dict(zip(columns, row))
                mode = data.pop('game_mode')
                if mode in output:
                    output[mode].append(data)
            return output
        except Exception as e:
            print(f"Error extracting duos: {e}")
            return {}

    def _extract_teams(self, con):
        try:
            results = con.execute(self.TEAMS_QUERY, [f"%{self.character_name}%"]).fetchall()
            desired_order = ['MOC', 'PURE_FICTION', 'APOC', 'ANOMALY_F0', 'ANOMALY_F4']
            output = {mode: [] for mode in desired_order}
            columns = ['game_mode', 'team', 'appearance', 'score', 'weighted', 'best', 'samples']
            
            for row in results:
                data = dict(zip(columns, row))
                mode = data.pop('game_mode')
                if mode in output:
                    output[mode].append(data)
            return output
        except Exception as e:
            print(f"Error extracting teams: {e}")
            return {}

    def _extract_archetypes(self, con):
        try:
            results = con.execute(self.ARCHETYPES_QUERY, [f"%{self.character_name}%"]).fetchall()
            desired_order = ['MOC', 'PURE_FICTION', 'APOC', 'ANOMALY_F0', 'ANOMALY_F4']
            output = {mode: [] for mode in desired_order}
            columns = ['game_mode', 'core', 'appearance', 'score', 'weighted', 'best', 'samples']
            
            for row in results:
                data = dict(zip(columns, row))
                mode = data.pop('game_mode')
                if mode in output and len(output[mode]) < 8:
                    output[mode].append(data)
            return output
        except Exception as e:
            print(f"Error extracting archetypes: {e}")
            return {}

    def _build_icons_dictionary(self, teams_data, duos_data):
        unique_names = {self.character_name}
        
        for mode, teams in teams_data.items():
            for t in teams:
                cleaned = t['team'].replace('(', '').replace(')', '')
                for name in cleaned.split(','):
                    unique_names.add(name.strip())
                    
        for mode, duos in duos_data.items():
            for d in duos:
                unique_names.add(d['partner'].strip())
                
        try:
            # FIX: Reads from self.icons_path instead of the hardcoded "../character_icons.json" string
            with open(self.icons_path, "r", encoding="utf-8") as f:
                all_icons = json.load(f)
        except Exception as e:
            print(f"Warning: Could not open icons file at {self.icons_path} ({e}). Using empty structure.")
            all_icons = {}
            
        return {name: all_icons.get(name, "") for name in unique_names if name}

    def _embed_with_jinja(self, output_path, template_vars):
        env = Environment(loader=FileSystemLoader(self.template_dir))
        template = env.get_template(self.template_name)
        rendered_html = template.render(template_vars)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(rendered_html)
        print(f"Jinja automated dashboard written to {output_path}")

    def generate(self, output_file="output.html"):
        """Executes the extraction pipeline and compiles the final HTML."""
        con = duckdb.connect(self.db_path, read_only=True)
        
        latest_version, correct_node = self._get_character_version_and_node(con)
        
        # Extract Database Info
        db_builds_data = self._extract_builds_data(con)
        gear_data = self._extract_gear_usage(con)
        teams_data = self._extract_teams(con)
        duos_data = self._extract_duos(con)
        archetypes_data = self._extract_archetypes(con)
        icons_map = self._build_icons_dictionary(teams_data, duos_data)
        
        con.close()

        # Helper to prioritize data: 1. User Input, 2. Database, 3. Default fallback
        def get_stat(key, default_val):
            return self.custom_build_stats.get(key, db_builds_data.get(key, default_val))

        # Bind parameters
        master_template_vars = {
            # Identity Tokens
            "CHAR_NAME": self.character_name,
            "CHAR_ELEMENT": get_stat("element", "Unknown"),
            "CHAR_PATH": get_stat("path", "Unknown"),
            "CHAR_SUBTITLE": get_stat("subtitle", "Unknown"),
            "CHAR_ICON_URL": icons_map.get(self.character_name, ""),
            "DATA_VERSION": latest_version,
            
            # Numeric Overviews
            "TOTAL_SAMPLES": get_stat("total_sample_size", "0"),
            "NUM_VERSIONS": get_stat("num_versions", "1"),
            
            # Theme Tokens
            "THEME_C1": get_stat("theme_c1", "#555555"), 
            "THEME_C2": get_stat("theme_c2", "#777777"), 
            "THEME_C3": get_stat("theme_c3", "#333333"),
            "THEME_P1": get_stat("theme_p1", "#555555"), 
            "THEME_P2": get_stat("theme_p2", "#777777"), 
            "THEME_GLOW": get_stat("theme_glow", "rgba(85,85,85,0.4)"),

            # HTML Layout Segments
            "HERO_STATS": get_stat("hero_stats", ""),
            "STAT_CELLS": get_stat("stat_cells", ""),
            "MAIN_STATS": get_stat("main_stats", ""),
            "SUBSTATS": get_stat("substats", ""),
            "PERCENTILES": get_stat("percentiles", ""),
            "AVG_SIDEBAR": get_stat("avg_sidebar", ""),

            # JavaScript Globals
            "ICONS": icons_map,
            "GEAR": gear_data,
            "TEAMS": teams_data,
            "ARCHETYPES": archetypes_data,
            "DUOS": duos_data
        }

        self._embed_with_jinja(output_file, master_template_vars)


# ── Execution Pipeline Example ──
# if __name__ == "__main__":
#     user_inputs = {
#         # Core Identity
#         "element": "Fire",
#         "path": "Abundance",
#         "subtitle": "The Fiery Pharmacist",
#         "total_sample_size": "63,007",
#         "num_versions": "41",
        
#         # Theme
#         "theme_c1": "#cc542f",
#         "theme_c2": "#f5a060",
#         "theme_c3": "#b03010",
#         "theme_p1": "#e05a30",
#         "theme_p2": "#f5a060",
#         "theme_glow": "rgba(224,90,48,0.4)",
        
#         # HTML Block: Hero Stats
#         "hero_stats": """<div class="hero-stat">
#   <span class="hero-stat-val">63,007</span>
#   <span class="hero-stat-lbl">Sampled Builds</span>
# </div>
# <div class="hero-stat">
#   <span class="hero-stat-val">27.6%</span>
#   <span class="hero-stat-lbl">Avg Outgoing Healing</span>
# </div>
# <div class="hero-stat">
#   <span class="hero-stat-val">149.0</span>
#   <span class="hero-stat-lbl">Avg SPD</span>
# </div>
# <div class="hero-stat">
#   <span class="hero-stat-val">164.1%</span>
#   <span class="hero-stat-lbl">Avg Break Effect</span>
# </div>""",

#         # HTML Block: Stat Cells
#         "stat_cells": """<div class="stat-cell featured">
#   <span class="stat-val">27.6%</span>
#   <span class="stat-lbl">Outgoing Healing Boost — Defining Stat</span>
# </div>
# <div class="stat-cell featured">
#   <span class="stat-val">149.0</span>
#   <span class="stat-lbl">Avg SPD — Defining Stat</span>
# </div>
# <div class="stat-cell">
#   <span class="stat-val">4018</span>
#   <span class="stat-lbl">Avg HP</span>
# </div>
# <div class="stat-cell">
#   <span class="stat-val">2415</span>
#   <span class="stat-lbl">Avg ATK</span>
# </div>
# <div class="stat-cell">
#   <span class="stat-val">1060</span>
#   <span class="stat-lbl">Avg DEF</span>
# </div>
# <div class="stat-cell">
#   <span class="stat-val">13.0%</span>
#   <span class="stat-lbl">Avg CRIT Rate</span>
# </div>
# <div class="stat-cell">
#   <span class="stat-val">64.9%</span>
#   <span class="stat-lbl">Avg CRIT DMG</span>
# </div>
# <div class="stat-cell">
#   <span class="stat-val">164.1%</span>
#   <span class="stat-lbl">Avg Break Effect</span>
# </div>""",

#         # HTML Block: Main Stats
#         "main_stats": """<div class="main-stat-card">
#   <div class="main-stat-slot">Body</div>
#   <span class="main-stat-pill dominant">Outgoing Healing Boost 77.8%</span><span class="main-stat-pill alt">ATK 17.2%</span><span class="main-stat-pill alt">CRIT Rate 2.4%</span>
# </div>
# <div class="main-stat-card">
#   <div class="main-stat-slot">Feet</div>
#   <span class="main-stat-pill dominant">SPD 98.2%</span><span class="main-stat-pill alt">ATK 1.4%</span>
# </div>
# <div class="main-stat-card">
#   <div class="main-stat-slot">Sphere</div>
#   <span class="main-stat-pill dominant">ATK 86.5%</span><span class="main-stat-pill alt">Fire DMG Boost 6.0%</span><span class="main-stat-pill alt">HP 4.3%</span>
# </div>
# <div class="main-stat-card">
#   <div class="main-stat-slot">Rope</div>
#   <span class="main-stat-pill dominant">Energy Regeneration Rate 60.6%</span><span class="main-stat-pill alt">Break Effect 36.6%</span><span class="main-stat-pill alt">ATK 2.2%</span>
# </div>""",

#         # HTML Block: Substats
#         "substats": """<div class="substat-row">
#   <span class="substat-name">Break Effect sub</span>
#   <div class="substat-bar-bg"><div class="substat-bar-fill" style="width:100.0%"></div></div>
#   <span class="substat-val">53.8</span>
# </div>
# <div class="substat-row">
#   <span class="substat-name">SPD sub</span>
#   <div class="substat-bar-bg"><div class="substat-bar-fill" style="width:38.5%"></div></div>
#   <span class="substat-val">20.7</span>
# </div>
# <div class="substat-row">
#   <span class="substat-name">ATK sub</span>
#   <div class="substat-bar-bg"><div class="substat-bar-fill" style="width:31.3%"></div></div>
#   <span class="substat-val">16.9</span>
# </div>
# <div class="substat-row">
#   <span class="substat-name">DEF sub</span>
#   <div class="substat-bar-bg"><div class="substat-bar-fill" style="width:28.1%"></div></div>
#   <span class="substat-val">15.1</span>
# </div>
# <div class="substat-row">
#   <span class="substat-name">HP sub</span>
#   <div class="substat-bar-bg"><div class="substat-bar-fill" style="width:22.5%"></div></div>
#   <span class="substat-val">12.1</span>
# </div>
# <div class="substat-row">
#   <span class="substat-name">CRIT DMG sub</span>
#   <div class="substat-bar-bg"><div class="substat-bar-fill" style="width:21.7%"></div></div>
#   <span class="substat-val">11.7</span>
# </div>
# <div class="substat-row">
#   <span class="substat-name">Effect RES sub</span>
#   <div class="substat-bar-bg"><div class="substat-bar-fill" style="width:20.9%"></div></div>
#   <span class="substat-val">11.2</span>
# </div>
# <div class="substat-row">
#   <span class="substat-name">Effect Hit Rate sub</span>
#   <div class="substat-bar-bg"><div class="substat-bar-fill" style="width:15.3%"></div></div>
#   <span class="substat-val">8.2</span>
# </div>
# <div class="substat-row">
#   <span class="substat-name">CRIT Rate sub</span>
#   <div class="substat-bar-bg"><div class="substat-bar-fill" style="width:10.8%"></div></div>
#   <span class="substat-val">5.8</span>
# </div>""",

#         # HTML Block: Percentiles
#         "percentiles": """<div class="percentile-card">
#   <div class="percentile-title">Break Effect Spread</div>
#   <div class="percentile-row"><span class="p-label">p25</span><span class="p-val">135.6%</span></div>
#   <div class="percentile-row"><span class="p-label">p50 (median)</span><span class="p-val p50">165.8%</span></div>
#   <div class="percentile-row"><span class="p-label">p75</span><span class="p-val">194.9%</span></div>
# </div>
# <div class="percentile-card">
#   <div class="percentile-title">SPD Spread</div>
#   <div class="percentile-row"><span class="p-label">p25</span><span class="p-val">141.6</span></div>
#   <div class="percentile-row"><span class="p-label">p50 (median)</span><span class="p-val p50">150.7</span></div>
#   <div class="percentile-row"><span class="p-label">p75</span><span class="p-val">156.6</span></div>
# </div>""",

#         # HTML Block: Average Sidebar
#         "avg_sidebar": """<div class="avg-item">
#   <div class="avg-item-lbl">SPD Spread p25–p75</div>
#   <div class="avg-item-val">141.6 – 156.6</div>
# </div>
# <div class="avg-item">
#   <div class="avg-item-lbl">Break Effect Spread p25–p75</div>
#   <div class="avg-item-val">135.6% – 194.9%</div>
# </div>
# <div class="avg-item">
#   <div class="avg-item-lbl">Outgoing Healing Spread p25–p75</div>
#   <div class="avg-item-val">34.6% – 34.6%</div>
# </div>
# <div class="avg-item">
#   <div class="avg-item-lbl">Total Samples</div>
#   <div class="avg-item-val">63,007</div>
# </div>
# <div class="avg-item">
#   <div class="avg-item-lbl">Dataset Versions</div>
#   <div class="avg-item-val">41</div>
# </div>
# <div class="avg-item">
#   <div class="avg-item-lbl">Latest Version</div>
#   <div class="avg-item-val">4.2.3</div>
# </div>"""
#     }
#     BASE_DIR = os.path.dirname(os.path.abspath(__file__))
#     DB_PATH = os.path.join(BASE_DIR, '..', os.getenv("DB_File"))
#     CHARACTER_ICONS_PATH = os.path.join(BASE_DIR, '..', 'character_icons.json')

#     # Custom assignment example:
#     dashboard = CharacterDashboard(
#         character_name="Lingsha",
#         db_path=DB_PATH, # Optional manual path override
#         icons_path=CHARACTER_ICONS_PATH,                # Optional manual path override
#         custom_build_stats=user_inputs
#     )
    
#     dashboard.generate(output_file="tedsst.html")