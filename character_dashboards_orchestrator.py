
import os
from dotenv import load_dotenv
from character_dashboard_generator import CharacterDashboard

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath("__file__"))
DB_PATH =os.getenv("DB_File")
CHARACTER_ICONS_PATH = (r'character_icons.json')


import orjson

with open('char_config.json', 'rb') as f:
            info = orjson.loads(f.read())
user_inputs = info


for char in user_inputs:
    dashboard = CharacterDashboard(
        character_name=char['character_name'],
        db_path=DB_PATH,
        icons_path=CHARACTER_ICONS_PATH,
       
        custom_build_stats=char
    )

    dashboard.generate(output_file=f"docs/characters/{char['character_name']}_Dashboard.html",path_prefix="../")
    