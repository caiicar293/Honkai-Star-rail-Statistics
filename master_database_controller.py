from database_chars import HonkaiCharacterWarehouse
from database_Archetype_warehouse import HonkaiArchetypeWarehouse
from database_Teams_Warehouse import HonkaiTeamsWarehouse
from clean_data import clear_data_from_warehouse


#For replacing a mode
DB = "honkai_star_rail_stats2.duckdb"
VER = "4.0.2"

# --- TOGGLE MODES HERE ---
# Set to None to clear the version everywhere
# Set to a list to target specific archetypes/teams
MODES_TO_DELETE = ["MOC", "MOC_BOTH_SIDES"] 

clear_data_from_warehouse(DB, VER, target_modes=MODES_TO_DELETE)

arche = HonkaiArchetypeWarehouse()
arche.run(target_version= "4.1.1")
arche.run_dual(target_version= "4.1.1")

teams = HonkaiTeamsWarehouse()
teams.run(target_version= "4.1.1")
teams.run_dual(target_version= "4.1.1")

chars= HonkaiCharacterWarehouse()
chars.run(target_version="4.1.1")