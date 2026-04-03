from database_dual_archetypes_warehouse import HonkaiDualArchetypeWarehouse
from database_Dual_Teams_Warehouse import HonkaiDualTeamWarehouse
from database_chars import HonkaiCharacterWarehouse
from database_Archetype_warehouse import HonkaiArchetypeWarehouse
from database_Team_Warehouse import HonkaiTeamWarehouse


dual_arches = HonkaiDualArchetypeWarehouse()
dual_arches.run(target_version= "4.1.1")


dual_teams = HonkaiDualTeamWarehouse()
dual_teams.run(target_version= "4.1.1")

teams = HonkaiDualTeamWarehouse()
teams.run(target_version= "4.1.1")



archetypes = HonkaiArchetypeWarehouse()
archetypes.run(target_version="4.1.1")


chars= HonkaiCharacterWarehouse()
chars.run(target_version="4.1.1")