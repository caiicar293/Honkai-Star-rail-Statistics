from database_chars import HonkaiCharacterWarehouse
from database_Archetype_warehouse import HonkaiArchetypeWarehouse
from database_Teams_Warehouse import HonkaiTeamsWarehouse
from clean_data import clear_data_from_warehouse


# --- CONFIGURATION ---
DB = "honkai_star_rail_stats2.duckdb"
VER = "3.6.3.2"  # Version to delete
curr_v = "3.6.3"    # Version to add

# --- 1. CLEANUP ---
MODES_TO_DELETE = ["APOC"] 
clear_data_from_warehouse(DB, VER, target_modes=MODES_TO_DELETE)

# --- 2. RUN PIPELINES ---
# These will insert the 3.6.3 data into your tables
arche = HonkaiArchetypeWarehouse()
arche.run(target_version=curr_v,target_mode="APOC")
arche.run_dual(target_version=curr_v,target_mode="APOC")

teams = HonkaiTeamsWarehouse()
teams.run(target_version=curr_v,target_mode="APOC")
teams.run_dual(target_version=curr_v,target_mode="APOC")

chars = HonkaiCharacterWarehouse()
chars.run(target_version=curr_v,target_mode="APOC")

# --- 3. GLOBAL REORDERING ---
def reorder_all_tables_by_samples_desc(db_name):
    import duckdb
    conn = duckdb.connect(db_name)
    
    # Identify all tables with 'version' and 'Samples' columns
    tables = conn.execute("""
        SELECT table_name FROM information_schema.columns 
        WHERE column_name IN ('version', 'Samples')
        GROUP BY table_name HAVING COUNT(*) = 2
    """).fetchall()
    
    for (table,) in tables:
        print(f"Sorting {table}: Version ASC, Samples DESC...")
        try:
            # Create a sorted copy
            conn.execute(f"""
                CREATE TABLE {table}_sorted AS 
                SELECT * FROM {table} 
                ORDER BY version ASC, Samples DESC
            """)
            # Replace old table with sorted version
            conn.execute(f"DROP TABLE {table}")
            conn.execute(f"ALTER TABLE {table}_sorted RENAME TO {table}")
        except Exception as e:
            print(f"Failed to sort {table}: {e}")
            
    conn.close()
    print("Database reordering complete.")

# Run the sorter
reorder_all_tables_by_samples_desc(DB)