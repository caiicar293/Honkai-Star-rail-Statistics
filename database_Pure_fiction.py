import sqlite3
import pandas as pd
import warnings

# Importing your specific Pure Fiction statistics class
from Appearance_rate_Pure_fiction import HonkaiStatistics_Pure

# Suppress runtime warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)

# --- CONFIGURATION ---
DB_NAME = "honkai_star_rail_stats2.db"

def clean_and_standardize_pure(df, version, eidolon, node, floor):
    """
    Standardizes Pure Fiction column names and adds metadata.
    """
    # 1. Add Metadata
    df['version'] = version
    df['mode'] = "PURE_FICTION"
    df['floor'] = floor
    df['eidolon_level'] = eidolon
    df['node'] = node
    
    # 2. Rename columns for SQL compatibility (spaces to underscores, remove symbols)
    # Pure Fiction usually uses "Score" already, so we just clean the formatting.
    rename_map = {
        'Appearance Rate (%)': 'Appearance_Rate_pct',
        'Min Score': 'Min_Score',
        'Median Score': 'Median_Score',
        'Average Score': 'Average_Score',
        'Max Score': 'Max_Score'
    }
    df.rename(columns=rename_map, inplace=True)
    
    # Clean special characters from any remaining column names
    df.columns = [c.replace(' ', '_').replace('(', '').replace(')', '').replace('%', 'pct') for c in df.columns]
    
    # 3. Handle Archetype string conversion
    if 'Archetype' in df.columns:
        df['Archetype'] = df['Archetype'].astype(str)
        
    return df

def run_pure_fiction_pipeline():
    # Connect to the same database file
    conn = sqlite3.connect(DB_NAME)
    
    # Pure Fiction specific versions
    pure_versions = [
        "2.3.2", "2.4.2", "2.5.2", "2.6.2", "2.7.2", 
        "3.1.1", "3.2.1", "3.3.1", "3.4.1", "3.5.1", 
        "3.6.1", "3.7.1", "3.8.1", "3.8.4", "4.0.2"
    ]
    
    current_floor = 4  # Standard max floor for Pure Fiction
    
    print(f"Starting Pure Fiction Pipeline... Floor: {current_floor} | Database: {DB_NAME}")
    
    for v in pure_versions:
        for e in [0, 1, 2, 6]:
            for g in [0, 1, 2]:
                try:
                    # Using the specialized Pure Fiction class
                    h = HonkaiStatistics_Pure(version=v, floor=current_floor, by_ed=e, node=g)
                    df = h.print_archetypes(output=False)
                    
                    if df is not None and not df.empty:
                        df_clean = clean_and_standardize_pure(df, v, e, g, current_floor)
                        
                        # Save to a NEW table/subheader called 'pure_fiction_stats'
                        df_clean.to_sql('pure_fiction_stats', conn, if_exists='append', index=False)
                        
                        print(f"Success: {v} | Floor {current_floor} | E{e} | Node {g}")
                    else:
                        print(f"No Data: {v} | Floor {current_floor} | E{e} | Node {g}")
                        
                except Exception as ex:
                    print(f"Error at {v}, E{e}, G{g}: {ex}")

    conn.close()
    print(f"\nPure Fiction Pipeline Finished. Data saved to table: pure_fiction_stats")

if __name__ == "__main__":
    run_pure_fiction_pipeline()