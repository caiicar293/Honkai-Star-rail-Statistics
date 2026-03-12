import sqlite3
import pandas as pd
import warnings
import ast

# Importing the Apocalyptic Shadow specific statistics class
from Appearance_rate_Apocalytic_Shadow import HonkaiStatistics_APOC

# Suppress runtime warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)

# --- CONFIGURATION ---
DB_NAME = "honkai_star_rail_stats2.db"

def clean_and_standardize_both_sides_apoc(df, version, eidolon, floor):
    """
    Standardizes the dual-archetype format for Apocalyptic Shadow.
    Maps Action Value scores to the unified warehouse structure.
    """
    # 1. Add Metadata
    df['version'] = version
    df['mode'] = "APOC_BOTH_SIDES"
    df['floor'] = floor
    df['requested_eidolon_level'] = eidolon
    
    # 2. Map Column names
    # APOC uses Score (Action Value based), similar to PF naming conventions
    rename_map = {
        'Appearance Rate (%)': 'Appearance_Rate_pct',
        '25th Percentile': 'Percentile_25',
        '75th Percentile': 'Percentile_75',
        'Min Score': 'Min_Score',
        'Median Score': 'Median_Score',
        'Average Score': 'Average_Score',
        'Max Score': 'Max_Score'
    }
    df.rename(columns=rename_map, inplace=True)
    
    # 3. Clean up column names for SQL compatibility
    df.columns = [c.replace(' ', '_').replace('(', '').replace(')', '').replace('%', 'pct') for c in df.columns]

    # 4. Handle Archetype String Conversion
    for col in ['Archetype_1', 'Archetype_2']:
        if col in df.columns:
            df[col] = df[col].astype(str)
        
    return df

def run_apoc_both_sides_pipeline():
    conn = sqlite3.connect(DB_NAME)
    
    # Apocalyptic Shadow specific versions
    apoc_versions = [
        "2.3.1", "2.4.1", "2.5.1", "2.6.1", "2.7.1", 
        "3.0.3", "3.1.3", "3.2.3", "3.3.3", "3.4.3", 
        "3.5.3", "3.6.3.2", "3.7.3", "3.8.3", "4.0.2"
    ]
    
    current_floor = 4 # Max APOC Floor
    
    print(f"Starting APOC Both Sides Pipeline... Database: {DB_NAME}")
    
    for v in apoc_versions:
        for e in [0, 1, 2, 6]:
            try:
                # Initialize APOC scraper
                h = HonkaiStatistics_APOC(version=v, floor=current_floor, by_ed=e)
                
                # Fetch dual-side archetype data
                df = h.print_archetypes_both_sides(output=False)
                
                if df is not None and not df.empty:
                    # Clean and standardize
                    df_clean = clean_and_standardize_both_sides_apoc(df, v, e, current_floor)
                    
                    # Save to its own separate table: archetypes_both_sides_apoc
                    df_clean.to_sql('archetypes_both_sides_apoc', conn, if_exists='append', index=False)
                    
                    print(f"Success: {v} | APOC Both Sides | E{e}")
                else:
                    print(f"No Data: {v} | E{e}")
                    
            except Exception as ex:
                print(f"Error at {v}, E{e}: {ex}")

    conn.close()
    print(f"\nAPOC Both Sides Pipeline Finished. Saved to table: archetypes_both_sides_apoc")

if __name__ == "__main__":
    run_apoc_both_sides_pipeline()