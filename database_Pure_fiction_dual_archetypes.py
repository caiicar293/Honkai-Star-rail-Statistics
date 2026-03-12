import sqlite3
import pandas as pd
import warnings
import ast

# Importing the Pure Fiction specific statistics class
from Appearance_rate_Pure_fiction import HonkaiStatistics_Pure

# Suppress runtime warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)

# --- CONFIGURATION ---
DB_NAME = "honkai_star_rail_stats2.db"

def clean_and_standardize_both_sides_pf(df, version, eidolon, floor):
    """
    Standardizes the dual-archetype format for Pure Fiction.
    Ensures scores are mapped correctly for synergistic analysis.
    """
    # 1. Add Metadata
    df['version'] = version
    df['mode'] = "PURE_FICTION_BOTH_SIDES"
    df['floor'] = floor
    df['requested_eidolon_level'] = eidolon
    
    # 2. Map Column names to match the master schema
    # Pure Fiction uses 'Score' labels, we keep this consistent with the MOC table structure
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
    
    # 3. Clean up column names (Standardizing spaces and underscores)
    df.columns = [c.replace(' ', '_').replace('(', '').replace(')', '').replace('%', 'pct') for c in df.columns]

    # 4. Handle Archetype String Conversion
    for col in ['Archetype_1', 'Archetype_2']:
        if col in df.columns:
            df[col] = df[col].astype(str)
        
    return df

def run_pure_fiction_both_sides_pipeline():
    conn = sqlite3.connect(DB_NAME)
    
    # Pure Fiction specific versions
    pf_versions = [
        "2.3.2", "2.4.2", "2.5.2", "2.6.2", "2.7.2", 
        "3.1.1", "3.2.1", "3.3.1", "3.4.1", "3.5.1", 
        "3.6.1", "3.7.1", "3.8.1", "3.8.4", "4.0.2"
    ]
    
    current_floor = 4 # Max PF Floor
    
    print(f"Starting Pure Fiction Both Sides Pipeline... Database: {DB_NAME}")
    
    for v in pf_versions:
        for e in [0, 1, 2, 6]:
            try:
                # Initialize Pure Fiction scraper
                h = HonkaiStatistics_Pure(version=v, floor=current_floor, by_ed=e)
                
                # Fetch dual-side archetype data
                df = h.print_archetypes_both_sides(output=False)
                
                if df is not None and not df.empty:
                    # Clean and standardize
                    df_clean = clean_and_standardize_both_sides_pf(df, v, e, current_floor)
                    
                    # Save to a NEW separate table: archetypes_both_sides_pf
                    df_clean.to_sql('archetypes_both_sides_pf', conn, if_exists='append', index=False)
                    
                    print(f"Success: {v} | PF Both Sides | E{e}")
                else:
                    print(f"No Data: {v} | E{e}")
                    
            except Exception as ex:
                print(f"Error at {v}, E{e}: {ex}")

    conn.close()
    print(f"\nPF Both Sides Pipeline Finished. Saved to table: archetypes_both_sides_pf")

if __name__ == "__main__":
    run_pure_fiction_both_sides_pipeline()