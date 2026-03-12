import sqlite3
import pandas as pd
import warnings
import ast

# Importing the MOC statistics class
from Appearance_rate import HonkaiStatistics

# Suppress runtime warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)

# --- CONFIGURATION ---
DB_NAME = "honkai_star_rail_stats2.db"

def clean_and_standardize_both_sides(df, version, eidolon, floor):
    """
    Standardizes the dual-archetype format. 
    Handles Archetype 1 and Archetype 2 separately.
    """
    # 1. Add Metadata (Node is implicitly 'Both' here)
    df['version'] = version
    df['mode'] = "MOC_BOTH_SIDES"
    df['floor'] = floor
    df['requested_eidolon_level'] = eidolon
    
    # 2. Map Column names to match the master schema
    # The dataframe provided uses 'Samples' and 'Average Score' etc.
    rename_map = {
        'Appearance Rate (%)': 'Appearance_Rate_pct',
        '25th Percentile': 'Percentile_25',
        '75th Percentile': 'Percentile_75',
    }
    df.rename(columns=rename_map, inplace=True)
    
    # 3. Clean up column names (Standardizing spaces and underscores)
    df.columns = [c.replace(' ', '_').replace('(', '').replace(')', '').replace('%', 'pct') for c in df.columns]

    # 4. Handle Archetype String Conversion
    # We ensure both columns are stored as searchable strings
    for col in ['Archetype_1', 'Archetype_2']:
        if col in df.columns:
            df[col] = df[col].astype(str)
        
    return df

def run_moc_both_sides_pipeline():
    conn = sqlite3.connect(DB_NAME)
    
    # MOC Versions for full clear analysis
    moc_versions = [
        "2.3.3", "2.4.3", "2.5.3", "2.6.3", "2.7.3", 
        "3.1.2", "3.2.2", "3.3.2", "3.4.2", "3.5.2", 
        "3.6.2", "3.7.2", "3.8.2", "4.0.1", "4.0.2"
    ]
    
    current_floor = 12
    
    print(f"Starting MOC Both Sides Pipeline... Database: {DB_NAME}")
    
    for v in moc_versions:
        # We focus on the provided Eidolon levels
        for e in [0, 1, 2, 6]:
            try:
                # Initialize scraper for the specific version/eidolon
                h = HonkaiStatistics(version=v, floor=current_floor, by_ed=e)
                
                # Using the both_sides specific function
                df = h.print_archetypes_both_sides(output=False)
                
                if df is not None and not df.empty:
                    # Clean and standardize
                    df_clean = clean_and_standardize_both_sides(df, v, e, current_floor)
                    
                    # Save to a NEW separate table
                    df_clean.to_sql('archetypes_both_sides_moc', conn, if_exists='append', index=False)
                    
                    print(f"Success: {v} | Both Sides | E{e}")
                else:
                    print(f"No Data: {v} | E{e}")
                    
            except Exception as ex:
                print(f"Error at {v}, E{e}: {ex}")

    conn.close()
    print(f"\nBoth Sides Pipeline Finished. Saved to table: archetypes_both_sides")

if __name__ == "__main__":
    run_moc_both_sides_pipeline()