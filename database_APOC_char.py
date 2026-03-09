import sqlite3
import pandas as pd
import warnings

# Importing the Apocalyptic Shadow specific statistics class
from Appearance_rate_Apocalytic_Shadow import HonkaiStatistics_APOC

# Suppress runtime warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)

# --- CONFIGURATION ---
DB_NAME = "honkai_star_rail_stats2.db"

def clean_and_standardize_char_apoc(df, version, eidolon, node, floor):
    """
    Standardizes Character-specific columns for Apocalyptic Shadow.
    Ensures columns match the unified 'character_stats' table schema.
    """
    # 1. Add Metadata
    df['version'] = version
    df['mode'] = "APOC"
    df['floor'] = floor
    df['requested_eidolon_level'] = eidolon
    df['node'] = node
    
    # 2. Map Column names to match the master 'character_stats' schema
    # APOC uses Score metrics (Action Value based)
    rename_map = {
        'Appearance Rate (%)': 'Appearance_Rate_pct',
        'Min Score': 'Min_Score',
        '25th Percentile': 'Percentile_25',
        'Median Score': 'Median_Score',
        '75th Percentile': 'Percentile_75',
        'Average Score': 'Average_Score',
        'Std Dev Score': 'Std_Dev',
        'Max Score': 'Max_Score'
    }
    df.rename(columns=rename_map, inplace=True)
    
    # 3. Clean column names for SQL (handle Eidolon percentages and special characters)
    df.columns = [
        c.replace(' ', '_')
         .replace('(', '')
         .replace(')', '')
         .replace('%', 'pct')
         .replace('•', '') 
         .strip() 
        for c in df.columns
    ]
    
    return df

def run_apoc_character_pipeline():
    conn = sqlite3.connect(DB_NAME)
    
    # Apocalyptic Shadow specific versions
    apoc_versions = [
        "2.3.1", "2.4.1", "2.5.1", "2.6.1", "2.7.1", 
        "3.0.3", "3.1.3", "3.2.3", "3.3.3", "3.4.3", 
        "3.5.3", "3.6.3.2", "3.7.3", "3.8.3", "4.0.2"
    ]
    
    current_floor = 4  # APOC max difficulty floor
    
    print(f"Starting APOC Character Pipeline... Database: {DB_NAME}")
    
    for v in apoc_versions:
        for e in [0, 1, 2, 6]:
            for g in [0, 1, 2]:
                try:
                    # Initialize APOC scraper
                    h = HonkaiStatistics_APOC(version=v, floor=current_floor, by_ed=e, node=g)
                    
                    # Target character-level data
                    df = h.print_appearance_rate_by_char(output=False)
                    
                    if df is not None and not df.empty:
                        # Clean and Standardize to match the master character table format
                        df_clean = clean_and_standardize_char_apoc(df, v, e, g, current_floor)
                        
                        # Save to the unified character_stats table
                        df_clean.to_sql('character_stats', conn, if_exists='append', index=False)
                        
                        print(f"Success: {v} | APOC Char Stats | E{e} | Node {g}")
                    else:
                        print(f"No Data: {v} | E{e} | Node {g}")
                        
                except Exception as ex:
                    print(f"Error at {v}, E{e}, G{g}: {ex}")

    conn.close()
    print(f"\nAPOC Character Pipeline Finished. Saved to table: character_stats")

if __name__ == "__main__":
    run_apoc_character_pipeline()