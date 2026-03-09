import sqlite3
import pandas as pd
import warnings

# Importing the Pure Fiction specific statistics class
from Appearance_rate_Pure_fiction import HonkaiStatistics_Pure

# Suppress runtime warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)

# --- CONFIGURATION ---
DB_NAME = "honkai_star_rail_stats2.db"

def clean_and_standardize_char_pf(df, version, eidolon, node, floor):
    """
    Standardizes Character-specific columns for Pure Fiction.
    Ensures columns match the 'character_stats' table schema.
    """
    # 1. Add Metadata
    df['version'] = version
    df['mode'] = "PURE_FICTION"
    df['floor'] = floor
    df['requested_eidolon_level'] = eidolon
    df['node'] = node
    
    # 2. Map Column names to match the master 'character_stats' schema
    # Pure Fiction usually uses 'Score' instead of 'Cycles'
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

def run_pure_fiction_character_pipeline():
    conn = sqlite3.connect(DB_NAME)
    
    # Pure Fiction specific version list
    pf_versions = [
        "2.3.2", "2.4.2", "2.5.2", "2.6.2", "2.7.2", 
        "3.1.1", "3.2.1", "3.3.1", "3.4.1", "3.5.1", 
        "3.6.1", "3.7.1", "3.8.1", "3.8.4", "4.0.2"
    ]
    
    current_floor = 4 # Pure Fiction max floor
    
    print(f"Starting Pure Fiction Character Pipeline... Database: {DB_NAME}")
    
    for v in pf_versions:
        for e in [0, 1, 2, 6]:
            for g in [0, 1, 2]:
                try:
                    # Initialize PF scraper
                    h = HonkaiStatistics_Pure(version=v, floor=current_floor, by_ed=e, node=g)
                    
                    # Target character-level data
                    df = h.print_appearance_rate_by_char(output=False)
                    
                    if df is not None and not df.empty:
                        # Clean and Standardize to match MoC format
                        df_clean = clean_and_standardize_char_pf(df, v, e, g, current_floor)
                        
                        # Save to the SAME table as MoC
                        df_clean.to_sql('character_stats', conn, if_exists='append', index=False)
                        
                        print(f"Success: {v} | PF Char Stats | E{e} | Node {g}")
                    else:
                        print(f"No Data: {v} | E{e} | Node {g}")
                        
                except Exception as ex:
                    print(f"Error at {v}, E{e}, G{g}: {ex}")

    conn.close()
    print(f"\nPF Character Pipeline Finished. Saved to table: character_stats")

if __name__ == "__main__":
    run_pure_fiction_character_pipeline()