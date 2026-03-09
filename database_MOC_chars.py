import sqlite3
import pandas as pd
import warnings
import ast

# Importing your specific statistics classes
from Appearance_rate import HonkaiStatistics

# Suppress runtime warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)

# --- CONFIGURATION ---
DB_NAME = "honkai_star_rail_stats2.db"

def clean_and_standardize_char(df, version, eidolon, node, floor):
    """
    Standardizes Character-specific columns, handles cycle-to-score mapping,
    and cleans up eidolon distribution headers.
    """
    # 1. Add Metadata
    df['version'] = version
    df['mode'] = "MOC"
    df['floor'] = floor
    df['requested_eidolon_level'] = eidolon # The filter used for the scrape
    df['node'] = node
    
    # 2. Map Cycle variations to standardized Score names
    # This keeps it consistent with your warehouse structure
    rename_map = {
        'Min Cycles': 'Min_Score',
        '25th Percentile Cycles': 'Percentile_25',
        'Median Cycles': 'Median_Score',
        '75th Percentile Cycles': 'Percentile_75',
        'Average Cycles': 'Average_Score',
        'Std Dev Cycles': 'Std_Dev',
        'Max Cycles': 'Max_Score',
        'Appearance Rate (%)': 'Appearance_Rate_pct'
    }
    df.rename(columns=rename_map, inplace=True)
    
    # 3. Clean column names for SQL compatibility
    # Specifically targets 'Eidolon 0 (%)' -> 'Eidolon_0_pct' and removes dots/spaces
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

def run_character_pipeline():
    conn = sqlite3.connect(DB_NAME)
    
    # Versions list for MOC Character Analysis
    moc_versions = [
        "2.3.3", "2.4.3", "2.5.3", "2.6.3", "2.7.3", 
        "3.1.2", "3.2.2", "3.3.2", "3.4.2", "3.5.2", 
        "3.6.2", "3.7.2", "3.8.2", "4.0.1", "4.0.2"
    ]
    
    current_floor = 12
    
    print(f"Starting Character Pipeline... Database: {DB_NAME}")
    
    for v in moc_versions:
        for e in [0, 1, 2, 6]:
            for g in [0, 1, 2]:
                try:
                    # Initialize scraper
                    h = HonkaiStatistics(version=v, floor=current_floor, by_ed=e, node=g)
                    
                    # Using the character-specific function
                    df = h.print_appearance_rate_by_char(output=False)
                    
                    if df is not None and not df.empty:
                        # Clean and Standardize
                        df_clean = clean_and_standardize_char(df, v, e, g, current_floor)
                        
                        # Save to a dedicated character table
                        df_clean.to_sql('character_stats', conn, if_exists='append', index=False)
                        
                        print(f"Success: {v} | Char Stats | E{e} | Node {g}")
                    else:
                        print(f"No Data: {v} | E{e} | Node {g}")
                        
                except Exception as ex:
                    print(f"Error at {v}, E{e}, G{g}: {ex}")

    conn.close()
    print(f"\nCharacter Pipeline Finished. Saved to table: character_stats")

if __name__ == "__main__":
    run_character_pipeline()