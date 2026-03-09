import sqlite3
import pandas as pd
import warnings

# Importing the Anomaly specific statistics class
from Appearance_rate_anomaly import HonkaiStatistics_Anomaly

# Suppress runtime warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)

# --- CONFIGURATION ---
DB_NAME = "honkai_star_rail_stats2.db"

def clean_and_standardize_char_anomaly(df, version, eidolon, floor):
    """
    Standardizes Character-specific columns for Anomaly mode.
    Sets node to None (NULL in SQL) for table compatibility.
    """
    # 1. Add Metadata
    df['version'] = version
    df['mode'] = "ANOMALY"
    df['floor'] = floor
    df['requested_eidolon_level'] = eidolon
    df['node'] = None  # Explicitly setting node to NULL as Anomaly doesn't have sides
    
    # 2. Map Column names to match the master 'character_stats' schema
    # Anomaly usually outputs Cycles; we standardize these to Score headers.
    rename_map = {
        'Appearance Rate (%)': 'Appearance_Rate_pct',
        'Min Cycles': 'Min_Score',
        '25th Percentile Cycles': 'Percentile_25',
        'Median Cycles': 'Median_Score',
        '75th Percentile Cycles': 'Percentile_75',
        'Average Cycles': 'Average_Score',
        'Std Dev Cycles': 'Std_Dev',
        'Max Cycles': 'Max_Score'
    }
    df.rename(columns=rename_map, inplace=True)
    
    # 3. Clean column names for SQL compatibility
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

def run_anomaly_character_pipeline():
    conn = sqlite3.connect(DB_NAME)
    
    # Anomaly specific versions
    anomaly_versions = ["3.6.3", "3.7.3", "3.8.4", "4.0.2"]
    
    # Anomaly Floor loop (0-4)
    floors = [0, 1, 2, 3, 4]
    
    print(f"Starting Anomaly Character Pipeline... Database: {DB_NAME}")
    
    for v in anomaly_versions:
        for f in floors:
            for e in [0, 1, 2, 6]:
                try:
                    # Initialize Anomaly scraper (No node parameter)
                    h = HonkaiStatistics_Anomaly(version=v, floor=f, by_ed=e)
                    
                    # Target character-level data
                    df = h.print_appearance_rate_by_char(output=False)
                    
                    if df is not None and not df.empty:
                        # Clean and Standardize
                        df_clean = clean_and_standardize_char_anomaly(df, v, e, f)
                        
                        # Save to the unified character_stats table
                        df_clean.to_sql('character_stats', conn, if_exists='append', index=False)
                        
                        print(f"Success: {v} | Floor {f} | Anomaly Char Stats | E{e}")
                    else:
                        print(f"No Data: {v} | Floor {f} | E{e}")
                        
                except Exception as ex:
                    print(f"Error at {v}, Floor {f}, E{e}: {ex}")

    conn.close()
    print(f"\nAnomaly Character Pipeline Finished. Saved to table: character_stats")

if __name__ == "__main__":
    run_anomaly_character_pipeline()