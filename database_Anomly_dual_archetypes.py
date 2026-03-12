import sqlite3
import pandas as pd
import warnings
import ast

# Importing the Anomaly specific statistics class
from Appearance_rate_anomaly import HonkaiStatistics_Anomaly

# Suppress runtime warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)

# --- CONFIGURATION ---
DB_NAME = "honkai_star_rail_stats2.db"

def clean_and_standardize_triple_anomaly(df, version, eidolon, floor):
    """
    Standardizes the triple-archetype format for Anomaly Arbitratio.
    Handles Archetype 1, 2, and 3.
    """
    # 1. Add Metadata
    df['version'] = version
    df['mode'] = "ANOMALY_TRIPLE_SIDES"
    df['floor'] = floor
    df['requested_eidolon_level'] = eidolon
    
    # 2. Map Column names to match the master schema
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

    # 4. Handle Archetype String Conversion for all three teams
    for col in ['Archetype_1', 'Archetype_2', 'Archetype_3']:
        if col in df.columns:
            df[col] = df[col].astype(str)
        
    return df

def run_anomaly_triple_pipeline():
    conn = sqlite3.connect(DB_NAME)
    
    # Anomaly specific versions
    anomaly_versions = ["3.6.3", "3.7.3", "3.8.4", "4.0.2"]
    
    # Per your instruction, floor is always 0 for this specific side-by-side analysis
    target_floor = 0 
    
    print(f"Starting Anomaly Triple-Archetype Pipeline... Floor: {target_floor}")
    
    for v in anomaly_versions:
        # Loop through target eidolon levels
        for e in [0, 1, 2, 6]:
            try:
                # Initialize Anomaly scraper
                h = HonkaiStatistics_Anomaly(version=v, floor=target_floor, by_ed=e)
                
                # Fetch the triple-side archetype data
                df = h.print_archetypes_both_sides(output=False)
                
                if df is not None and not df.empty:
                    # Clean and standardize (Handling the 3-team structure)
                    df_clean = clean_and_standardize_triple_anomaly(df, v, e, target_floor)
                    
                    # Save to its own separate table: archetypes_triple_anomaly
                    df_clean.to_sql('archetypes_triple_anomaly', conn, if_exists='append', index=False)
                    
                    print(f"Success: {v} | Triple Anomaly | E{e}")
                else:
                    print(f"No Data: {v} | E{e}")
                    
            except Exception as ex:
                print(f"Error at {v}, E{e}: {ex}")

    conn.close()
    print(f"\nAnomaly Triple Pipeline Finished. Saved to table: archetypes_triple_anomaly")

if __name__ == "__main__":
    run_anomaly_triple_pipeline()