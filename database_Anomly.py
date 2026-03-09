import sqlite3
import pandas as pd
import warnings

# Importing your specific Anomaly statistics class
from Appearance_rate_anomaly import HonkaiStatistics_Anomaly

# Suppress runtime warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)

# --- CONFIGURATION ---
DB_NAME = "honkai_star_rail_stats2.db"

def clean_and_standardize_anomaly(df, version, eidolon, floor):
    """
    Standardizes Anomaly-specific column names and adds metadata.
    Note: No 'node' parameter is used for Anomaly.
    """
    # 1. Add Metadata
    df['version'] = version
    df['mode'] = "ANOMALY"
    df['floor'] = floor
    df['eidolon_level'] = eidolon
    
    # 2. Rename columns for SQL compatibility
    # Anomaly performance is usually measured in Score/Cycles; we standardize to Score.
    rename_map = {
        'Appearance Rate (%)': 'Appearance_Rate_pct',
        'Min Cycles': 'Min_Score',
        'Median Cycles': 'Median_Score',
        'Average Cycles': 'Average_Score',
        'Max Cycles': 'Max_Score'
    }
    df.rename(columns=rename_map, inplace=True)
    
    # Clean up any remaining spaces or parentheses in headers
    df.columns = [c.replace(' ', '_').replace('(', '').replace(')', '').replace('%', 'pct') for c in df.columns]
    
    # 3. Handle Archetype string conversion for SQL
    if 'Archetype' in df.columns:
        df['Archetype'] = df['Archetype'].astype(str)
        
    return df

def run_anomaly_pipeline():
    # Connect to the main database file
    conn = sqlite3.connect(DB_NAME)
    
    # Specific versions allowed for Anomaly mode
    anomaly_versions = ["3.6.3", "3.7.3", "3.8.4", "4.0.2"]
    
    # Floor possibilities for Anomaly
    floors = [0, 1, 2, 3, 4]
    
    # Eidolon levels to track
    eidolons = [0, 1, 2, 6]
    
    print(f"Starting Anomaly Pipeline... Database: {DB_NAME}")
    
    for v in anomaly_versions:
        for f in floors:
            for e in eidolons:
                try:
                    # Initialize the Anomaly class (No 'node' parameter as requested)
                    h = HonkaiStatistics_Anomaly(version=v, floor=f, by_ed=e)
                    df = h.print_archetypes(output=False)
                    
                    if df is not None and not df.empty:
                        df_clean = clean_and_standardize_anomaly(df, v, e, f)
                        
                        # Save to a NEW table/subheader called 'anomaly_stats'
                        df_clean.to_sql('anomaly_stats', conn, if_exists='append', index=False)
                        
                        print(f"Success: Ver {v} | Floor {f} | E{e}")
                    else:
                        print(f"No Data: Ver {v} | Floor {f} | E{e}")
                        
                except Exception as ex:
                    print(f"Error at Ver {v}, Floor {f}, E{e}: {ex}")

    conn.close()
    print(f"\nAnomaly Pipeline Finished. Data saved to table: anomaly_stats")

if __name__ == "__main__":
    run_anomaly_pipeline()