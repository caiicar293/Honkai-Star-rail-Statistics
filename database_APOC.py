import sqlite3
import pandas as pd
import warnings

# Importing your specific Apocalyptic Shadow statistics class
from Appearance_rate_Apocalytic_Shadow import HonkaiStatistics_APOC

# Suppress runtime warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)

# --- CONFIGURATION ---
DB_NAME = "honkai_star_rail_stats2.db"

def clean_and_standardize_apoc(df, version, eidolon, node, floor):
    """
    Standardizes Apocalyptic Shadow column names and adds metadata.
    """
    # 1. Add Metadata
    df['version'] = version
    df['mode'] = "APOC"
    df['floor'] = floor
    df['eidolon_level'] = eidolon
    df['node'] = node
    
    # 2. Rename columns for SQL compatibility
    # APOC usually focuses on Score, so we standardize headers here.
    rename_map = {
        'Appearance Rate (%)': 'Appearance_Rate_pct',
        'Min Score': 'Min_Score',
        'Median Score': 'Median_Score',
        'Average Score': 'Average_Score',
        'Max Score': 'Max_Score'
    }
    df.rename(columns=rename_map, inplace=True)
    
    # Clean up any remaining spaces or parentheses in headers
    df.columns = [c.replace(' ', '_').replace('(', '').replace(')', '').replace('%', 'pct') for c in df.columns]
    
    # 3. Handle Archetype string conversion for SQL
    if 'Archetype' in df.columns:
        df['Archetype'] = df['Archetype'].astype(str)
        
    return df

def run_apoc_pipeline():
    # Connect to the main database file
    conn = sqlite3.connect(DB_NAME)
    
    # Apocalyptic Shadow specific versions from your list
    apoc_versions = [
        "2.3.1", "2.4.1", "2.5.1", "2.6.1", "2.7.1", 
        "3.0.3", "3.1.3", "3.2.3", "3.3.3", "3.4.3", 
        "3.5.3", "3.6.3.2", "3.7.3", "3.8.3", "4.0.2"
    ]
    
    current_floor = 4  # Standard max difficulty floor for APOC
    
    print(f"Starting APOC Pipeline... Floor: {current_floor} | Database: {DB_NAME}")
    
    for v in apoc_versions:
        for e in [0, 1, 2, 6]:
            for g in [0, 1, 2]:
                try:
                    # Using the specialized APOC class
                    h = HonkaiStatistics_APOC(version=v, floor=current_floor, by_ed=e, node=g)
                    df = h.print_archetypes(output=False)
                    
                    if df is not None and not df.empty:
                        df_clean = clean_and_standardize_apoc(df, v, e, g, current_floor)
                        
                        # Save to a NEW table/subheader called 'apoc_stats'
                        df_clean.to_sql('apoc_stats', conn, if_exists='append', index=False)
                        
                        print(f"Success: {v} | Floor {current_floor} | E{e} | Node {g}")
                    else:
                        print(f"No Data: {v} | Floor {current_floor} | E{e} | Node {g}")
                        
                except Exception as ex:
                    print(f"Error at {v}, E{e}, G{g}: {ex}")

    conn.close()
    print(f"\nAPOC Pipeline Finished. Data saved to table: apoc_stats")

if __name__ == "__main__":
    run_apoc_pipeline()