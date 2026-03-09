
import warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)

import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter
import os
from multiprocessing import Pool, cpu_count

from Appearance_rate_Apocalytic_Shadow import HonkaiStatistics_APOC
from Appearance_rate_Pure_fiction import HonkaiStatistics_Pure
from Appearance_rate import HonkaiStatistics
from Appearance_rate_anomaly import HonkaiStatistics_Anomaly


import sqlite3
import pandas as pd
import warnings

# Suppress runtime warnings from heavy data processing
warnings.filterwarnings("ignore", category=RuntimeWarning)

# --- CONFIGURATION ---
DB_NAME = "honkai_star_rail_stats2.db"

def clean_and_standardize_moc(df, version, eidolon, node, floor):
    """
    Standardizes MOC-specific Cycle columns and adds metadata including Floor.
    """
    # 1. Add Metadata (Now including Floor)
    df['version'] = version
    df['mode'] = "MOC"
    df['floor'] = floor
    df['eidolon_level'] = eidolon
    df['node'] = node
    
    # 2. Map MOC "Cycles" to standardized "Score" names
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
    
    # Clean up column names: remove spaces/special chars for SQL compatibility
    df.columns = [c.replace(' ', '_').replace('(', '').replace(')', '').replace('%', 'pct') for c in df.columns]
    
    # 3. Handle Archetype string conversion for SQL
    if 'Archetype' in df.columns:
        df['Archetype'] = df['Archetype'].astype(str)
        
    return df

def run_moc_pipeline():
    conn = sqlite3.connect(DB_NAME)
    
    # Target versions for MOC
    moc_versions = [
        "2.3.3", "2.4.3", "2.5.3", "2.6.3", "2.7.3", 
        "3.1.2", "3.2.2", "3.3.2", "3.4.2", "3.5.2", 
        "3.6.2", "3.7.2", "3.8.2", "4.0.1", "4.0.2"
    ]
    
    current_floor = 12 # Set your target floor here
    
    print(f"Starting MOC Pipeline... Floor: {current_floor} | Database: {DB_NAME}")
    
    for v in moc_versions:
        for e in [0, 1, 2, 6]:
            for g in [0, 1, 2]:
                try:
                    # Initialize your scraper class
                    h = HonkaiStatistics(version=v, floor=current_floor, by_ed=e, node=g)
                    df = h.print_archetypes(output=False)
                    
                    if df is not None and not df.empty:
                        # Process, standardize, and add Floor data
                        df_clean = clean_and_standardize_moc(df, v, e, g, current_floor)
                        
                        # Append to SQL
                        df_clean.to_sql('moc_stats', conn, if_exists='append', index=False)
                        
                        print(f"Success: {v} | Floor {current_floor} | E{e} | Node {g}")
                    else:
                        print(f"No Data: {v} | Floor {current_floor} | E{e} | Node {g}")
                        
                except Exception as ex:
                    print(f"Error at {v}, E{e}, G{g}: {ex}")

    conn.close()
    print(f"\nPipeline Finished. Data saved to {DB_NAME}")

if __name__ == "__main__":
    run_moc_pipeline()