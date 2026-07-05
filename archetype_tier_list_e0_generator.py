import duckdb
import os
import pandas as pd
import json
import argparse
from dotenv import load_dotenv

# Load environment variables to get the database file path
load_dotenv()
db_file = os.getenv("DB_File")

if not db_file:
    raise ValueError("DB_File environment variable not set. Please ensure .env is configured.")

def assign_tier(row):
    mode = row['Game_Mode']
    score = row['Weighted_Avg_Score']

    # MoC and Anomaly use Cycles (Lower = Better)
    # PF and APOC use Points (Higher = Better)
    
    if mode == 'MOC':
        if score < 5.5: return 'T0'
        elif score < 6.5: return 'T0.5'
        elif score < 7.5: return 'T1'
        elif score < 8.5: return 'T1.5'
        else: return 'T2'
    elif mode == 'ANOMALY_F0':
        if score < 2.0: return 'T0'
        elif score < 3.0: return 'T0.5'
        elif score < 4.0: return 'T1'
        elif score < 5.0: return 'T1.5'
        else: return 'T2'
    elif mode == 'ANOMALY_F4':
        if score < 3.0: return 'T0'
        elif score < 4.0: return 'T0.5'
        elif score < 4.5: return 'T1'
        elif score < 5.5: return 'T1.5'
        else: return 'T2'
    elif mode == 'PURE_FICTION':
        if score > 35000: return 'T0'
        elif score >= 30000: return 'T0.5'
        elif score >= 27000: return 'T1'
        elif score >= 24000: return 'T1.5'
        else: return 'T2'
    elif mode == 'APOC':
        if score > 3400: return 'T0'
        elif score >= 3100: return 'T0.5'
        elif score >= 2800: return 'T1'
        elif score >= 2500: return 'T1.5'
        else: return 'T2'
    return 'Unknown'

def extract_and_build_html(icons_path, template_path, output_path, version, eidolon):
    # Connect to the DuckDB database
    conn = duckdb.connect(db_file)
    
    # SQL query to extract the required archetype performance metrics
    sql_query = """
    SELECT 
        Game_Mode, 
        Archetype_Core, 
        Weighted_Avg_Score
    FROM archetype_recent_meta_summary
    WHERE up_to_eidolon_level = 0
    AND at_eidolon_level = 0
    AND (
        Archetype_Core IN (
            'Evanescia', 'Sparxie + Silver Wolf LV.999', 'Sparxie', 'Castorice + Evernight', 'Evernight',
            'Silver Wolf LV.999', 'Ashveil', 'Kafka + Black Swan + Hysilens', 'Firefly', 'Archer',
            'Feixiao + Ashveil', 'Aglaea', 'Jade + The Herta', 'Mydei', 'Anaxa', 'Castorice',
            'Kafka + Hysilens', 'The Herta + Anaxa', 'Boothill', 'Phainon', 'Herta + The Herta',
            'Saber', 'Rappa', 'Acheron', 'Seele', 'Silver Wolf LV.999 + Evanescia',
            'Sparxie + Evanescia', 'Welt + Silver Wolf LV.999', 'Welt + Sparxie + Silver Wolf LV.999',
            'Feixiao', 'Serval + The Herta', 'Yunli', 'Acheron + Ashveil', 'Welt + Ashveil', 'Sparxie + Evanescia'
        )
    )
    ORDER BY Game_Mode, Weighted_Avg_Score;
    """
    
    # Execute query and load into pandas DataFrame
    df = conn.execute(sql_query).df()

    # SQL query to extract amplifier performance metrics from the character-level
    # meta summary table. "Character" is aliased to "Archetype_Core" so the
    # resulting frame is shape-compatible with the archetype query above and
    # can reuse the same downstream tiering / CSV / HTML-injection logic.
    amplifier_query = """
        SELECT
            CASE 
                WHEN Game_Mode = 'ANOMALY_ALL' THEN 'ANOMALY_F0' 
                ELSE Game_Mode 
            END AS Game_Mode,
            Character AS Archetype_Core,
            ROUND(Weighted_Avg_Score, 2) AS Weighted_Avg_Score
        FROM character_recent_meta_summary
        WHERE up_to_eidolon_level = 0
        AND at_eidolon_level = 0
        AND role = 'amplifier'
        AND Simple_Avg_Appearance > 0.1
        ORDER BY Game_Mode, Weighted_Avg_Score;
        """

    # Same as above, but for sustains
    sustain_query = """
        SELECT
            CASE 
                WHEN Game_Mode = 'ANOMALY_ALL' THEN 'ANOMALY_F0' 
                ELSE Game_Mode 
            END AS Game_Mode,
            Character AS Archetype_Core,
            ROUND(Weighted_Avg_Score, 2) AS Weighted_Avg_Score
        FROM character_recent_meta_summary
        WHERE up_to_eidolon_level = 0
        AND at_eidolon_level = 0
        AND role = 'sustain'
        AND Simple_Avg_Appearance > 0.1
        ORDER BY Game_Mode, Weighted_Avg_Score;
        """

    df_amplifiers = conn.execute(amplifier_query).df()
    df_sustains = conn.execute(sustain_query).df()

    # Close connection
    conn.close()
    
    # Assign Tiers
    df['Tier'] = df.apply(assign_tier, axis=1)
    df_amplifiers['Tier'] = df_amplifiers.apply(assign_tier, axis=1)
    df_sustains['Tier'] = df_sustains.apply(assign_tier, axis=1)
    
    # Convert DataFrames directly to CSV-formatted strings (no file writing)
    csv_data_string = df.to_csv(index=False)
    csv_data_string_amplifiers = df_amplifiers.to_csv(index=False)
    csv_data_string_sustains = df_sustains.to_csv(index=False)
    
    print("DuckDB extraction and tier assignment complete.")
    
    # --- HTML INJECTION LOGIC ---
    
    # 1. Read the JSON icons
    if not os.path.exists(icons_path):
        print(f"Error: JSON file '{icons_path}' not found.")
        return
        
    with open(icons_path, 'r', encoding='utf-8') as f:
        try:
            icons_dict = json.load(f)
            icons_json_str = json.dumps(icons_dict, indent=4)
        except json.JSONDecodeError:
            print(f"Error: '{icons_path}' is not a valid JSON file.")
            return

    # 2. Read the HTML template
    if not os.path.exists(template_path):
        print(f"Error: Template file '{template_path}' not found. Make sure you saved template.html")
        return
        
    with open(template_path, 'r', encoding='utf-8') as f:
        html_template = f.read()

    # 3. Inject the data into the template
    print("Injecting data into HTML template...")
    final_html = html_template.replace('$$ICON_DATA$$', icons_json_str)
    final_html = final_html.replace('$$RAW_CSV_DATA$$', csv_data_string)
    final_html = final_html.replace('$$RAW_CSV_DATA_AMPLIFIERS$$', csv_data_string_amplifiers)
    final_html = final_html.replace('$$RAW_CSV_DATA_SUSTAINS$$', csv_data_string_sustains)
    final_html = final_html.replace('$$VERSION$$', version)
    final_html = final_html.replace('$$EIDOLON$$', eidolon)

    # 4. Write the final HTML file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(final_html)

    print(f"Success! Interactive tier list generated at: {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract from DuckDB and inject directly into Tier List HTML template.")
    parser.add_argument('--icons', default='Other Dashboards/character_icons.json', help='Path to the character icons JSON file')
    parser.add_argument('--template', default='archetype_tier_list_e0_template.html', help='Path to the HTML template')
    parser.add_argument('--output', default='docs/archetype_tierlist_interactive.html', help='Path for the final generated HTML file')
    parser.add_argument('--version', default='4.3.2', help='Game version (e.g., 4.3.2)')
    parser.add_argument('--eidolon', default='E0', help='Eidolon level (e.g., E0)')
    
    args = parser.parse_args()
    
    extract_and_build_html(args.icons, args.template, args.output, args.version, args.eidolon)