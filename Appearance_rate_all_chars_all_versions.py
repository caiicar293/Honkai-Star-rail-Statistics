import warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)

import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter
import os
import ast
from Appearance_rate_Apocalytic_Shadow import HonkaiStatistics_APOC
from Appearance_rate_Pure_fiction import HonkaiStatistics_Pure
from Appearance_rate import HonkaiStatistics
from Appearance_rate_all_time_char import HonkaiAnalyzer




versions = ["2.3.3", "2.4.3", "2.5.3", "2.6.3", "2.7.3", "3.1.2", "3.2.2","3.3.2","3.4.2","3.5.2","3.6.2","3.7.2","3.8.2","4.0.1","4.0.2"]
all_dfs = []
g = HonkaiStatistics(version=versions[-1], floor=12, by_ed=6)
b = g.chars
# 1. Collect data from all versions
for v in versions:
    h = HonkaiStatistics(version=v, floor=12, by_ed=0)
    df_version = h.print_archetypes(output=False)
    all_dfs.append(df_version)

# 2. Combine into one massive table
master_df = pd.concat(all_dfs, ignore_index=True)

# 3. Create 'Weighted Cycles' to prepare for the true average
# Sum of Cycles = Average * Samples
master_df['Weighted_Cycles'] = master_df['Average Cycles'] * master_df['Samples']
master_df['Archetype']= master_df['Archetype'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)
master_df['Archetype'] = master_df['Archetype'].apply(lambda x: tuple(sorted(x, key=lambda x :b.get(x,{}).get('Index', 999))) if type(x)==tuple else x)

# 4. Group by Character and Aggregate
aggregated = master_df.groupby('Archetype').agg({
    'Samples': 'sum',
    'Weighted_Cycles': 'sum',
    'Appearance Rate (%)': 'mean',
    'Min Cycles': 'min',
    'Max Cycles': 'max'
}).reset_index()

# 5. Final Calculation: True Average Cycles
aggregated['Avg Cycles'] = aggregated['Weighted_Cycles'] / aggregated['Samples']

# 6. Final Polish: Sort by performance (lower cycles is better)
final_table = aggregated.drop(columns=['Weighted_Cycles'])
final_table = final_table.sort_values(by='Avg Cycles').round(6)


print(final_table.to_string(index=False))