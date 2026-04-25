import polars as pl
import os
import orjson
from itertools import chain
import matplotlib.pyplot as plt
pl.Config.set_tbl_rows(200)      # -1 or None shows all rows
pl.Config.set_tbl_cols(-1)      # -1 or None shows all columns
pl.Config.set_fmt_str_lengths(100)  # Prevents long strings from being cut off
class HonkaiStatistics_V2:
    def __init__(self, version, floor, node=0, by_ed=6, by_cycle=30, by_ed_inclusive=False,
                 by_ed_inclusive_combined=False, by_char=None, by_cycles_combined=30,
                 not_char=False, sustain_condition=None, star_num=None):

        self.version, self.floor, self.node = version, floor, node
        self.by_ed, self.by_cycle = by_ed, by_cycle
        self.by_ed_inclusive = by_ed_inclusive
        self.by_ed_inclusive_combined = by_ed_inclusive_combined
        self.by_cycles_combined = by_cycles_combined

        # 1. LOAD DATA VIA POLARS (Multi-threaded Parquet/CSV)
        folder = "raw_data"
        path = os.path.join(folder, f"{version}.parquet")
        path2 = os.path.join(folder, f"{version}_char.parquet")
        if os.path.exists(path):
            self.df = pl.read_parquet(path)
        else:
            url = f"https://huggingface.co/datasets/LvlUrArti/MocData/resolve/main/{version}.csv"
            self.df = pl.read_csv(url)
            os.makedirs(folder, exist_ok=True)
            self.df.write_parquet(path)
            
        if os.path.exists(path2):    
            self.char_df = pl.read_parquet(path2)
        else:
            url = f"https://huggingface.co/datasets/LvlUrArti/MocData/resolve/main/{version}_char.csv"
            self.char_df = pl.read_csv(url)
            os.makedirs(folder, exist_ok=True)
            self.char_df.write_parquet(path2)

        # 2. LOAD CHARACTER METADATA
        with open('characters.json', 'rb') as f:
            info = orjson.loads(f.read())

        # Convert metadata to a lookup LazyFrame
        self.rol = pl.DataFrame([
            {"char": k, "is_limited": v.get('availability') == 'Limited 5*',
             "is_sustain": 'sustain' in v.get('role', []),
             "is_dps": bool(set(v.get('role', [])).intersection({"dps", "specialist"})),
             "sort_index": i}
            for i, (k, v) in enumerate(info.items())
        ]).lazy()




        # Convert main DF to Lazy for optimization pipeline
        lf = self.df.lazy()
        char_lf = self.char_df.lazy()

        # 3. INITIAL FILTERING (Floor/Node/Stars)
        if self.node != 0:
            lf = lf.filter((pl.col("floor") == self.floor) & (pl.col("node") == self.node))
        else:
            lf = lf.filter(pl.col("floor") == self.floor)

        if hasattr(self, 'star_num') and self.star_num:
            lf = lf.filter(pl.col("star_num") == self.star_num)

        # 4. EIDOLON & SUSTAIN LOGIC (Vectorized)
        # Create helper flags for limited status and sustain status
        char_cols = ["ch1", "ch2", "ch3", "ch4"]
        cons_cols = ["cons1", "cons2", "cons3", "cons4"]

        # Pre-fill NaNs
        lf = lf.with_columns([pl.col(c).fill_null(-1) for c in cons_cols])

        # Get limited character names for fast check
        limited_names = self.rol.filter(pl.col("is_limited")).select("char").collect().to_series().to_list()
        sustain_names = self.rol.filter(pl.col("is_sustain")).select("char").collect().to_series().to_list()
        dps_names = self.rol.filter(pl.col("is_dps")).select("char").collect().to_series().to_list()
        char_to_index = {
              row["char"]: row["sort_index"]
              for row in self.rol.collect().to_dicts()
          }

        # Calculate max eidolon for limited units per row
        lf = lf.with_columns([
            pl.max_horizontal([
                pl.when(pl.col(char_cols[i]).is_in(limited_names)).then(pl.col(cons_cols[i])).otherwise(0)
                for i in range(4)
            ]).alias("max_eidolon"),
            pl.any_horizontal([pl.col(c).is_in(sustain_names) for c in char_cols]).alias("has_sustain")
        ])

        # Apply thresholds
        if self.by_ed_inclusive:
            lf = lf.filter(pl.col("max_eidolon") == self.by_ed)
        else:
            # Check if any limited unit exceeds by_ed
            for i in range(4):
                lf = lf.filter(~(pl.col(char_cols[i]).is_in(limited_names) & (pl.col(cons_cols[i]) > self.by_ed)))

        self.lf = lf.filter(pl.col("round_num") <= self.by_cycle)



        self._process_data(self.lf,char_lf,char_cols,cons_cols,dps_names,char_to_index)
         # If node is 0, process combined data as well

        if self.node == 0:
            # Explicitly select base columns needed for n1 and n2 before splitting
            base_cols_for_n = ["uid", "node", "round_num", "max_eidolon"] + char_cols
            lf_base_for_combined = lf.select(base_cols_for_n)

            # 1. SPLIT DATA INTO TWO NODES
            n1 = lf_base_for_combined.filter(pl.col("node") == 1).select([
                pl.col("uid"),
                pl.concat_list(char_cols).alias("n1_chars").list.eval(

                        pl.element().sort_by(
                            pl.element().replace_strict(char_to_index, default=999)
                        )),
                pl.col("round_num").alias("n1_cycles"),
                pl.col("max_eidolon").alias("n1_max_ed")
            ])

            n2 = lf_base_for_combined.filter(pl.col("node") == 2).select([
                pl.col("uid"),
                pl.concat_list(char_cols).alias("n2_chars").list.eval(

                        pl.element().sort_by(
                            pl.element().replace_strict(char_to_index, default=999)
                        )),
                pl.col("round_num").alias("n2_cycles"),
                pl.col("max_eidolon").alias("n2_max_ed")
            ])

            # 2. INNER JOIN (Only keep UIDs that completed both nodes)
            combined = n1.join(n2, on="uid", how="inner")

            # 3. CALCULATE COMBINED STATS
            combined = combined.with_columns([
                (pl.col("n1_cycles")).alias("total_cycles"),
                pl.max_horizontal(["n1_max_ed", "n2_max_ed"]).alias("combined_max_ed")
            ])

            # 4. FILTER BY THRESHOLDS
            if self.by_ed_inclusive_combined:
                combined = combined.filter(pl.col("combined_max_ed") == self.by_ed)

            self.combined = combined.filter(pl.col("total_cycles") <= self.by_cycles_combined)
            self._process_combined_data(self.combined,char_cols,cons_cols,dps_names,char_to_index)


    def _process_data(self,lf,char_lf,char_cols,cons_cols,dps_names,char_to_index):


        # 5. AGGREGATE CHARACTERS (The "Unpivot" trick)
        # Instead of a loop, we melt the 4 columns into 1 long column
        chars = self.lf.unpivot(
            index=["uid", "round_num","has_sustain"], 
            on=char_cols, 
            value_name="Character"
        ).drop("variable")

        # Unpivot values
        cons = self.lf.unpivot(
            index=["uid", "round_num","has_sustain"], 
            on=cons_cols, 
            value_name="cons"
        ).drop(["uid", "round_num","has_sustain", "variable"])    
                    
        # 1. Combine and Clean
        # We map -1 to 0 so they merge with the E0 data
        # 1. Combine and Clean
        base_data = (
            pl.concat([chars, cons], how="horizontal")
            .with_columns([
                pl.col("Character").fill_null("Empty Slot"),
                pl.col("cons").replace(-1, 0)
            ])
        )

        # Join and handle data availability labels
        base_data = (
            base_data.join(
                char_lf, 
                left_on=["uid", 'Character'], 
                right_on=["uid", 'name'], 
                how="left"
            )
            .drop(['phase', 'cons_right', 'level'])
            .with_columns([
                pl.col("weapon").fill_null("Info_not_found"),
                pl.col("artifacts").fill_null("Info_not_found"),
                pl.col("relics").fill_null("Info_not_found")
            ])
        )

        def get_performance_stats(df, group_keys):
            """
            Groups data by keys (Character/Eidolon) and gear, collecting counts 
            and a list of cycle performance for every unique build.
            """
            def rollup_gear(df, gear_col, alias):
                # Step A: Aggregate cycles per specific gear item
                return (
                    df.group_by(group_keys + [gear_col])
                    .agg([
                        pl.count("uid").alias("count"),
                        pl.col("round_num").alias("cycles") 
                    ])
                    # Step B: Roll up gear items into a List of Structs for the main group
                    .group_by(group_keys)
                    .agg(
                        pl.struct([
                            pl.col(gear_col).alias("name"), # Normalize field name inside struct
                            "count", 
                            "cycles"
                        ]).alias(alias)
                    )
                )

            # Calculate individual gear distributions
            w_df = rollup_gear(df, "weapon", "Lightcones")
            a_df = rollup_gear(df, "artifacts", "Relics")
            r_df = rollup_gear(df, "relics", "Planar_Set")

            # Final top-level stats
            is_eidolon = "cons" in group_keys
            base_stats = df.group_by(group_keys).agg([
                pl.count("uid").alias("Samples" if is_eidolon else "Total_Samples"),
                pl.col("round_num").alias("Cycles" if is_eidolon else "Total_Cycles"),
                pl.col("has_sustain").sum().alias("Sustains" if is_eidolon else "Total_Sustains"),
                pl.col("uid").unique().alias("uids")
            ])

            return (
                base_stats
                .join(w_df, on=group_keys, how="left")
                .join(a_df, on=group_keys, how="left")
                .join(r_df, on=group_keys, how="left")
            )

        # 2. Calculate TOTALS
        totals = get_performance_stats(base_data, ["Character"])

        # 3. Calculate metrics per Eidolon
        per_eidolon = (
            get_performance_stats(base_data, ["Character", "cons"])
            .with_columns(
                ("Eidolon " + pl.col("cons").cast(pl.String)).alias("Eidolon_Level")
            )
            .collect()
        )

        # 4. Pivot
        # Values now include the nested structs with performance lists
        pivoted = per_eidolon.pivot(
            on="Eidolon_Level",
            index="Character",
            values=["Samples", "Cycles", "Sustains", "Lightcones", "Relics", "Planar_Set"],
            aggregate_function="first" 
        )

        # 5. Join and Final Selection
        final_df = (
            totals.collect()
            .join(pivoted, on="Character", how="left")
        )

        # Organize columns: Character -> Global Totals -> Per Eidolon Stats
        eidolon_cols = sorted([c for c in final_df.columns if "Eidolon" in c])
        header_cols = [
            "Character", "Total_Samples", "Total_Cycles", "Total_Sustains", 
            "uids", "Lightcones", "Relics", "Planar_Set"
        ]

        self.char_stats = final_df.select(header_cols + eidolon_cols)

        

        # 2. TEAM AGGREGATION

        self.team_stats = (
            lf.with_columns(
                pl.concat_list(char_cols).alias("temp_team")
            )
            .with_columns(
                pl.col("temp_team").list.eval(
                    # We use pl.element() to refer to the team list
                    # and sort it by mapping each member to its index
                    pl.element().sort_by(
                        pl.element().replace_strict(char_to_index, default=999)
                    )
                ).alias("team_key")
            )
            .group_by("team_key")
            .agg([
                pl.count("uid").alias("Samples"),
                pl.col("round_num").alias("Cycles"),
                pl.col("uid").unique().alias("uids"),
                pl.col("has_sustain").sum().alias("Total_Sustains")
            ])
            .collect()
        )

        # 6. ARCHETYPE AGGREGATION


        self.archetypes_stats = (
            self.team_stats.with_columns(
                pl.col("team_key")
                .list.eval(
                    # Just keep the DPS characters, no extra sorting overhead
                    pl.element().filter(
                        pl.element().is_in(dps_names) & pl.element().is_not_null()
                    )
                )
                .alias("archetype_key")
            )
            .group_by("archetype_key")
            .agg([
                # Sum up samples from all teams that share this DPS core
                pl.col("Samples").sum(),

                # Combine all cycle lists into one big distribution

                pl.col("Cycles").list.explode().alias("Cycles"),

                # For UIDs: list.explode them, then grab the unique ones
                pl.col("uids").list.explode().unique().alias("uids"),
                pl.col("Total_Sustains").sum()
            ])
        )


        

        # Total Samples (Global)
        self.total_samples = lf.select(pl.col("uid").n_unique()).collect().item()




    def _process_combined_data(self, combined,char_cols,cons_cols,dps_names,char_to_index):




        # 5. AGGREGATE COMBINED TEAMS
        # We create a unique key for the "Team Pair"
        self.combined_team_stats = (
            combined.group_by(["n1_chars", "n2_chars"])
            .agg([
                pl.count("uid").alias("Samples"),
                pl.col("total_cycles").alias("Cycles"), # Keep as list for percentile calculations
                pl.col("uid").unique().alias("uids")
            ])
            .collect()
        )
        # 6. COMBINED ARCHETYPE AGGREGATION
        self.combined_archetypes_stats = (
            self.combined_team_stats.with_columns([
                # Extract DPS from Node 1
                pl.col("n1_chars").list.eval(
                    pl.element().filter(pl.element().is_in(dps_names) & pl.element().is_not_null())
                ).alias("n1_archetype"),

                # Extract DPS from Node 2
                pl.col("n2_chars").list.eval(
                    pl.element().filter(pl.element().is_in(dps_names) & pl.element().is_not_null())
                ).alias("n2_archetype")
            ])
            .group_by(["n1_archetype", "n2_archetype"])
            .agg([
                # Sum up samples from all team combinations that fit this dual-archetype
                pl.col("Samples").sum(),

                # Combine the cycle distributions from all those teams
                pl.col("Cycles").list.explode().alias("Cycles"),

                # Combine and unique the UIDs
                pl.col("uids").list.explode().unique().alias("uids")
            ])
        )

        # 6. CHARACTER PAIRING (Replaces itertools.product)
        # This calculates how often Char A (Node 1) appears with Char B (Node 2)
        self.combined_char_stats = (
            combined.select(["uid", "total_cycles", "n1_chars", "n2_chars"])
            # Explode Node 1 chars, then Explode Node 2 chars to get all combinations
            .explode("n1_chars")
            .explode("n2_chars")
            .group_by(["n1_chars", "n2_chars"])
            .agg([
                pl.count("uid").alias("Samples"),
                pl.col("total_cycles").alias("Cycles") # Changed to alias "Cycles" and keep as list for percentile calculations
            ])
            .collect()
        )

    def _plot_cycle_distribution(self, df: pl.DataFrame, round_num: str, eidolon_col: str, cumulative: bool, output: bool, title: str):
          """Core helper to calculate stats and plot cycle distributions."""

          # Drop rows with missing cycles just in case
          df = df.drop_nulls(subset=[round_num])
          df = df.collect()
          sample_size = len(df)

          if sample_size == 0:
              print("No cycle data available")
              return None
         
          # 1. Calculate Global Statistics using Polars
          mean = df.select(pl.col(round_num).mean()).item()
          median = df.select(pl.col(round_num).median()).item()
          mode = df.select(pl.col(round_num).mode().first()).item()
          std_dev = df.select(pl.col(round_num).std(ddof=1).fill_null(0)).item()

          # 2. Group by Cycles and Count Eidolons
          # This completely replaces the need for `self.cyc` loops!
          agg_cols = [pl.len().alias("Count")]
          for i in range(7):
              agg_cols.append((pl.col(eidolon_col) == i).sum().alias(f"E{i}_Count"))

          stats_df = df.group_by(round_num).agg(agg_cols).sort(round_num)

          # 3. Calculate Cumulative Counts and Percentiles
          stats_df = stats_df.with_columns(
              pl.col("Count").cum_sum().alias("Cum_Count")
          ).with_columns(
              ((1 - (pl.col("Cum_Count") / sample_size)) * 100).round(2).alias("Percentile (%)")
          )

          # 4. Calculate Eidolon Percentages
          e_exprs = []
          for i in range(7):
              if cumulative:
                  # Cumulative E_count / Cumulative Total Count
                  e_exprs.append(
                      ((pl.col(f"E{i}_Count").cum_sum() / pl.col("Cum_Count")) * 100).round(2).alias(f"E{i} (%)")
                  )
              else:
                  # Cycle E_count / Cycle Total Count
                  e_exprs.append(
                      ((pl.col(f"E{i}_Count") / pl.col("Count")) * 100).round(2).alias(f"E{i} (%)")
                  )

          # Apply expressions and clean up columns
          final_df = stats_df.with_columns(e_exprs).select(
              [pl.col(round_num).alias("Cycles"), "Count", "Percentile (%)",
              "E0 (%)", "E1 (%)", "E2 (%)", "E3 (%)", "E4 (%)", "E5 (%)", "E6 (%)"]
          )

          # 5. Output and Plotting
          if output:
              print(f"Sample Size: {sample_size}")
              with pl.Config(tbl_rows=-1):
                  print(final_df)

              # Extract the raw cycles column to a standard python list for Matplotlib
              cycles_list = df.get_column(round_num).to_list()

              plt.figure(figsize=(12, 6))
              plt.hist(cycles_list, bins='auto', alpha=0.5, color='blue', edgecolor='black', label='Scores Frequency')

              plt.axvline(mean, color='orange', linestyle='dashed', linewidth=1, label=f'Mean: {mean:.2f}')
              plt.axvline(median, color='green', linestyle='dashed', linewidth=1, label=f'Median: {median:.2f}')
              plt.axvline(mode, color='red', linestyle='dashed', linewidth=1, label=f'Mode: {mode:.2f}')
              plt.axvline(mean + std_dev, color='purple', linestyle='dashed', linewidth=1, label=f'Std Dev: {std_dev:.2f}')
              plt.axvline(mean - std_dev, color='purple', linestyle='dashed', linewidth=1)

              plt.title(title)
              plt.xlabel('Avg Cycles')
              plt.ylabel('Frequency')
              plt.legend()
              plt.text(mean, max(plt.ylim()) * 0.8, f'Sample Size: {sample_size}',
                      horizontalalignment='center', fontsize=10, color='black')
              plt.show()
              return

          return final_df

    def get_team_df(self):
        df = self.team_stats.with_columns([
            pl.col("team_key").list.join(", ").map_elements(lambda s: f"({s})", return_dtype=pl.String).alias("Team"),
            (pl.col("Samples") / self.total_samples * 100).round(2).alias("Appearance Rate (%)"),
            (pl.col("Total_Sustains") == pl.col("Samples")).alias("Sustain?"),
            # Stats
            pl.col("Cycles").list.eval(pl.element().quantile(0.25)).list.first().round(2).alias("25th Percentile Cycles"),
            pl.col("Cycles").list.median().round(2).alias("Median Cycles"),
            pl.col("Cycles").list.eval(pl.element().quantile(0.75)).list.first().round(2).alias("75th Percentile Cycles"),
            pl.col("Cycles").list.eval(pl.element().std(ddof=1)).list.first().round(2).alias("Std Dev Cycles"),
            # Aggregations
            pl.col("Cycles").list.min().alias("Min Cycles"),
            pl.col("Cycles").list.mean().round(2).alias("Average Cycles"),
            pl.col("Cycles").list.max().alias("Max Cycles")
        ]).sort("Samples", descending=True)

        return df.with_row_index("Rank", offset=1).select([
            "Rank", "Team", "Appearance Rate (%)", "Samples",
            "Min Cycles", "25th Percentile Cycles", "Median Cycles",
            "75th Percentile Cycles", "Average Cycles", "Std Dev Cycles", "Max Cycles","Sustain?"
        ])

    def get_archetype_df(self):
      df = self.archetypes_stats.with_columns([
          # Join list to string, handle empty lists as "Other/No DPS"
          pl.col("archetype_key").list.join(" + ")
              .map_elements(lambda s: s if s != "" else "Other / No DPS", return_dtype=pl.String)
              .alias("Archetype Core"),

          (pl.col("Samples") / self.total_samples * 100).round(2).alias("Usage %"),
          (pl.col("Total_Sustains") / pl.col("Samples") * 100).round(2).alias("Sustain_Percentage"),
          
          # Stats
          pl.col("Cycles").list.min().alias("Min Cycles"),
          pl.col("Cycles").list.eval(pl.element().quantile(0.25)).list.first().round(2).alias("25th %"),
          pl.col("Cycles").list.median().round(2).alias("Median"),
          pl.col("Cycles").list.eval(pl.element().quantile(0.75)).list.first().round(2).alias("75th %"),
          pl.col("Cycles").list.mean().round(2).alias("Avg Cycles"),
          pl.col("Cycles").list.max().alias("Max Cycles"),
          pl.col("Cycles").list.eval(pl.element().std(ddof=1)).list.first().round(2).alias("Std Dev Cycles"),
      ]).sort("Samples", descending=True)

      return df.with_row_index("Rank", offset=1).select([
          "Rank", "Archetype Core", "Usage %", "Samples","Sustain_Percentage",pl.col("Total_Sustains").alias("Sustain Samples"),
         "Min Cycles", "25th %", "Median", "75th %", "Avg Cycles","Max Cycles","Std Dev Cycles", 
      ])


    def get_char_df(self):
        # 1. Identify the Eidolon Sample columns
        eidolon_sample_cols = [c for c in self.char_stats.columns if "Samples_Eidolon" in c]

        # 2. Calculate the core stats and convert Eidolon counts to Percentages
        df = self.char_stats.with_columns([
            # Total Rates
            (pl.col("Total_Samples") / self.total_samples * 100).round(3).alias("Appearance Rate (%)"),
            (pl.col("Total_Sustains") / pl.col("Total_Samples") * 100).round(2).alias("Sustain_Percentage"),
            
            # Cycle Stats
            pl.col("Total_Cycles").list.min().alias("Min Cycles"),
            pl.col("Total_Cycles").list.eval(pl.element().quantile(0.25)).list.first().round(2).alias("25th Percentile Cycles"),
            pl.col("Total_Cycles").list.median().round(2).alias("Median Cycles"),
            pl.col("Total_Cycles").list.eval(pl.element().quantile(0.75)).list.first().round(2).alias("75th Percentile Cycles"),
            pl.col("Total_Cycles").list.mean().round(2).alias("Average Cycles"),
            pl.col("Total_Cycles").list.eval(pl.element().std()).list.first().round(2).alias("Std Dev Cycles"),
            pl.col("Total_Cycles").list.max().alias("Max Cycles"),

            # --- DYNAMIC PERCENTAGE CALCULATION ---
            # For every column like 'Samples_Eidolon 0', divide by Total_Samples and multiply by 100
            *[
                ((pl.col(c) / pl.col("Total_Samples")) * 100).round(2).alias(f"{c.replace('Samples_', '')} %")
                for c in eidolon_sample_cols
            ]
        ])

        # 3. Sort and Rank
        df = df.sort("Total_Samples", descending=True).with_row_index("Rank", offset=1)

        # 4. Get the new Percentage column names for the final selection
        eidolon_perc_cols = sorted([c for c in df.columns if "Eidolon" in c and "%" in c])

        return df.select([
            "Rank", 
            "Character", 
            "Appearance Rate (%)", 
            pl.col("Total_Samples").alias("Samples"),
            "Min Cycles", 
            "25th Percentile Cycles", 
            "Median Cycles", 
            "75th Percentile Cycles", 
            "Average Cycles", 
            "Std Dev Cycles", 
            "Max Cycles",
            pl.col("Total_Sustains").alias("Sustain Samples"),
            "Sustain_Percentage",
            *eidolon_perc_cols  # Displays "Eidolon 0 %", "Eidolon 1 %", etc.
        ])
    def get_eidolon_performance_df(self):
        # 1. Identify the base columns from your pivot
        # These usually look like "Cycles_Eidolon 0", "Sustains_Eidolon 0", etc.
        cycle_cols = [c for c in self.char_stats.columns if "Cycles_Eidolon" in c]
        sustain_cols = [c for c in self.char_stats.columns if "Sustains_Eidolon" in c]
        sample_cols = [c for c in self.char_stats.columns if "Samples_Eidolon" in c]

        stat_exprs = []

        # 2. Map Cycles to Averages and Sustains to Percentages
        # We use the actual column names to ensure we don't miss anything
        for col in cycle_cols:
            label = col.replace("Cycles_", "") # Result: "Eidolon 0"
            stat_exprs.append(
                pl.col(col).list.mean().round(2).alias(f"{label} Avg Cycles")
            )
            
        for col in sustain_cols:
            label = col.replace("Sustains_", "")
            sample_col = f"Samples_{label}"
            if sample_col in self.char_stats.columns:
                stat_exprs.append(
                    (pl.col(col) / pl.col(sample_col) * 100).round(2).alias(f"{label} Sustain %")
                )

        # 3. Apply transformations
        df = self.char_stats.with_columns(stat_exprs)

        # 4. Sorting
        df = df.sort("Total_Samples", descending=True).with_row_index("Rank", offset=1)

        # 5. DYNAMIC SELECTION
        # Instead of hardcoding range(7), we look at what actually exists in the columns
        # This finds every "Eidolon X Avg Cycles" and "Eidolon X Sustain %" column
        new_stat_cols = sorted([
            c for c in df.columns 
            if "Avg Cycles" in c or "Sustain %" in c
        ])

        # We also want to include the Totals for context as seen in your table example
        header_cols = ["Rank", "Character", "Total_Samples", "Total_Sustains"]
        
        # Force Polars to show everything in the console
        pl.Config.set_tbl_cols(-1)
        
        return df.select(header_cols + new_stat_cols)
    
    def get_combined_team_df(self):
        # Total unique players/runs
        total_combined_samples = self.combined_team_stats.select(pl.col("Samples").sum()).item()

        df = self.combined_team_stats.with_columns([
            # Formatting
            pl.col("n1_chars").list.join(", ").map_elements(lambda s: f"({s})", return_dtype=pl.String).alias("Team Node 1"),
            pl.col("n2_chars").list.join(", ").map_elements(lambda s: f"({s})", return_dtype=pl.String).alias("Team Node 2"),

            # Rates & Sustains (Added Sustain % here)
            (pl.col("Samples") / total_combined_samples * 100).round(2).alias("Appearance Rate (%)"),
            # (pl.col("Sustains") / pl.col("Samples") * 100).round(2).alias("Sustain %"),

            # Stats
            pl.col("Cycles").list.eval(pl.element().quantile(0.25)).list.first().round(2).alias("25th Percentile Cycles"),
            pl.col("Cycles").list.median().round(2).alias("Median Cycles"),
            pl.col("Cycles").list.eval(pl.element().quantile(0.75)).list.first().round(2).alias("75th Percentile Cycles"),
            pl.col("Cycles").list.eval(pl.element().std(ddof=1)).list.first().round(2).alias("Std Dev Cycles"),

            # Aggregations
            pl.col("Cycles").list.min().alias("Min Cycles"),
            pl.col("Cycles").list.mean().round(2).alias("Average Cycles"),
            pl.col("Cycles").list.max().alias("Max Cycles")
        ]).sort("Samples", descending=True)

        # Prepare final selection
        final_df = df.with_row_index("Rank", offset=1).select([
            "Rank", "Team Node 1", "Team Node 2", "Appearance Rate (%)", "Samples",
             # Added sustain columns
            "Min Cycles", "25th Percentile Cycles", "Median Cycles",
            "75th Percentile Cycles", "Average Cycles", "Std Dev Cycles", "Max Cycles"
        ])

        return final_df

    def get_combined_archetype_df(self):
        # Total combined samples from the stats table
        total_samples = self.combined_archetypes_stats.select(pl.col("Samples").sum()).item()

        df = self.combined_archetypes_stats.with_columns([
            # Format Node 1 Core
            pl.col("n1_archetype").list.join(" + ")
                .map_elements(lambda s: f"[{s}]" if s != "" else "[Other]", return_dtype=pl.String)
                .alias("Core Node 1"),

            # Format Node 2 Core
            pl.col("n2_archetype").list.join(" + ")
                .map_elements(lambda s: f"[{s}]" if s != "" else "[Other]", return_dtype=pl.String)
                .alias("Core Node 2"),

            (pl.col("Samples") / total_samples * 100).round(2).alias("Appearance Rate (%)"),

             # Stats
            pl.col("Cycles").list.eval(pl.element().quantile(0.25)).list.first().round(2).alias("25th Percentile Cycles"),
            pl.col("Cycles").list.median().round(2).alias("Median Cycles"),
            pl.col("Cycles").list.eval(pl.element().quantile(0.75)).list.first().round(2).alias("75th Percentile Cycles"),
            pl.col("Cycles").list.eval(pl.element().std(ddof=1)).list.first().round(2).alias("Std Dev Cycles"),

            # Aggregations
            pl.col("Cycles").list.min().alias("Min Cycles"),
            pl.col("Cycles").list.mean().round(2).alias("Average Cycles"),
            pl.col("Cycles").list.max().alias("Max Cycles")
        ]).sort("Samples", descending=True)

        return df.with_row_index("Rank", offset=1).select([
            "Rank", "Core Node 1", "Core Node 2", "Appearance Rate (%)", "Samples",
            "Min Cycles", "25th Percentile Cycles", "Median Cycles",
            "75th Percentile Cycles", "Average Cycles", "Std Dev Cycles", "Max Cycles"
        ])

    def get_combined_char_df(self):
        # Total unique pairings count
        total_combined_char_samples = self.combined.select(pl.col("uid").n_unique()).collect().item()
        
        
        df = self.combined_char_stats.with_columns([
            # --- ADD THESE TWO LINES TO RENAME THE COLUMNS ---
            pl.col("n1_chars").alias("Character Node 1"),
            pl.col("n2_chars").alias("Character Node 2"),
            (pl.col("Samples") / total_combined_char_samples * 100).round(2).alias("Appearance Rate (%)"),
            # Stats
            pl.col("Cycles").list.eval(pl.element().quantile(0.25)).list.first().round(2).alias("25th Percentile Cycles"),
            pl.col("Cycles").list.median().round(2).alias("Median Cycles"),
            pl.col("Cycles").list.eval(pl.element().quantile(0.75)).list.first().round(2).alias("75th Percentile Cycles"),
            pl.col("Cycles").list.eval(pl.element().std(ddof=1)).list.first().round(2).alias("Std Dev Cycles"),
            # Aggregations
            pl.col("Cycles").list.min().alias("Min Cycles"),
            pl.col("Cycles").list.mean().round(2).alias("Average Cycles"),
            pl.col("Cycles").list.max().alias("Max Cycles")
        ]).sort("Samples", descending=True)

        return df.with_row_index("Rank", offset=1).select([
            "Rank", "Character Node 1", "Character Node 2", "Samples", "Appearance Rate (%)",
            "Min Cycles", "25th Percentile Cycles", "Median Cycles",
            "75th Percentile Cycles", "Average Cycles", "Std Dev Cycles", "Max Cycles"
        ])
        
        
    def display_top_gear(self):
        df = self.char_stats
        
        # 1. Identify all Eidolon levels
        eidolon_levels = sorted(list(set([c.split('_')[-1] for c in df.columns if "Eidolon" in c])))
        
        results = []

        for level in eidolon_levels:
            for gear_type in ["Lightcones", "Relics", "Planar_Set"]:
                col_name = f"{gear_type}_{level}"
                
                if col_name not in df.columns:
                    continue

                # Explode and prepare base columns
                temp = (
                    df.select(["Character", col_name])
                    .explode(col_name)
                    .drop_nulls(col_name)
                    .with_columns([
                        pl.col(col_name).struct.field("name").alias("Gear_Name"),
                        pl.col(col_name).struct.field("count").alias("Usage"),
                        pl.col(col_name).struct.field("cycles").alias("_cycles_list")
                    ])
                    # Filter out "Info_not_found" FIRST
                    .filter(pl.col("Gear_Name") != "Info_not_found")
                )

                # Check if there's data left after filtering to avoid errors
                if temp.is_empty():
                    continue

                # Now calculate stats and the usage rate based on the filtered total
                processed = (
                    temp.with_columns([
                        # Calculate new total usage per character after filter
                        pl.col("Usage").sum().over("Character").alias("_total_filtered_usage")
                    ])
                    .with_columns([
                        # Usage Percentage relative to the new filtered total
                        (pl.col("Usage") / pl.col("_total_filtered_usage")).alias("Usage_Rate"),
                        
                        # Statistical Metrics
                        pl.col("_cycles_list").list.mean().round(2).alias("Avg_Cycles"),
                        pl.col("_cycles_list").list.median().alias("Median_Cycles"),
                        pl.col("_cycles_list").list.min().alias("Min_Cycles"),
                        pl.col("_cycles_list").list.max().alias("Max_Cycles"),
                        pl.col("_cycles_list").list.std().round(2).alias("Std_Cycles"),
                        pl.col("_cycles_list").list.eval(pl.element().quantile(0.25)).list.first().alias("25th Percentile Cycles"),
                        pl.col("_cycles_list").list.eval(pl.element().quantile(0.75)).list.first().alias("75th Percentile Cycles"),
                    ])
                )

                # Add metadata columns
                full_list = processed.with_columns([
                    pl.lit(level).alias("Eidolon"),
                    pl.lit(gear_type).alias("Category")
                ])
                
                results.append(full_list.select([
                    "Character", "Eidolon", "Category", "Gear_Name", 
                    "Usage", "Usage_Rate", "Avg_Cycles", "25th Percentile Cycles", 
                    "Median_Cycles", "75th Percentile Cycles", "Min_Cycles", 
                    "Max_Cycles", "Std_Cycles"
                ]))

        if not results:
            return pl.DataFrame()

        # Final concat and sorting
        return (
            pl.concat(results)
            .sort(
                by=[
                    "Character", 
                    "Eidolon", 
                    pl.col("Category").str.slice(0, 1), # Sorts L -> P -> R
                    "Usage"
                ], 
                descending=[False, False, False, True]
            )
        )

    def display_single_char_full(self, char_name):
        # Call the main display method to get the fully processed and sorted DataFrame
        full_df = self.display_top_gear()
        
        # Filter the resulting DataFrame for the specific character
        char_data = full_df.filter(pl.col("Character") == char_name)
        
        # Check if the character exists in the results
        if char_data.is_empty():
            return f"Character '{char_name}' not found or has no valid gear data."
        
        # Optional: Drop the 'Character' column since it's redundant for a single-character view
        return char_data.drop("Character")

    def plot_statistics_all(self, cumulative=False, output=True):
        title = f"Avg Cycles Frequency for all for version {self.version}, Node {self.node}, up to {self.by_ed} Eidolon"

        # Pass your raw single-node DataFrame here
        return self._plot_cycle_distribution(
            df = self.lf, # <-- Change to your actual raw dataframe variable
            round_num="round_num",
            eidolon_col="max_eidolon", # <-- Column representing max eidolon in the team
            cumulative=cumulative,
            output=output,
            title=title
        )

    def plot_statistics_all_combined(self, cumulative=False, output=True):
        title = f"Combined Avg Cycles Frequency for version {self.version}, up to {self.by_ed} Eidolon"

        # Pass your raw combined-node DataFrame here
        return self._plot_cycle_distribution(
            df=self.combined, # <-- Change to your actual combined raw dataframe variable
            round_num="total_cycles",
            eidolon_col="combined_max_ed", # <-- Column representing max combined eidolon
            cumulative=cumulative,
            output=output,
            title=title
        )
