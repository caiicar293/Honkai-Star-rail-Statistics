import polars as pl
import os
import orjson
from itertools import chain
import matplotlib.pyplot as plt
pl.Config.set_tbl_rows(-1)      # -1 or None shows all rows
pl.Config.set_tbl_cols(-1)      # -1 or None shows all columns
pl.Config.set_fmt_str_lengths(100)  # Prevents long strings from being cut off

class HonkaiStatistics_Anomaly_V2:
    def __init__(self, version, floor, by_ed=6, by_cycle=30, by_ed_inclusive=False,
                 by_ed_inclusive_combined=False, by_char=None, by_cycles_combined=30,
                 not_char=False, sustain_condition=None, star_num=None):

        self.version, self.floor = version, floor
        self.by_ed, self.by_cycle = by_ed, by_cycle
        self.by_ed_inclusive = by_ed_inclusive
        self.by_ed_inclusive_combined = by_ed_inclusive_combined
        self.by_cycles_combined = by_cycles_combined

        # 1. LOAD DATA VIA POLARS (Multi-threaded Parquet/CSV)
        folder = "raw_data"
        path = os.path.join(folder, f"{version}_aa.parquet")
        if os.path.exists(path):
            self.df = pl.read_parquet(path)
        else:
            url = f"https://huggingface.co/datasets/LvlUrArti/MocData/resolve/main/{version}_aa.csv"
            self.df = pl.read_csv(url)
            os.makedirs(folder, exist_ok=True)
            self.df.write_parquet(path)

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
        lf = lf.filter((pl.col('hard_mode') == False))
        
        # 3. INITIAL FILTERING (Floor/Floor/Stars)
        if self.floor == 0:
            lf = lf.filter((pl.col("floor") !=4) )
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



        self._process_data(self.lf,char_cols,cons_cols,dps_names,char_to_index)
         # If Floor is 0, process combined data as well

        if self.floor == 0:
            # Explicitly select base columns needed for f1 and f2 before splitting
            base_cols_for_n = ["uid", "floor", "round_num", "max_eidolon"] + char_cols
            lf_base_for_combined = lf.select(base_cols_for_n)

            # 1. SPLIT DATA INTO TWO FloorS
            f1 = lf_base_for_combined.filter(pl.col("floor") == 1).select([
                pl.col("uid"),
                pl.concat_list(char_cols).alias("f1_chars").list.eval(

                        pl.element().sort_by(
                            pl.element().replace_strict(char_to_index, default=999)
                        )),
                pl.col("round_num").alias("f1_cycles"),
                pl.col("max_eidolon").alias("f1_max_ed")
            ])

            f2 = lf_base_for_combined.filter(pl.col("floor") == 2).select([
                pl.col("uid"),
                pl.concat_list(char_cols).alias("f2_chars").list.eval(

                        pl.element().sort_by(
                            pl.element().replace_strict(char_to_index, default=999)
                        )),
                pl.col("round_num").alias("f2_cycles"),
                pl.col("max_eidolon").alias("f2_max_ed")
            ])
            
            
            f3 = lf_base_for_combined.filter(pl.col("floor") == 3).select([
                pl.col("uid"),
                pl.concat_list(char_cols).alias("f3_chars").list.eval(

                        pl.element().sort_by(
                            pl.element().replace_strict(char_to_index, default=999)
                        )),
                pl.col("round_num").alias("f3_cycles"),
                pl.col("max_eidolon").alias("f3_max_ed")
            ])

            # 2. INNER JOIN (Only keep UIDs that completed both Floors)
            combined = f1.join(f2, on="uid", how="inner")
            combined = combined.join(f3, on="uid", how="inner")
            # 3. CALCULATE COMBINED STATS
            combined = combined.with_columns([
                (pl.col("f1_cycles") +pl.col("f2_cycles")+pl.col("f3_cycles")).alias("total_cycles"),
                pl.max_horizontal(["f1_max_ed", "f2_max_ed","f3_max_ed"]).alias("combined_max_ed")
            ])

            # 4. FILTER BY THRESHOLDS
            if self.by_ed_inclusive_combined:
                combined = combined.filter(pl.col("combined_max_ed") == self.by_ed)

            self.combined = combined.filter(pl.col("total_cycles") <= self.by_cycles_combined)
            self._process_combined_data(self.combined,char_cols,cons_cols,dps_names,char_to_index)


    def _process_data(self,lf,char_cols,cons_cols,dps_names,char_to_index):


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
        base_data = (
            pl.concat([chars, cons], how="horizontal")
            .with_columns([
                pl.col("Character").fill_null("Empty Slot"),
                # Replace -1 with 0 so it groups with Eidolon 0
                pl.col("cons").replace(-1, 0)
            ])
        )

        # 2. Calculate TOTALS per Character
        totals = (
            base_data.group_by("Character")
            .agg([
                pl.count("uid").alias("Total_Samples"),
                pl.col("round_num").alias("Total_Cycles"), # Kept as list
                pl.col("has_sustain").sum().alias("Total_Sustains"),
                pl.col("uid").unique().alias("uids")
            ])
        )

        # 3. Calculate metrics per Eidolon (0-6 only)
        per_eidolon = (
            base_data.group_by("Character", "cons")
            .agg([
                pl.count("uid").alias("Samples"),
                pl.col("round_num").alias("Cycles"), # Kept as list
                pl.col("has_sustain").sum().alias("Sustains")
            ])
            .with_columns(
                ("Eidolon " + pl.col("cons").cast(pl.String)).alias("Eidolon_Level")
            )
            .collect()
        )

        # 4. Pivot
        # aggregate_function="first" preserves the Cycles lists
        pivoted = per_eidolon.pivot(
            on="Eidolon_Level",
            index="Character",
            values=["Samples", "Cycles", "Sustains"],
            aggregate_function="first" 
        )

        # 5. Join and Final Selection
        final_df = (
            totals.collect()
            .join(pivoted, on="Character", how="left")
        )

        # Organize columns: Character -> Totals -> E0...E6
        eidolon_cols = sorted([c for c in final_df.columns if "Eidolon" in c])
        header_cols = ["Character", "Total_Samples", "Total_Cycles", "Total_Sustains","uids"]

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
            combined.group_by(["f1_chars", "f2_chars","f3_chars"])
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
                # Extract DPS from Floor 1
                pl.col("f1_chars").list.eval(
                    pl.element().filter(pl.element().is_in(dps_names) & pl.element().is_not_null())
                ).alias("f1_archetype"),

                # Extract DPS from Floor 2
                pl.col("f2_chars").list.eval(
                    pl.element().filter(pl.element().is_in(dps_names) & pl.element().is_not_null())
                ).alias("f2_archetype"),
                
                # Extract DPS from Floor 3
                pl.col("f3_chars").list.eval(
                    pl.element().filter(pl.element().is_in(dps_names) & pl.element().is_not_null())
                ).alias("f3_archetype")
            ])
            .group_by(["f1_archetype", "f2_archetype","f3_archetype"])
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
        # This calculates how often Char A (Floor 1) appears with Char B (Floor 2)
        self.combined_char_stats = (
            combined.select(["uid", "total_cycles", "f1_chars", "f2_chars" ,"f3_chars"])
            # Explode Floor 1 chars, then Explode Floor 2 chars to get all combinations
            .explode("f1_chars")
            .explode("f2_chars")
            .explode("f3_chars")
            .group_by(["f1_chars", "f2_chars","f3_chars"])
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
            (pl.col("Total_Sustains") == pl.col("Samples")).alias("Sustainless?"),
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
            "75th Percentile Cycles", "Average Cycles", "Std Dev Cycles", "Max Cycles","Sustainless?"
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
          pl.col("Cycles").list.eval(pl.element().std()).list.first().round(2).alias("Std Dev Cycles"),
          pl.col("Cycles").list.max().alias("Max Cycles"),
      ]).sort("Samples", descending=True)

      return df.with_row_index("Rank", offset=1).select([
          "Rank", "Archetype Core", "Usage %", "Samples","Sustain_Percentage",pl.col("Total_Sustains").alias("Sustain Samples"),"Min Cycles",
          "25th %", "Median", "75th %", "Avg Cycles","Max Cycles","Std Dev Cycles"
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
            pl.col("f1_chars").list.join(", ").map_elements(lambda s: f"({s})", return_dtype=pl.String).alias("Team Floor 1"),
            pl.col("f2_chars").list.join(", ").map_elements(lambda s: f"({s})", return_dtype=pl.String).alias("Team Floor 2"),
            pl.col("f3_chars").list.join(", ").map_elements(lambda s: f"({s})", return_dtype=pl.String).alias("Team Floor 3"),
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
            "Rank", "Team Floor 1", "Team Floor 2","Team Floor 3" ,"Appearance Rate (%)", "Samples",
             # Added sustain columns
            "Min Cycles", "25th Percentile Cycles", "Median Cycles",
            "75th Percentile Cycles", "Average Cycles", "Std Dev Cycles", "Max Cycles"
        ])

        return final_df

    def get_combined_archetype_df(self):
        # Total combined samples from the stats table
        total_samples = self.combined_archetypes_stats.select(pl.col("Samples").sum()).item()

        df = self.combined_archetypes_stats.with_columns([
            # Format Floor 1 Core
            pl.col("f1_archetype").list.join(" + ")
                .map_elements(lambda s: f"[{s}]" if s != "" else "[Other]", return_dtype=pl.String)
                .alias("Core Floor 1"),

            # Format Floor 2 Core
            pl.col("f2_archetype").list.join(" + ")
                .map_elements(lambda s: f"[{s}]" if s != "" else "[Other]", return_dtype=pl.String)
                .alias("Core Floor 2"),
            
            
            
            # Format Floor 3 Core
            pl.col("f3_archetype").list.join(" + ")
                .map_elements(lambda s: f"[{s}]" if s != "" else "[Other]", return_dtype=pl.String)
                .alias("Core Floor 3"),
                
                
                
            (pl.col("Samples") / total_samples * 100).round(2).alias("Appearance Rate (%)"),

            # Stats
            pl.col("Cycles").list.median().round(2).alias("Median Cycles"),
            pl.col("Cycles").list.mean().round(2).alias("Avg Cycles"),
            pl.col("Cycles").list.min().alias("Min"),
            pl.col("Cycles").list.max().alias("Max")
        ]).sort("Samples", descending=True)

        return df.with_row_index("Rank", offset=1).select([
            "Rank", "Core Floor 1", "Core Floor 2" , "Core Floor 3" ,"Appearance Rate (%)", "Samples",
            "Min", "Median Cycles", "Avg Cycles", "Max"
        ])

    def get_combined_char_df(self):
        # Total unique pairings count
        total_combined_char_samples = self.combined.select(pl.col("uid").n_unique()).collect().item()

        df = self.combined_char_stats.with_columns([
            # --- ADD THESE TWO LINES TO RENAME THE COLUMNS ---
            pl.col("f1_chars").alias("Character Floor 1"),
            pl.col("f2_chars").alias("Character Floor 2"),
            pl.col("f3_chars").alias("Character Floor 3"),
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
            "Rank", "Character Floor 1", "Character Floor 2", "Character Floor 3", "Samples", "Appearance Rate (%)",
            "Min Cycles", "25th Percentile Cycles", "Median Cycles",
            "75th Percentile Cycles", "Average Cycles", "Std Dev Cycles", "Max Cycles"
        ])
    def plot_statistics_all(self, cumulative=False, output=True):
        title = f"Avg Cycles Frequency for all for version {self.version}, Floor {self.floor}, up to {self.by_ed} Eidolon"

        # Pass your raw single-Floor DataFrame here
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

        # Pass your raw combined-Floor DataFrame here
        return self._plot_cycle_distribution(
            df=self.combined, # <-- Change to your actual combined raw dataframe variable
            round_num="total_cycles",
            eidolon_col="combined_max_ed", # <-- Column representing max combined eidolon
            cumulative=cumulative,
            output=output,
            title=title
        )
