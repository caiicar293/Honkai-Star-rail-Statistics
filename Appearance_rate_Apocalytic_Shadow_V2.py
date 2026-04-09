import polars as pl
import os
import orjson
from itertools import chain
import matplotlib.pyplot as plt

class HonkaiStatistics_V2_APOC:
    def __init__(self, version, floor, node=0, by_ed=6, by_score =0, by_ed_inclusive=False,
                 by_ed_inclusive_combined=False, by_char=None, by_scores_combined = 0,
                 not_char=False, sustain_condition=None, star_num=None):

        self.version, self.floor, self.node = version, floor, node
        self.by_ed, self.by_score = by_ed, by_score
        self.by_ed_inclusive = by_ed_inclusive
        self.by_ed_inclusive_combined = by_ed_inclusive_combined
        self.by_scores_combined = by_scores_combined

        # 1. LOAD DATA VIA POLARS (Multi-threaded Parquet/CSV)
        folder = "raw_data"
        path = os.path.join(folder, f"{version}_as.parquet")
        if os.path.exists(path):
            self.df = pl.read_parquet(path)
        else:
            url = f"https://huggingface.co/datasets/LvlUrArti/MocData/resolve/main/{version}_as.csv"
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

        self.lf = lf.filter(pl.col("round_num") >= self.by_score)



        self._process_data(self.lf,char_cols,cons_cols,dps_names,char_to_index)
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
                pl.col("round_num").alias("n1_scores"),
                pl.col("max_eidolon").alias("n1_max_ed")
            ])

            n2 = lf_base_for_combined.filter(pl.col("node") == 2).select([
                pl.col("uid"),
                pl.concat_list(char_cols).alias("n2_chars").list.eval(

                        pl.element().sort_by(
                            pl.element().replace_strict(char_to_index, default=999)
                        )),
                pl.col("round_num").alias("n2_scores"),
                pl.col("max_eidolon").alias("n2_max_ed")
            ])

            # 2. INNER JOIN (Only keep UIDs that completed both nodes)
            combined = n1.join(n2, on="uid", how="inner")

            # 3. CALCULATE COMBINED STATS
            combined = combined.with_columns([
                (pl.col("n1_scores")+pl.col("n2_scores")).alias("total_scores"),
                pl.max_horizontal(["n1_max_ed", "n2_max_ed"]).alias("combined_max_ed")
            ])

            # 4. FILTER BY THRESHOLDS
            if self.by_ed_inclusive_combined:
                combined = combined.filter(pl.col("combined_max_ed") == self.by_ed)

            self.combined = combined.filter(pl.col("total_scores") >= self.by_scores_combined)
            self._process_combined_data(self.combined,char_cols,cons_cols,dps_names,char_to_index)


    def _process_data(self,lf,char_cols,cons_cols,dps_names,char_to_index):


        # 5. AGGREGATE CHARACTERS (The "Unpivot" trick)
        # Instead of a loop, we melt the 4 columns into 1 long column
        self.char_stats = (
            lf.select(["uid", "round_num", "has_sustain"] + char_cols + cons_cols)
            .unpivot(index=["uid", "round_num", "has_sustain"], on=char_cols, value_name="Character")
            # This is tricky: we need the matching eidolon. For simplicity, we filter in a specialized way or map.
            # Optimized: Just group by character and calculate stats
            .group_by("Character")
            .agg([
                pl.count("uid").alias("Samples"),
                pl.col("round_num").alias("Scores"),
                pl.col("has_sustain").sum().alias("Sustains"),
                pl.col("uid").unique().alias("uids")
            ])
            .collect()
        )

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
                pl.col("round_num").alias("Scores"),
                pl.col("uid").unique().alias("uids")
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

                pl.col("Scores").list.explode().alias("Scores"),

                # For UIDs: list.explode them, then grab the unique ones
                pl.col("uids").list.explode().unique().alias("uids")
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
                pl.col("total_scores").alias("Scores"), # Keep as list for percentile calculations
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
                pl.col("Scores").list.explode().alias("Scores"),

                # Combine and unique the UIDs
                pl.col("uids").list.explode().unique().alias("uids")
            ])
        )

        # 6. CHARACTER PAIRING (Replaces itertools.product)
        # This calculates how often Char A (Node 1) appears with Char B (Node 2)
        self.combined_char_stats = (
            combined.select(["uid", "total_scores", "n1_chars", "n2_chars"])
            # Explode Node 1 chars, then Explode Node 2 chars to get all combinations
            .explode("n1_chars")
            .explode("n2_chars")
            .group_by(["n1_chars", "n2_chars"])
            .agg([
                pl.count("uid").alias("Samples"),
                pl.col("total_scores").alias("Scores") # Changed to alias "Scores" and keep as list for percentile calculations
            ])
            .collect()
        )

    def _plot_cycle_distribution(self, df: pl.DataFrame, round_num: str, eidolon_col: str, cumulative: bool, output: bool, title: str):
          """Core helper to calculate stats and plot cycle distributions."""

          # Drop rows with missing Scores just in case
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

          # 2. Group by Scores and Count Eidolons
          # This completely replaces the need for `self.cyc` loops!
          agg_cols = [pl.len().alias("Count")]
          for i in range(7):
              agg_cols.append((pl.col(eidolon_col) == i).sum().alias(f"E{i}_Count"))

          stats_df = df.group_by(round_num).agg(agg_cols).sort(round_num,descending=True)

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
              [pl.col(round_num).alias("Scores"), "Count", "Percentile (%)",
              "E0 (%)", "E1 (%)", "E2 (%)", "E3 (%)", "E4 (%)", "E5 (%)", "E6 (%)"]
          )

          # 5. Output and Plotting
          if output:
              print(f"Sample Size: {sample_size}")
              with pl.Config(tbl_rows=-1):
                  print(final_df)

              # Extract the raw Scores column to a standard python list for Matplotlib
              Scores_list = df.get_column(round_num).to_list()

              plt.figure(figsize=(12, 6))
              plt.hist(Scores_list, bins='auto', alpha=0.5, color='blue', edgecolor='black', label='Scores Frequency')

              plt.axvline(mean, color='orange', linestyle='dashed', linewidth=1, label=f'Mean: {mean:.2f}')
              plt.axvline(median, color='green', linestyle='dashed', linewidth=1, label=f'Median: {median:.2f}')
              plt.axvline(mode, color='red', linestyle='dashed', linewidth=1, label=f'Mode: {mode:.2f}')
              plt.axvline(mean + std_dev, color='purple', linestyle='dashed', linewidth=1, label=f'Std Dev: {std_dev:.2f}')
              plt.axvline(mean - std_dev, color='purple', linestyle='dashed', linewidth=1)

              plt.title(title)
              plt.xlabel('Avg Scores')
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
            # Stats
            pl.col("Scores").list.eval(pl.element().quantile(0.25)).list.first().round(2).alias("25th Percentile Scores"),
            pl.col("Scores").list.median().round(2).alias("Median Scores"),
            pl.col("Scores").list.eval(pl.element().quantile(0.75)).list.first().round(2).alias("75th Percentile Scores"),
            pl.col("Scores").list.eval(pl.element().std(ddof=1)).list.first().round(2).alias("Std Dev Scores"),
            # Aggregations
            pl.col("Scores").list.min().alias("Min Scores"),
            pl.col("Scores").list.mean().round(2).alias("Average Scores"),
            pl.col("Scores").list.max().alias("Max Scores")
        ]).sort("Samples", descending=True)

        return df.with_row_index("Rank", offset=1).select([
            "Rank", "Team", "Appearance Rate (%)", "Samples",
            "Min Scores", "25th Percentile Scores", "Median Scores",
            "75th Percentile Scores", "Average Scores", "Std Dev Scores", "Max Scores"
        ])

    def get_archetype_df(self):
      df = self.archetypes_stats.with_columns([
          # Join list to string, handle empty lists as "Other/No DPS"
          pl.col("archetype_key").list.join(" + ")
              .map_elements(lambda s: s if s != "" else "Other / No DPS", return_dtype=pl.String)
              .alias("Archetype Core"),

          (pl.col("Samples") / self.total_samples * 100).round(2).alias("Usage %"),

          # Stats
          pl.col("Scores").list.eval(pl.element().quantile(0.25)).list.first().round(2).alias("25th %"),
          pl.col("Scores").list.median().round(2).alias("Median"),
          pl.col("Scores").list.eval(pl.element().quantile(0.75)).list.first().round(2).alias("75th %"),
          pl.col("Scores").list.mean().round(2).alias("Avg Scores")
      ]).sort("Samples", descending=True)

      return df.with_row_index("Rank", offset=1).select([
          "Rank", "Archetype Core", "Usage %", "Samples",
          "25th %", "Median", "75th %", "Avg Scores"
      ])


    def get_char_df(self):
        df = self.char_stats.with_columns([
            (pl.col("Samples") / self.total_samples * 100).round(2).alias("Appearance Rate (%)"),
            (pl.col("Sustains") / pl.col("Samples") * 100).round(2).alias("Sustain %"),
            # Stats
            pl.col("Scores").list.eval(pl.element().quantile(0.25)).list.first().round(2).alias("25th Percentile Scores"),
            pl.col("Scores").list.median().round(2).alias("Median Scores"),
            pl.col("Scores").list.eval(pl.element().quantile(0.75)).list.first().round(2).alias("75th Percentile Scores"),
            pl.col("Scores").list.eval(pl.element().std(ddof=1)).list.first().round(2).alias("Std Dev Scores"),
            # Aggregations
            pl.col("Scores").list.min().alias("Min Scores"),
            pl.col("Scores").list.mean().round(2).alias("Average Scores"),
            pl.col("Scores").list.max().alias("Max Scores")
        ]).sort("Samples", descending=True)

        return df.with_row_index("Rank", offset=1).select([
            "Rank", "Character", "Appearance Rate (%)", "Samples", "Sustains", "Sustain %",
            "Min Scores", "25th Percentile Scores", "Median Scores",
            "75th Percentile Scores", "Average Scores", "Std Dev Scores", "Max Scores"
        ])

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
            pl.col("Scores").list.eval(pl.element().quantile(0.25)).list.first().round(2).alias("25th Percentile Scores"),
            pl.col("Scores").list.median().round(2).alias("Median Scores"),
            pl.col("Scores").list.eval(pl.element().quantile(0.75)).list.first().round(2).alias("75th Percentile Scores"),
            pl.col("Scores").list.eval(pl.element().std(ddof=1)).list.first().round(2).alias("Std Dev Scores"),

            # Aggregations
            pl.col("Scores").list.min().alias("Min Scores"),
            pl.col("Scores").list.mean().round(2).alias("Average Scores"),
            pl.col("Scores").list.max().alias("Max Scores")
        ]).sort("Samples", descending=True)

        # Prepare final selection
        final_df = df.with_row_index("Rank", offset=1).select([
            "Rank", "Team Node 1", "Team Node 2", "Appearance Rate (%)", "Samples",
             # Added sustain columns
            "Min Scores", "25th Percentile Scores", "Median Scores",
            "75th Percentile Scores", "Average Scores", "Std Dev Scores", "Max Scores"
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
            pl.col("Scores").list.median().round(2).alias("Median Scores"),
            pl.col("Scores").list.mean().round(2).alias("Avg Scores"),
            pl.col("Scores").list.min().alias("Min"),
            pl.col("Scores").list.max().alias("Max")
        ]).sort("Samples", descending=True)

        return df.with_row_index("Rank", offset=1).select([
            "Rank", "Core Node 1", "Core Node 2", "Appearance Rate (%)", "Samples",
            "Min", "Median Scores", "Avg Scores", "Max"
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
            pl.col("Scores").list.eval(pl.element().quantile(0.25)).list.first().round(2).alias("25th Percentile Scores"),
            pl.col("Scores").list.median().round(2).alias("Median Scores"),
            pl.col("Scores").list.eval(pl.element().quantile(0.75)).list.first().round(2).alias("75th Percentile Scores"),
            pl.col("Scores").list.eval(pl.element().std(ddof=1)).list.first().round(2).alias("Std Dev Scores"),
            # Aggregations
            pl.col("Scores").list.min().alias("Min Scores"),
            pl.col("Scores").list.mean().round(2).alias("Average Scores"),
            pl.col("Scores").list.max().alias("Max Scores")
        ]).sort("Samples", descending=True)

        return df.with_row_index("Rank", offset=1).select([
            "Rank", "Character Node 1", "Character Node 2", "Samples", "Appearance Rate (%)",
            "Min Scores", "25th Percentile Scores", "Median Scores",
            "75th Percentile Scores", "Average Scores", "Std Dev Scores", "Max Scores"
        ])
    def plot_statistics_all(self, cumulative=False, output=True):
        title = f"Avg Scores Frequency for all for version {self.version}, Node {self.node}, up to {self.by_ed} Eidolon"

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
        title = f"Combined Avg Scores Frequency for version {self.version}, up to {self.by_ed} Eidolon"

        # Pass your raw combined-node DataFrame here
        return self._plot_cycle_distribution(
            df=self.combined, # <-- Change to your actual combined raw dataframe variable
            round_num="total_scores",
            eidolon_col="combined_max_ed", # <-- Column representing max combined eidolon
            cumulative=cumulative,
            output=output,
            title=title
        )
