import polars as pl
import os
import orjson
from itertools import chain
import matplotlib.pyplot as plt
import polars.selectors as cs
from dotenv import load_dotenv

load_dotenv()

pl.Config.set_tbl_rows(100)      # -1 or None shows all rows
pl.Config.set_tbl_cols(-1)      # -1 or None shows all columns
pl.Config.set_fmt_str_lengths(100)  # Prevents long strings from being cut off

class HonkaiStatistics_V2_APOC_Batch:
    def __init__(self, version, floor, node=0, by_ed=6, by_Scores =0, by_ed_inclusive=False,
                 by_ed_inclusive_combined=False, by_char=None, by_Scores_combined = 0,
                 not_char=False, sustain_condition=None, star_num=None):

        self.version, self.floor, self.node ,self.star_num= version, floor, node,star_num
        self.by_ed, self.by_Scores = by_ed, by_Scores
        self.by_ed_inclusive = by_ed_inclusive
        self.by_ed_inclusive_combined = by_ed_inclusive_combined
        self.by_Scores_combined = by_Scores_combined
        self.key = "APOC_VERSIONS"
        self.folder = "raw_data"

        # Initialize the two lazy lists
        self.lazy_frames = []
        self.char_lazy_frames = []

        # Populate the lists using our sequence builder
        self._build_lazy_sequences()

    def _load_data(self, version):
        """Scans raw sources and returns two optimized LazyFrames for a given version."""
        path = os.path.join(self.folder, f"{version}_as.parquet")
        path2 = os.path.join(self.folder, f"{version}_char.parquet")
        
        # --- PIPELINE 1: ACCOUNT STAGE RECORDS ---
        if os.path.exists(path):
            if self.node in (0, "all"):
                floor_filter = pl.col("floor") == self.floor
            else:
                floor_filter = (pl.col("floor") == self.floor) & (pl.col("node") == self.node)

            stage_lf = pl.scan_parquet(path).filter(floor_filter)
        else:
            url = f"https://huggingface.co/datasets/LvlUrArti/MocData/resolve/main/{version}_as.csv"
            temp_df = pl.read_csv(url)
            os.makedirs(self.folder, exist_ok=True)
            temp_df.write_parquet(path)
            stage_lf = pl.scan_parquet(path)

        stage_lf = stage_lf.with_columns([
            pl.col("uid").cast(pl.String),
            cs.contains("cons").cast(pl.Float64),
            pl.lit(version).alias("version")
        ])

        # --- PIPELINE 2: CHARACTER INDIVIDUAL BUILD RECORDS ---
        if os.path.exists(path2):    
            char_lf = pl.scan_parquet(path2)
        else:
            url = f"https://huggingface.co/datasets/LvlUrArti/MocData/resolve/main/{version}_char.csv"
            temp_df = pl.read_csv(url)
            os.makedirs(self.folder, exist_ok=True)
            temp_df.write_parquet(path2)
            char_lf = pl.scan_parquet(path2)

        char_lf = char_lf.with_columns([
            pl.col("uid").cast(pl.String),
            cs.contains("cons").cast(pl.Float64),
            pl.lit(version).alias("version")
        ])

        return stage_lf, char_lf

    def _build_lazy_sequences(self):
        """Processes version keys and builds the separate lazy execution tracks."""
        if self.version == "all":
            raw_val = os.getenv(self.key)
            if raw_val:
                versions = [v.strip() for v in raw_val.split(',') if v.strip()]
                for v in versions:
                    stage_lf, char_lf = self._load_data(v)
                    self.lazy_frames.append(stage_lf)
                    self.char_lazy_frames.append(char_lf)
        else:
            stage_lf, char_lf = self._load_data(self.version)
            self.lazy_frames.append(stage_lf)
            self.char_lazy_frames.append(char_lf)

        # --- GLOBAL CLEANUP APPLIED ONCE AFTER CONCAT ---
        corrupt_bullets = ["â€¢", "Ã¢â‚¬Â¢"]
        clean_bullets = ["•", "•"]

        # Wrap this where you combine your arrays into a single master LazyFrame
        if self.lazy_frames:
           
            # 1. Combine them vertically (faster than diagonal if schema matches)
            combined_stage = pl.concat(self.lazy_frames, how="vertical")
            combined_char = pl.concat(self.char_lazy_frames, how="vertical")

            # 2. Run the string adjustments once globally across all records
            lf = combined_stage.with_columns(
                cs.string().exclude("uid")
                .str.replace_many(corrupt_bullets, clean_bullets)
                .str.replace_all(r"\band\b", "&")
                .str.replace_all(r"^March 7th$", "Ice March 7th"),  # Strict full-string match
)

            char_lf = combined_char.with_columns(
                cs.string().exclude("uid")
                .str.replace_many(corrupt_bullets, clean_bullets)
                .str.replace_all(r"\band\b", "&")
                .str.replace_all(r"^March 7th$", "Ice March 7th"),  # Strict full-string match
            )
        
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
        
        if self.star_num:
            lf = lf.filter((pl.col("star_num")) == self.star_num)
            

       

        # 4. EIDOLON & SUSTAIN LOGIC (Vectorized)
        char_cols = ["ch1", "ch2", "ch3", "ch4"]
        cons_cols = ["cons1", "cons2", "cons3", "cons4"]

        lf = lf.with_columns([pl.col(c).fill_null(-1) for c in cons_cols])

        limited_names = self.rol.filter(pl.col("is_limited")).select("char").collect().to_series().to_list()
        sustain_names = self.rol.filter(pl.col("is_sustain")).select("char").collect().to_series().to_list()
        dps_names = self.rol.filter(pl.col("is_dps")).select("char").collect().to_series().to_list()
        char_to_index = {
              row["char"]: row["sort_index"]
              for row in self.rol.collect().to_dicts()
          }

        lf = lf.with_columns([
            pl.max_horizontal([
                pl.when(pl.col(char_cols[i]).is_in(limited_names)).then(pl.col(cons_cols[i])).otherwise(0)
                for i in range(4)
            ]).alias("max_eidolon"),
            pl.any_horizontal([pl.col(c).is_in(sustain_names) for c in char_cols]).alias("has_sustain")
        ])

        if self.by_ed_inclusive:
            lf = lf.filter(pl.col("max_eidolon") == self.by_ed).with_columns(
                pl.lit(self.by_ed).alias("up_to_eidolon_level")
            )
        elif self.by_ed =="all":
            self.frames = []
            # 1. Handle the '6' case first and add it to your frames list
            df_6 = lf.with_columns(pl.lit(6).alias("up_to_eidolon_level"))
            self.frames.append(df_6)
            
            # 2. Run your loop for 0, 1, and 2
            for eidolon in [0, 1, 2]:
                df = lf.filter(pl.col('max_eidolon') <= eidolon)
                self.frames.append(df.with_columns(pl.lit(eidolon).alias("up_to_eidolon_level")))
            
            # 3. Stack them all together (this will now include 6, 0, 1, and 2)
            lf = pl.concat(self.frames, how="vertical")
        else:
            lf = lf.filter(pl.col('max_eidolon') <= self.by_ed).with_columns(
                pl.lit(self.by_ed).alias("up_to_eidolon_level")
            )

        self.lf = lf.filter(pl.col("round_num") >= self.by_Scores)
      
        
        # If node is 0, process combined data as well
        if self.node == 0 or self.node=="all":
            # We must include version so we can join properly
            base_cols_for_n = ["uid", "version","up_to_eidolon_level" ,"node", "round_num", "max_eidolon"] + char_cols
            lf_base_for_combined = lf.select(base_cols_for_n)

            n1 = lf_base_for_combined.filter(pl.col("node") == 1).select([
                pl.col("uid"), pl.col("version"),"up_to_eidolon_level",
                pl.concat_list(char_cols).alias("n1_chars").list.eval(
                        pl.element().sort_by(pl.element().replace_strict(char_to_index, default=999))),
                pl.col("round_num").alias("n1_Scores"),
                pl.col("max_eidolon").alias("n1_max_ed")
            ])

            n2 = lf_base_for_combined.filter(pl.col("node") == 2).select([
                pl.col("uid"), pl.col("version"),"up_to_eidolon_level",
                pl.concat_list(char_cols).alias("n2_chars").list.eval(
                        pl.element().sort_by(pl.element().replace_strict(char_to_index, default=999))),
                pl.col("round_num").alias("n2_Scores"),
                pl.col("max_eidolon").alias("n2_max_ed")
            ])

            # JOIN on uid AND version
            combined = n1.join(n2, on=["uid", "version","up_to_eidolon_level"], how="inner")

            combined = combined.with_columns([
                (pl.col("n1_Scores")+pl.col("n2_Scores")).alias("total_Scores"),
                pl.max_horizontal(["n1_max_ed", "n2_max_ed"]).alias("combined_max_ed")
            ])

            if self.by_ed_inclusive_combined:
                combined = combined.filter(pl.col("combined_max_ed") == self.by_ed)

            self.combined = combined.filter(pl.col("total_Scores") >= self.by_Scores_combined)
            self._process_combined_data(self.combined, char_cols, cons_cols, dps_names, char_to_index)
            
            df = self.lf.with_columns(
                    pl.when(pl.col("node").is_in([1, 2]))
                    .then(0)
                    .otherwise(pl.col("node"))
                    .alias('node')
                )
            if self.node=="all":
                    self.lf = pl.concat([df, self.lf], how="diagonal")
            else:
                
                self.lf = df
                
            
        self._process_data(self.lf, char_lf, char_cols, cons_cols, dps_names, char_to_index)
    def _process_data(self, lf, char_lf, char_cols, cons_cols, dps_names, char_to_index):
        # 5. AGGREGATE CHARACTERS (The "Unpivot" trick)
        chars = self.lf.unpivot(
            index=["uid", "round_num", "has_sustain"], 
            on=char_cols, 
            value_name="Character"
        ).drop("variable")

        cons = self.lf.unpivot(
            index=["uid", "round_num", "has_sustain", "version","up_to_eidolon_level","node"], 
            on=cons_cols, 
            value_name="cons"
        ).drop(["uid", "round_num", "has_sustain", "variable"])    
                    
        base_data = (
            pl.concat([chars, cons], how="horizontal")
            .with_columns([
                pl.col("Character").fill_null("Empty Slot"),
                pl.col("cons").replace(-1, 0)
            ])
        )

        base_data = (
            base_data.join(
                char_lf, 
                left_on=["uid", 'Character', "version"], 
                right_on=["uid", 'name', "version"], 
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
            # Enforce that "version" exists exactly once in keys
            keys = list(set(group_keys + ["version","up_to_eidolon_level","node"]))

            def rollup_gear(df, gear_col, alias):
                return (
                    df.group_by(keys + [gear_col])
                    .agg([
                        pl.count("uid").alias("count"),
                        pl.col("round_num").alias("Scores") 
                    ])
                    .group_by(keys)
                    .agg(
                        pl.struct([
                            pl.col(gear_col).alias("name"), 
                            "count", 
                            "Scores"
                        ]).alias(alias)
                    )
                )

            w_df = rollup_gear(df, "weapon", "Lightcones")
            a_df = rollup_gear(df, "artifacts", "Relics")
            r_df = rollup_gear(df, "relics", "Planar_Set")

            is_eidolon = "cons" in keys
            base_stats = df.group_by(keys).agg([
                pl.count("uid").alias("Samples" if is_eidolon else "Total_Samples"),
                pl.col("round_num").alias("Scores" if is_eidolon else "Total_Scores"),
                pl.col("has_sustain").sum().alias("Sustains" if is_eidolon else "Total_Sustains"),
                pl.col("uid").unique().alias("uids")
            ])

            return (
                base_stats
                .join(w_df, on=keys, how="left")
                .join(a_df, on=keys, how="left")
                .join(r_df, on=keys, how="left")
            )

        totals = get_performance_stats(base_data, ["Character"])

        per_eidolon = (
            get_performance_stats(base_data, ["Character", "cons"])
            .with_columns(("Eidolon " + pl.col("cons").cast(pl.String)).alias("Eidolon_Level"))
            .collect()
        )

        pivoted = per_eidolon.pivot(
            on="Eidolon_Level",
            index=["version","up_to_eidolon_level","node", "Character"],
            values=["Samples", "Scores", "Sustains", "Lightcones", "Relics", "Planar_Set"],
            aggregate_function="first" 
        )

        final_df = (
            totals.collect()
            .join(pivoted, on=["version","up_to_eidolon_level","node", "Character"], how="left")
        )

        eidolon_cols = sorted([c for c in final_df.columns if "Eidolon" in c])
        header_cols = [
            "version","up_to_eidolon_level","node", "Character", "Total_Samples", "Total_Scores", "Total_Sustains", 
            "uids", "Lightcones", "Relics", "Planar_Set"
        ]
        self.char_stats = final_df.select(header_cols + eidolon_cols)

        # 2. TEAM AGGREGATION - Grouping by version
        self.team_stats = (
            lf.with_columns(pl.concat_list(char_cols).alias("temp_team"))
            .with_columns(
                pl.col("temp_team").list.eval(
                    pl.element().sort_by(pl.element().replace_strict(char_to_index, default=999))
                ).alias("team_key")
            )
            .group_by(["version","up_to_eidolon_level","node", "team_key"])
            .agg([
                pl.count("uid").alias("Samples"),
                pl.col("round_num").alias("Scores"),
                pl.col("uid").unique().alias("uids"),
                pl.col("has_sustain").sum().alias("Total_Sustains")
            ])
            .collect()
        )

        # 6. ARCHETYPE AGGREGATION
        self.archetypes_stats = (
            self.team_stats.with_columns(
                pl.col("team_key")
                .list.eval(pl.element().filter(pl.element().is_in(dps_names) & pl.element().is_not_null()))
                .alias("archetype_key")
            )
            .group_by(["version","up_to_eidolon_level","node", "archetype_key"])
            .agg([
                pl.col("Samples").sum(),
                pl.col("Scores").list.explode().alias("Scores"),
                pl.col("uids").list.explode().unique().alias("uids"),
                pl.col("Total_Sustains").sum()
            ])
        )

        new = self.team_stats.with_columns(pl.col('team_key').alias("Consequent"))
        result = new.explode('team_key').explode('Consequent')
        result = result.filter(pl.col("team_key") != pl.col("Consequent"))

        self.duos = result.group_by(["version","up_to_eidolon_level","node", pl.col('team_key').alias('Antecedent'), "Consequent"]).agg([
            pl.col("Samples").sum(),
            pl.col("Scores").list.explode().alias("Scores"),
            pl.col("uids").list.explode().unique().alias("uids"),
            pl.col("Total_Sustains").sum()
        ])
        
        # Calculate samples per version for denominator calculations
        self.total_samples_df = lf.group_by("version","up_to_eidolon_level","node").agg(pl.col("uid").n_unique().alias("version_total_samples")).collect()

    def _process_combined_data(self, combined, char_cols, cons_cols, dps_names, char_to_index):
        # 5. AGGREGATE COMBINED TEAMS
        self.combined_team_stats = (
            combined.group_by(["version","up_to_eidolon_level", "n1_chars", "n2_chars"])
            .agg([
                pl.count("uid").alias("Samples"),
                pl.col("total_Scores").alias("Scores"),
                pl.col("uid").unique().alias("uids")
            ])
            .collect()
        )

        # 6. COMBINED ARCHETYPE AGGREGATION
        self.combined_archetypes_stats = (
            self.combined_team_stats.with_columns([
                pl.col("n1_chars").list.eval(pl.element().filter(pl.element().is_in(dps_names) & pl.element().is_not_null())).alias("n1_archetype"),
                pl.col("n2_chars").list.eval(pl.element().filter(pl.element().is_in(dps_names) & pl.element().is_not_null())).alias("n2_archetype")
            ])
            .group_by(["version","up_to_eidolon_level", "n1_archetype", "n2_archetype"])
            .agg([
                pl.col("Samples").sum(),
                pl.col("Scores").list.explode().alias("Scores"),
                pl.col("uids").list.explode().unique().alias("uids")
            ])
        )

        # 6. CHARACTER PAIRING
        self.combined_char_stats = (
            combined.select(["uid", "version","up_to_eidolon_level","total_Scores", "n1_chars", "n2_chars"])
            .explode("n1_chars")
            .explode("n2_chars")
            .group_by(["version","up_to_eidolon_level", "n1_chars", "n2_chars"])
            .agg([
                pl.count("uid").alias("Samples"),
                pl.col("total_Scores").alias("Scores")
            ])
            .collect()
        )

        self.combined_total_samples_df = combined.group_by("version","up_to_eidolon_level").agg(pl.col("uid").n_unique().alias("combined_version_total_samples")).collect()


    def _plot_cycle_distribution(self, df: pl.DataFrame, round_num: str, eidolon_col: str, cumulative: bool, output: bool, title: str):
        # 1. Clean data and ensure it's collected (since we're calling individual items / plotting)
        if isinstance(df, pl.LazyFrame):
            df = df.collect()
        
        df = df.drop_nulls(subset=[round_num])
        
        if len(df) == 0:
            print("No cycle data available")
            return None

        # 2. Dynamic grouping columns determination
        group_cols = []
        if "version" in df.columns:
            group_cols.append("version")
        if "up_to_eidolon_level" in df.columns:
            group_cols.append("up_to_eidolon_level")
        if "node" in df.columns:
            group_cols.append("node")

        # 3. Compute overall stats per group (or globally if no group columns exist)
        stats_exprs = [
            pl.len().alias("sample_size"),
            pl.col(round_num).mean().alias("mean"),
            pl.col(round_num).median().alias("median"),
            pl.col(round_num).mode().first().alias("mode"),
            pl.col(round_num).std(ddof=1).fill_null(0).alias("std_dev")
        ]
        
        if group_cols:
            group_stats = df.group_by(group_cols).agg(stats_exprs)
        else:
            group_stats = df.select(stats_exprs)

        # 4. Process the main count aggregations grouped by cycle count
        agg_cols = [pl.len().alias("Count")]
        for i in range(7):
            agg_cols.append((pl.col(eidolon_col) == i).sum().alias(f"E{i}_Count"))

        # Sort explicitly by group and cycle count to ensure cumulative math runs in order
        main_group_keys = group_cols + [round_num]
        stats_df = df.group_by(main_group_keys).agg(agg_cols).sort(main_group_keys,descending=True)

        # 5. Join sample sizes back to correctly compute running totals and percentiles per track
        if group_cols:
            stats_df = stats_df.join(group_stats, on=group_cols, how="left")
        else:
            stats_df = stats_df.with_columns(group_stats.struct("*"))

       # Calculate cumulative totals securely isolated inside each unique partition block
        if group_cols:
            stats_df = stats_df.with_columns([
                pl.col("Count").cum_sum().over(group_cols).alias("Cum_Count"),
                # Total counts that took strictly LESS cycles than the current row
                (pl.col("Count").cum_sum().over(group_cols) - pl.col("Count")).alias("Strictly_Less")
            ])
        else:
            stats_df = stats_df.with_columns([
                pl.col("Count").cum_sum().alias("Cum_Count"),
                (pl.col("Count").cum_sum() - pl.col("Count")).alias("Strictly_Less")
            ])

        # Standard Percentile Ranking: 
        # Percentage of people who performed WORSE (took more cycles) than this bucket
        stats_df = stats_df.with_columns(
            (100 - ((pl.col("Strictly_Less") + (pl.col("Count"))) / pl.col("sample_size") * 100)).round(2).alias("Percentile (%)")
        )
        # Fix: Safely calculate Eidolon percentages over group blocks
        e_exprs = []
        for i in range(7):
            if cumulative:
                if group_cols:
                    # Calculate cumulative eidolon counts strictly within this group, divided by total samples in this group up to this cycle
                    e_exprs.append(
                        ((pl.col(f"E{i}_Count").cum_sum().over(group_cols) / pl.col("Cum_Count")) * 100).round(2).alias(f"E{i} (%)")
                    )
                else:
                    e_exprs.append(
                        ((pl.col(f"E{i}_Count").cum_sum() / pl.col("Cum_Count")) * 100).round(2).alias(f"E{i} (%)")
                    )
            else:
                # Non-cumulative: Just show the Eidolon distribution exactly inside this specific cycle bucket
                e_exprs.append(((pl.col(f"E{i}_Count") / pl.col("Count")) * 100).round(2).alias(f"E{i} (%)"))

        # 6. Final Select layout mapping
        select_cols = group_cols + [
            pl.col(round_num).alias("Cycles"), "Count", "Percentile (%)",
            "E0 (%)", "E1 (%)", "E2 (%)", "E3 (%)", "E4 (%)", "E5 (%)", "E6 (%)"
        ]
        final_df = stats_df.with_columns(e_exprs).select(select_cols)

        # 7. Safe Outputting & Plotting Block
        if output:
            unique_groups = df.select(group_cols).unique().to_dicts() if group_cols else [{}]

            for group in unique_groups:
                filtered_df = df
                filtered_final = final_df
                for col, val in group.items():
                    filtered_df = filtered_df.filter(pl.col(col) == val)
                    filtered_final = filtered_final.filter(pl.col(col) == val)

                if len(filtered_df) == 0:
                    continue

                m_info = group_stats.filter([pl.col(c) == v for c, v in group.items()]).to_dicts()[0] if group_cols else group_stats.to_dicts()[0]
                
                group_tag = " | ".join([f"{k}: {v}" for k, v in group.items()])
                display_title = f"{title} ({group_tag})" if group_tag else title

                print(f"\n--- {group_tag if group_tag else 'Global Distribution'} ---")
                print(f"Sample Size: {m_info['sample_size']}")
                with pl.Config(tbl_rows=-1):
                    print(filtered_final)

                # Matplotlib plotting
                cycles_list = filtered_df.get_column(round_num).to_list()

                plt.figure(figsize=(12, 6))
                plt.hist(cycles_list, bins='auto', alpha=0.5, color='blue', edgecolor='black', label='Scores Frequency')

                plt.axvline(m_info['mean'], color='orange', linestyle='dashed', linewidth=1, label=f"Mean: {m_info['mean']:.2f}")
                plt.axvline(m_info['median'], color='green', linestyle='dashed', linewidth=1, label=f"Median: {m_info['median']:.2f}")
                plt.axvline(m_info['mode'], color='red', linestyle='dashed', linewidth=1, label=f"Mode: {m_info['mode']:.2f}")
                plt.axvline(m_info['mean'] + m_info['std_dev'], color='purple', linestyle='dashed', linewidth=1, label=f"Std Dev: {m_info['std_dev']:.2f}")
                plt.axvline(m_info['mean'] - m_info['std_dev'], color='purple', linestyle='dashed', linewidth=1)

                plt.title(display_title)
                plt.xlabel('Avg Cycles')
                plt.ylabel('Frequency')
                plt.legend()
                plt.text(m_info['mean'], max(plt.ylim()) * 0.8, f"Sample Size: {m_info['sample_size']}",
                        horizontalalignment='center', fontsize=10, color='black')
                plt.show()
            
            return

        return final_df

    def get_team_df(self):
        df = self.team_stats.join(self.total_samples_df, on=["version","up_to_eidolon_level","node"], how="left").with_columns([
            pl.col("team_key").list.join(", ").map_elements(lambda s: f"({s})", return_dtype=pl.String).alias("Team"),
            (pl.col("Samples") / pl.col("version_total_samples") * 100).round(2).alias("Appearance Rate (%)"),
            (pl.col("Total_Sustains") == pl.col("Samples")).alias("Sustain?"),
            pl.col("Scores").list.eval(pl.element().quantile(0.25)).list.first().round(2).alias("25th Percentile Scores"),
            pl.col("Scores").list.median().round(2).alias("Median Scores"),
            pl.col("Scores").list.eval(pl.element().quantile(0.75)).list.first().round(2).alias("75th Percentile Scores"),
            pl.col("Scores").list.eval(pl.element().std(ddof=1)).list.first().round(2).alias("Std Dev Scores"),
            pl.col("Scores").list.min().alias("Min Scores"),
            pl.col("Scores").list.mean().round(2).alias("Average Scores"),
            pl.col("Scores").list.max().alias("Max Scores")
        ]).sort(["version", "Samples"], descending=[True, True])

        return df.with_row_index("Rank", offset=1).select([
            "Rank", "version","up_to_eidolon_level","node", "Team", "Appearance Rate (%)", "Samples",
            "Min Scores", "25th Percentile Scores", "Median Scores",
            "75th Percentile Scores", "Average Scores", "Std Dev Scores", "Max Scores", "Sustain?"
        ])

    def get_archetype_df(self):
      df = self.archetypes_stats.join(self.total_samples_df, on=["version","up_to_eidolon_level","node"], how="left").with_columns([
          pl.col("archetype_key").list.join(" + ")
              .map_elements(lambda s: s if s != "" else "Other / No DPS", return_dtype=pl.String)
              .alias("Archetype Core"),
          (pl.col("Samples") / pl.col("version_total_samples") * 100).round(2).alias("Usage %"),
          (pl.col("Total_Sustains") / pl.col("Samples") * 100).round(2).alias("Sustain_Percentage"),
          pl.col("Scores").list.min().alias("Min Scores"),
          pl.col("Scores").list.eval(pl.element().quantile(0.25)).list.first().round(2).alias("25th %"),
          pl.col("Scores").list.median().round(2).alias("Median"),
          pl.col("Scores").list.eval(pl.element().quantile(0.75)).list.first().round(2).alias("75th %"),
          pl.col("Scores").list.mean().round(2).alias("Avg Scores"),
          pl.col("Scores").list.max().alias("Max Scores"),
          pl.col("Scores").list.eval(pl.element().std(ddof=1)).list.first().round(2).alias("Std Dev Scores"),
      ]).sort(["version", "Samples"], descending=[True, True])

      return df.with_row_index("Rank", offset=1).select([
          "Rank", "version","up_to_eidolon_level","node", "Archetype Core", "Usage %", "Samples", "Sustain_Percentage",
          pl.col("Total_Sustains").alias("Sustain Samples"),
          "Min Scores", "25th %", "Median", "75th %", "Avg Scores", "Max Scores", "Std Dev Scores"
      ])

    def get_char_df(self):
        eidolon_sample_cols = [c for c in self.char_stats.columns if "Samples_Eidolon" in c]

        df = self.char_stats.join(self.total_samples_df, on=["version","up_to_eidolon_level","node"], how="left").with_columns([
            (pl.col("Total_Samples") / pl.col("version_total_samples") * 100).round(3).alias("Appearance Rate (%)"),
            (pl.col("Total_Sustains") / pl.col("Total_Samples") * 100).round(2).alias("Sustain_Percentage"),
            pl.col("Total_Scores").list.min().alias("Min Scores"),
            pl.col("Total_Scores").list.eval(pl.element().quantile(0.25)).list.first().round(2).alias("25th Percentile Scores"),
            pl.col("Total_Scores").list.median().round(2).alias("Median Scores"),
            pl.col("Total_Scores").list.eval(pl.element().quantile(0.75)).list.first().round(2).alias("75th Percentile Scores"),
            pl.col("Total_Scores").list.mean().round(2).alias("Average Scores"),
            pl.col("Total_Scores").list.eval(pl.element().std()).list.first().round(2).alias("Std Dev Scores"),
            pl.col("Total_Scores").list.max().alias("Max Scores"),
            *[
                ((pl.col(c) / pl.col("Total_Samples")) * 100).round(2).alias(f"{c.replace('Samples_', '')} %")
                for c in eidolon_sample_cols
            ]
        ])

        df = df.sort(["version", "Total_Samples"], descending=[True, True]).with_row_index("Rank", offset=1)
        eidolon_perc_cols = sorted([c for c in df.columns if "Eidolon" in c and "%" in c])

        return df.select([
            "Rank", "version","up_to_eidolon_level","node", "Character", "Appearance Rate (%)", 
            pl.col("Total_Samples").alias("Samples"),
            "Min Scores", "25th Percentile Scores", "Median Scores", 
            "75th Percentile Scores", "Average Scores", "Std Dev Scores", "Max Scores",
            pl.col("Total_Sustains").alias("Sustain Samples"), "Sustain_Percentage",
            *eidolon_perc_cols 
        ])

    def get_eidolon_performance_df(self):
        Scores_cols = [c for c in self.char_stats.columns if "Scores_Eidolon" in c]
        sustain_cols = [c for c in self.char_stats.columns if "Sustains_Eidolon" in c]

        stat_exprs = []
        for col in Scores_cols:
            label = col.replace("Scores_", "") 
            stat_exprs.append(pl.col(col).list.mean().round(2).alias(f"{label} Avg Scores"))
            
        for col in sustain_cols:
            label = col.replace("Sustains_", "")
            sample_col = f"Samples_{label}"
            if sample_col in self.char_stats.columns:
                stat_exprs.append((pl.col(col) / pl.col(sample_col) * 100).round(2).alias(f"{label} Sustain %"))

        df = self.char_stats.with_columns(stat_exprs)
        df = df.sort(["version","up_to_eidolon_level","node", "Total_Samples"], descending=[True, True]).with_row_index("Rank", offset=1)

        new_stat_cols = sorted([c for c in df.columns if "Avg Scores" in c or "Sustain %" in c])
        header_cols = ["Rank", "version","up_to_eidolon_level","node","Character", "Total_Samples", "Total_Sustains"]
        
        pl.Config.set_tbl_cols(-1)
        return df.select(header_cols + new_stat_cols)
    
    def get_duos_stats(self):
        # 1. Make sure we join on BOTH keys so we don't accidentally duplicate rows
        char_freq = (
            self.char_stats.lazy()
            .join(self.total_samples_df.lazy(), on=["version", "up_to_eidolon_level","node"], how="left")
            .select([
                "version", "up_to_eidolon_level","node", "Character",
                (pl.col("Total_Samples") / pl.col("version_total_samples")).alias("char_support")
            ])
        )

        # 2. Start with .lazy() here so it plays nice with char_freq
        rules = (
            self.duos.lazy()
            .join(char_freq, left_on=["version", "up_to_eidolon_level","node", "Antecedent"], right_on=["version", "up_to_eidolon_level","node", "Character"], how="left")
            .rename({"char_support": "support_A"})
            .join(char_freq, left_on=["version", "up_to_eidolon_level","node", "Consequent"], right_on=["version", "up_to_eidolon_level","node", "Character"], how="left")
            .rename({"char_support": "support_C"})
            .join(self.total_samples_df.lazy(), on=["version", "up_to_eidolon_level","node"], how="left")
        )

        return rules.with_columns([
            (pl.col("Samples") / pl.col("version_total_samples")).alias("support"),
        ]).with_columns([
            (pl.col("support") / pl.col("support_A")).alias("confidence"),
        ]).with_columns([
            (pl.col("confidence") / pl.col("support_C")).alias("lift"),
            (pl.col("support") - (pl.col("support_A") * pl.col("support_C"))).alias("leverage"),
            ((1 - pl.col("support_C")) / (1 - pl.col("confidence") + 1e-7)).alias("conviction")
        ]).select([
            "version", "up_to_eidolon_level","node", "Antecedent", "Consequent", "Samples",
            (pl.col("support") * 100).round(2).alias("Appearance Rate (%)"),
            pl.col("confidence").round(3).alias("Confidence"),
            pl.col("lift").round(3).alias("Lift"),
            pl.col("leverage").round(4).alias("Leverage"),
            pl.col("conviction").round(3).alias("Conviction"),
            (pl.col("Total_Sustains") / pl.col("Samples") * 100).round(2).alias("Sustain_Percentage"),
            
            # Scores stats processing (using robust list evaluation)
            pl.col("Scores").list.eval(pl.element().quantile(0.25)).list.first().round(2).alias("25th Percentile Scores"),
            pl.col("Scores").list.eval(pl.element().median()).list.first().round(2).alias("Median Scores"),
            pl.col("Scores").list.eval(pl.element().quantile(0.75)).list.first().round(2).alias("75th Percentile Scores"),
            pl.col("Scores").list.eval(pl.element().std(ddof=1)).list.first().round(2).alias("Std Dev Scores"),
            pl.col("Scores").list.min().alias("Min Scores"),
            pl.col("Scores").list.mean().round(2).alias("Average Scores"),
            pl.col("Scores").list.max().alias("Max Scores")
        ]).sort(["version","node", "Lift"], descending=[True,False, True]).collect()
    
    def get_combined_team_df(self):
        df = self.combined_team_stats.join(self.combined_total_samples_df, on=["version","up_to_eidolon_level"], how="left").with_columns([
            pl.col("n1_chars").list.join(", ").map_elements(lambda s: f"({s})", return_dtype=pl.String).alias("Team Node 1"),
            pl.col("n2_chars").list.join(", ").map_elements(lambda s: f"({s})", return_dtype=pl.String).alias("Team Node 2"),
            (pl.col("Samples") / pl.col("combined_version_total_samples") * 100).round(2).alias("Appearance Rate (%)"),
            pl.col("Scores").list.eval(pl.element().quantile(0.25)).list.first().round(2).alias("25th Percentile Scores"),
            pl.col("Scores").list.median().round(2).alias("Median Scores"),
            pl.col("Scores").list.eval(pl.element().quantile(0.75)).list.first().round(2).alias("75th Percentile Scores"),
            pl.col("Scores").list.eval(pl.element().std(ddof=1)).list.first().round(2).alias("Std Dev Scores"),
            pl.col("Scores").list.min().alias("Min Scores"),
            pl.col("Scores").list.mean().round(2).alias("Average Scores"),
            pl.col("Scores").list.max().alias("Max Scores")
        ]).sort(["version","up_to_eidolon_level", "Samples"], descending=[True,False, True])

        return df.with_row_index("Rank", offset=1).select([
            "Rank", "version","up_to_eidolon_level", "Team Node 1", "Team Node 2", "Appearance Rate (%)", "Samples",
            "Min Scores", "25th Percentile Scores", "Median Scores",
            "75th Percentile Scores", "Average Scores", "Std Dev Scores", "Max Scores"
        ])

    def get_combined_archetype_df(self):
        df = self.combined_archetypes_stats.join(self.combined_total_samples_df, on=["version","up_to_eidolon_level"], how="left").with_columns([
            pl.col("n1_archetype").list.join(" + ")
                .map_elements(lambda s: f"[{s}]" if s != "" else "[Other]", return_dtype=pl.String).alias("Core Node 1"),
            pl.col("n2_archetype").list.join(" + ")
                .map_elements(lambda s: f"[{s}]" if s != "" else "[Other]", return_dtype=pl.String).alias("Core Node 2"),
            (pl.col("Samples") / pl.col("combined_version_total_samples") * 100).round(2).alias("Appearance Rate (%)"),
            pl.col("Scores").list.eval(pl.element().quantile(0.25)).list.first().round(2).alias("25th Percentile Scores"),
            pl.col("Scores").list.median().round(2).alias("Median Scores"),
            pl.col("Scores").list.eval(pl.element().quantile(0.75)).list.first().round(2).alias("75th Percentile Scores"),
            pl.col("Scores").list.eval(pl.element().std(ddof=1)).list.first().round(2).alias("Std Dev Scores"),
            pl.col("Scores").list.min().alias("Min Scores"),
            pl.col("Scores").list.mean().round(2).alias("Average Scores"),
            pl.col("Scores").list.max().alias("Max Scores")
        ]).sort(["version", "Samples"], descending=[True, True])

        return df.with_row_index("Rank", offset=1).select([
            "Rank", "version","up_to_eidolon_level", "Core Node 1", "Core Node 2", "Appearance Rate (%)", "Samples",
            "Min Scores", "25th Percentile Scores", "Median Scores",
            "75th Percentile Scores", "Average Scores", "Std Dev Scores", "Max Scores"
        ])

    def get_combined_char_df(self):
        df = self.combined_char_stats.join(self.combined_total_samples_df, on=["version","up_to_eidolon_level"], how="left").with_columns([
            pl.col("n1_chars").alias("Character Node 1"),
            pl.col("n2_chars").alias("Character Node 2"),
            (pl.col("Samples") / pl.col("combined_version_total_samples") * 100).round(2).alias("Appearance Rate (%)"),
            pl.col("Scores").list.eval(pl.element().quantile(0.25)).list.first().round(2).alias("25th Percentile Scores"),
            pl.col("Scores").list.median().round(2).alias("Median Scores"),
            pl.col("Scores").list.eval(pl.element().quantile(0.75)).list.first().round(2).alias("75th Percentile Scores"),
            pl.col("Scores").list.eval(pl.element().std(ddof=1)).list.first().round(2).alias("Std Dev Scores"),
            pl.col("Scores").list.min().alias("Min Scores"),
            pl.col("Scores").list.mean().round(2).alias("Average Scores"),
            pl.col("Scores").list.max().alias("Max Scores")
        ]).sort(["version", "Samples"], descending=[True, True])

        return df.with_row_index("Rank", offset=1).select([
            "Rank", "version","up_to_eidolon_level", "Character Node 1", "Character Node 2", "Samples", "Appearance Rate (%)",
            "Min Scores", "25th Percentile Scores", "Median Scores",
            "75th Percentile Scores", "Average Scores", "Std Dev Scores", "Max Scores"
        ])
        
    def display_top_gear(self):
        df = self.char_stats
        eidolon_levels = sorted(list(set([c.split('_')[-1] for c in df.columns if "Eidolon" in c])))
        results = []

        for level in eidolon_levels:
            for gear_type in ["Lightcones", "Relics", "Planar_Set"]:
                col_name = f"{gear_type}_{level}"
                if col_name not in df.columns:
                    continue

                temp = (
                    df.select(["version","up_to_eidolon_level","node" ,"Character", col_name])
                    .explode(col_name)
                    .drop_nulls(col_name)
                    .with_columns([
                        pl.col(col_name).struct.field("name").alias("Gear_Name"),
                        pl.col(col_name).struct.field("count").alias("Usage"),
                        pl.col(col_name).struct.field("Scores").alias("_Scores_list")
                    ])
                    .filter(pl.col("Gear_Name") != "Info_not_found")
                )

                if temp.is_empty():
                    continue

                processed = (
                    temp.with_columns([
                        pl.col("Usage").sum().over(["version","up_to_eidolon_level","node", "Character"]).alias("_total_filtered_usage")
                    ])
                    .with_columns([
                        (pl.col("Usage") / pl.col("_total_filtered_usage")).alias("Usage_Rate"),
                        pl.col("_Scores_list").list.mean().round(2).alias("Avg_Scores"),
                        pl.col("_Scores_list").list.median().alias("Median_Scores"),
                        pl.col("_Scores_list").list.min().alias("Min_Scores"),
                        pl.col("_Scores_list").list.max().alias("Max_Scores"),
                        pl.col("_Scores_list").list.std().round(2).alias("Std_Scores"),
                        pl.col("_Scores_list").list.eval(pl.element().quantile(0.25)).list.first().alias("25th Percentile Scores"),
                        pl.col("_Scores_list").list.eval(pl.element().quantile(0.75)).list.first().alias("75th Percentile Scores"),
                    ])
                )

                full_list = processed.with_columns([
                    pl.lit(level).alias("Eidolon"),
                    pl.lit(gear_type).alias("Category")
                ])
                
                results.append(full_list.select([
                    "version","up_to_eidolon_level","node", "Character", "Eidolon", "Category", "Gear_Name", 
                    "Usage", "Usage_Rate", "Avg_Scores", "25th Percentile Scores", 
                    "Median_Scores", "75th Percentile Scores", "Min_Scores", 
                    "Max_Scores", "Std_Scores"
                ]))

        if not results:
            return pl.DataFrame()

        return (
            pl.concat(results)
            .sort(
                by=["version","up_to_eidolon_level","node", "Character", "Eidolon", pl.col("Category").str.slice(0, 1), "Usage"], 
                descending=[True,False,True, False, False, False, True]
            )
        )

    def display_single_char_full(self, char_name):
        full_df = self.display_top_gear()
        char_data = full_df.filter(pl.col("Character") == char_name)
        if char_data.is_empty():
            return f"Character '{char_name}' not found or has no valid gear data."
        return char_data.drop("Character")

    def plot_statistics_all(self, cumulative=False, output=True):
        title = f"Avg Scores Frequency for all for version {self.version}, Node {self.node}, up to {self.by_ed} Eidolon"
        return self._plot_cycle_distribution(
            df=self.lf, 
            round_num="round_num",
            eidolon_col="max_eidolon",
            cumulative=cumulative,
            output=output,
            title=title
        )

    def plot_statistics_all_combined(self, cumulative=False, output=True):
        title = f"Combined Avg Scores Frequency for version {self.version}, up to {self.by_ed} Eidolon"
        return self._plot_cycle_distribution(
            df=self.combined,
            round_num="total_Scores",
            eidolon_col="combined_max_ed",
            cumulative=cumulative,
            output=output,
            title=title
        )