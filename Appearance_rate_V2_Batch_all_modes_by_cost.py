import polars as pl
import os
import orjson
from itertools import chain, combinations_with_replacement
import matplotlib.pyplot as plt
import polars.selectors as cs
from dotenv import load_dotenv

load_dotenv()

pl.Config.set_tbl_rows(100)
pl.Config.set_tbl_cols(-1)
pl.Config.set_fmt_str_lengths(100)


class HonkaiStatistics_V2_eidolon_batch:
    def __init__(self, version, floor, node=0, by_ed=None, by_ed_start=0, by_ed_end=6,
                 by_Scores=None, by_Scores_combined=None,
                 by_Points=None, by_Points_combined=None,
                 by_cycle=None, by_cycles_combined=None,
                 by_ed_inclusive=False, by_ed_inclusive_combined=False, by_char=None,
                 not_char=False, sustain_condition=None, star_num=None, is_starward=True,
                 hard_mode=False, mode="apoc"):

        self.version, self.floor, self.node, self.star_num, self.is_starward = version, floor, node, star_num, is_starward
        self.by_ed_start, self.by_ed_end, self.by_ed = by_ed_start, by_ed_end, by_ed
        self.by_ed_inclusive = by_ed_inclusive
        self.by_ed_inclusive_combined = by_ed_inclusive_combined
        
        self.mode = mode.lower().strip()
        self.hard_mode = hard_mode

        # Configure metrics, threshold parameters, and keys based on mode
        if self.mode == "apoc":
            self.key = "APOC_VERSIONS"
            self.folder = "raw_data"
            self.metric_name = "Scores"
            self.struct_metric_field = "Scores"
            self.is_lower_better = False
            self.threshold = by_Scores if by_Scores is not None else 0
            self.threshold_combined = by_Scores_combined if by_Scores_combined is not None else 0
        elif self.mode == "pure fiction":
            self.key = "PF_VERSIONS"
            self.folder = "raw_data"
            self.metric_name = "Points"
            self.struct_metric_field = "Points"
            self.is_lower_better = False
            self.threshold = by_Points if by_Points is not None else 0
            self.threshold_combined = by_Points_combined if by_Points_combined is not None else 0
        elif self.mode == "moc":
            self.key = "MOC_VERSIONS"
            self.folder = "raw_data"
            self.metric_name = "Cycles"
            self.struct_metric_field = "cycles"
            self.is_lower_better = True
            self.threshold = by_cycle if by_cycle is not None else 30
            self.threshold_combined = by_cycles_combined if by_cycles_combined is not None else 30
        elif self.mode == "anomaly":
            self.key = "ANOMALY_VERSIONS"
            self.folder = "raw_data"
            self.metric_name = "Cycles"
            self.struct_metric_field = "cycles"
            self.is_lower_better = True
            self.threshold = by_cycle if by_cycle is not None else 30
            self.threshold_combined = by_cycles_combined if by_cycles_combined is not None else 30
        else:
            raise ValueError(f"Unknown mode: {self.mode}")

        self.node_or_floor_col = "floor" if self.mode == "anomaly" else "node"

        self.lazy_frames = []
        self.char_lazy_frames = []

        self._build_lazy_sequences()

    # ------------------------------------------------------------------
    # Schema helper — check once, cache the result
    # ------------------------------------------------------------------
    def _schema_has_starward(self, lf: pl.LazyFrame) -> bool:
        """Return True if the LazyFrame schema contains an 'is_starward' column."""
        return "is_starward" in lf.collect_schema().names()

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------
    def _load_data(self, version):
        if self.mode == "apoc":
            suffix = "_as"
        elif self.mode == "pure fiction":
            suffix = "_pf"
        elif self.mode == "moc":
            suffix = ""
        elif self.mode == "anomaly":
            suffix = "_aa"

        path = os.path.join(self.folder, f"{version}{suffix}.parquet")
        path2 = os.path.join(self.folder, f"{version}_char.parquet")

        if os.path.exists(path):
            if self.mode == "anomaly":
                if self.floor == 0:
                    floor_filter = pl.col("floor") != 4
                else:
                    floor_filter = pl.col("floor") == self.floor

                if self.floor != "all":
                    stage_lf = pl.scan_parquet(path).filter(floor_filter)
                else:
                    stage_lf = pl.scan_parquet(path)
            else:
                if self.node in (0, "all"):
                    floor_filter = pl.col("floor") == self.floor
                else:
                    floor_filter = (pl.col("floor") == self.floor) & (pl.col("node") == self.node)

                stage_lf = pl.scan_parquet(path).filter(floor_filter)
        else:
            url = f"https://huggingface.co/datasets/LvlUrArti/MocData/resolve/main/{version}{suffix}.csv"
            temp_df = pl.read_csv(url)
            os.makedirs(self.folder, exist_ok=True)
            temp_df.write_parquet(path)
            stage_lf = pl.scan_parquet(path)

        stage_lf = stage_lf.with_columns([
            pl.col("uid").cast(pl.String),
            cs.contains("cons").cast(pl.Float64),
            pl.lit(version).alias("version")
        ])

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

    # ------------------------------------------------------------------
    # Build lazy sequences
    # ------------------------------------------------------------------
    def _build_lazy_sequences(self):
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

        corrupt_bullets = ["â€¢", "Ã¢â‚¬Â¢"]
        clean_bullets = ["•", "•"]

        if self.lazy_frames:
            combined_stage = pl.concat(self.lazy_frames, how="diagonal")
            combined_char = pl.concat(self.char_lazy_frames, how="diagonal")

            lf = combined_stage.with_columns(
                cs.string().exclude("uid")
                .str.replace_many(corrupt_bullets, clean_bullets)
                .str.replace_all(r"\band\b", "&")
                .str.replace_all(r"^March 7th$", "Ice March 7th"),
            )

            char_lf = combined_char.with_columns(
                cs.string().exclude("uid")
                .str.replace_many(corrupt_bullets, clean_bullets)
                .str.replace_all(r"\band\b", "&")
                .str.replace_all(r"^March 7th$", "Ice March 7th"),
            )

        # Detect Starward Mode once here, store on self for use downstream
        self._has_starward_col = self._schema_has_starward(lf)

        # Apply is_starward filter early — no scattered if-checks downstream
        if self._has_starward_col:
            # Allow older versions (where the column is null) to bypass the filter,
            # while correctly filtering the newer versions.
            lf = lf.filter(
                pl.col("is_starward").is_null() | (pl.col("is_starward") == self.is_starward)
            )

        with open('characters.json', 'rb') as f:
            info = orjson.loads(f.read())

        self.rol = pl.DataFrame([
            {"char": k, "is_limited": v.get('availability') == 'Limited 5*',
             "is_sustain": 'sustain' in v.get('role', []),
             "is_dps": bool(set(v.get('role', [])).intersection({"dps", "specialist"})),
             "sort_index": i}
            for i, (k, v) in enumerate(info.items())
        ]).lazy()

        if self.star_num:
            lf = lf.filter(pl.col("star_num") == self.star_num)

        if self.mode == "anomaly":
            lf = lf.filter(pl.col('hard_mode') == self.hard_mode)

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

        row_max_stars_expr = (
            pl.when(pl.col("is_starward").is_not_null()).then(pl.lit(4)).otherwise(pl.lit(3))
            if self._has_starward_col else pl.lit(3)
        )

        lf = lf.with_columns([
            pl.max_horizontal([
                pl.when(pl.col(char_cols[i]).is_in(limited_names)).then(pl.col(cons_cols[i])).otherwise(0)
                for i in range(4)
            ]).alias("max_eidolon"),
            pl.sum_horizontal([
                pl.when(pl.col(char_cols[i]).is_in(limited_names)).then(pl.col(cons_cols[i]) + pl.lit(1)).otherwise(0)
                for i in range(4)
            ]).alias("estimated_min_cost"),
            pl.sum_horizontal([
                pl.when(pl.col(char_cols[i]).is_in(limited_names)).then(pl.col(cons_cols[i]) + pl.lit(2)).otherwise(1)
                for i in range(4)
            ]).alias("estimated_max_cost"),
            pl.any_horizontal([pl.col(c).is_in(sustain_names) for c in char_cols]).alias("has_sustain"),
            pl.concat_list(char_cols).alias("temp_team"),
            pl.concat_list(cons_cols).alias("temp_cons"),
            (pl.col('star_num') == row_max_stars_expr).alias("is_full_clear"),
        ])

        # Use pl.struct inside a list concatenation to map them element-wise
        df_struct = lf.with_columns(
            char_cons_pairs = pl.concat_list([
                pl.struct([
                    pl.col("temp_team").list.get(i).alias("char"),
                    pl.col("temp_cons").list.get(i).alias("eidolon")
                ])
                for i in range(4)
            ])
        )

        # 1. Create and sort the pairs
        df_struct = df_struct.with_columns(
            pl.col("char_cons_pairs").list.eval(
                pl.element().sort_by(pl.element().struct.field('char').replace_strict(char_to_index, default=999))
            ).alias("char_cons_pairs_sorted")
        )

        # 2. Extract archetypes using the fully created sorted column
        df_struct = df_struct.with_columns(
            pl.col("char_cons_pairs_sorted").list.eval(
                pl.element().filter(
                    pl.element().struct.field('char').is_in(dps_names) & 
                    pl.element().struct.field('char').is_not_null()
                )
            ).alias("archetypes_pairs_sorted")
        )

        # 3. Second pass: Extract the team_key from the newly created column
        df_struct = df_struct.with_columns([
            pl.col("char_cons_pairs_sorted").list.eval(
                pl.element().struct.field('char')
            ).alias("team_key"),
            pl.col("archetypes_pairs_sorted").list.eval(
                pl.element().struct.field('char')
            ).alias("archetype_key")
        ])

        df_struct = df_struct.with_columns([
            pl.col("char_cons_pairs_sorted").list.eval(
                pl.element().struct.field('char').cast(pl.String) +
                "(E" + pl.element().struct.field('eidolon').cast(pl.Int64).cast(pl.String)  + ")"
            ).alias("char_cons_zipped_sorted"),
            pl.col("archetypes_pairs_sorted").list.eval(
                pl.element().struct.field('char').cast(pl.String) +
                "(E" + pl.element().struct.field('eidolon').cast(pl.Int64).cast(pl.String)  + ")"
            ).alias("archetypes_pairs_zipped_sorted")
        ])

        self.lf = df_struct.drop(["temp_team", "temp_cons", "char_cons_pairs_sorted", "archetypes_pairs_sorted"])

        if self.node == 0 or self.node == "all" or (self.mode == "anomaly" and self.floor == 0):
            base_cols_for_n = ["uid", "version", self.node_or_floor_col, "round_num", "max_eidolon", "char_cons_zipped_sorted", "archetypes_pairs_zipped_sorted", "estimated_min_cost", "estimated_max_cost", "has_sustain", "star_num", "is_full_clear", "team_key", "archetype_key"] 
            lf_base_for_combined = self.lf.select(base_cols_for_n)

            prefix = "f" if self.mode == "anomaly" else "n"

            def _node_lf(node_num, metric_alias, ed_alias, min_cost_alias, max_cost_alias):
                return lf_base_for_combined.filter(pl.col(self.node_or_floor_col) == node_num).select([
                    pl.col("char_cons_zipped_sorted").alias(f"{prefix}{node_num}_char_cons_zipped_sorted"),
                    pl.col("archetypes_pairs_zipped_sorted").alias(f"{prefix}{node_num}_archetypes_zipped_sorted"),
                    pl.col("team_key").alias(f"{prefix}{node_num}_team_key"),
                    pl.col("archetype_key").alias(f"{prefix}{node_num}_archetype_key"),
                    pl.col("uid"), pl.col("version"),
                    pl.col("round_num").alias(metric_alias),
                    pl.col("max_eidolon").alias(ed_alias),
                    pl.col("is_full_clear").alias(f"{prefix}{node_num}_is_full_clear" if self.mode == "anomaly" else "is_full_clear"),
                    pl.col("has_sustain").alias(f"{prefix}{node_num}_has_sustain"),
                    pl.col("estimated_min_cost").alias(min_cost_alias),
                    pl.col("estimated_max_cost").alias(max_cost_alias)
                ])

            join_keys = ["uid", "version","is_full_clear"]

            if self.mode == "anomaly":
                n1 = _node_lf(1, "n1_metric", "n1_max_ed", "n1_min_estimated_cost", "n1_max_estimated_cost")
                n2 = _node_lf(2, "n2_metric", "n2_max_ed", "n2_min_estimated_cost", "n2_max_estimated_cost")
                n3 = _node_lf(3, "n3_metric", "n3_max_ed", "n3_min_estimated_cost", "n3_max_estimated_cost")
                join_keys = ["uid", "version"]
                combined = n1.join(n2, on=join_keys, how="inner")
                combined = combined.join(n3, on=join_keys, how="inner")

                combined = combined.with_columns([
                    (pl.col("n1_metric") + pl.col("n2_metric") + pl.col("n3_metric")).alias("total_metric"),
                    pl.max_horizontal(["n1_max_ed", "n2_max_ed", "n3_max_ed"]).alias("combined_max_ed"),
                    (pl.col("n1_min_estimated_cost") + pl.col("n2_min_estimated_cost") + pl.col("n3_min_estimated_cost")).alias("total_min_estimated_cost"),
                    (pl.col("n1_max_estimated_cost") + pl.col("n2_max_estimated_cost") + pl.col("n3_max_estimated_cost")).alias("total_max_estimated_cost"),
                ])
            else:
                n1 = _node_lf(1, "n1_metric", "n1_max_ed", "n1_min_estimated_cost", "n1_max_estimated_cost")
                n2 = _node_lf(2, "n2_metric", "n2_max_ed", "n2_min_estimated_cost", "n2_max_estimated_cost")

                combined = n1.join(n2, on=join_keys, how="inner")

                if self._has_starward_col and self.is_starward:
                    n3 = _node_lf(3, "n3_metric", "n3_max_ed", "n3_min_estimated_cost", "n3_max_estimated_cost")
                    combined = combined.join(n3, on=join_keys, how="left")
                    combined = combined.with_columns([
                        pl.sum_horizontal(["n1_metric", "n2_metric", "n3_metric"]).alias("total_metric"),
                        pl.sum_horizontal(["n1_min_estimated_cost", "n2_min_estimated_cost", "n3_min_estimated_cost"]).alias("total_min_estimated_cost"),
                        pl.sum_horizontal(["n1_max_estimated_cost", "n2_max_estimated_cost", "n3_max_estimated_cost"]).alias("total_max_estimated_cost"),
                        pl.max_horizontal(["n1_max_ed", "n2_max_ed", "n3_max_ed"]).alias("combined_max_ed")
                    ])
                else:
                    combined = combined.with_columns([
                        (pl.col("n1_metric") if self.mode == "moc" else (pl.col("n1_metric") + pl.col("n2_metric"))).alias("total_metric"),
                        pl.max_horizontal(["n1_max_ed", "n2_max_ed"]).alias("combined_max_ed"),
                        (pl.col("n1_min_estimated_cost") + pl.col("n2_min_estimated_cost")).alias("total_min_estimated_cost"),
                        (pl.col("n1_max_estimated_cost") + pl.col("n2_max_estimated_cost")).alias("total_max_estimated_cost"),
                    ])

            if self.is_lower_better:
                self.combined = combined.filter(pl.col("total_metric") <= self.threshold_combined)
            else:
                self.combined = combined.filter(pl.col("total_metric") >= self.threshold_combined)

            self._process_combined_data(self.combined)

            if self.mode == "anomaly":
                standard_nodes = [1, 2, 3]
            else:
                standard_nodes = [1, 2, 3] if (self._has_starward_col and self.is_starward) else [1, 2]

            df = self.lf.with_columns(
                pl.when(pl.col(self.node_or_floor_col).is_in(standard_nodes))
                .then(0)
                .otherwise(pl.col(self.node_or_floor_col))
                .alias(self.node_or_floor_col)
            )

            if (self.mode == "anomaly" and self.floor == "all") or (self.mode != "anomaly" and self.node == "all"):
                if self.mode == "anomaly":
                    self.lf = pl.concat([df, self.lf.filter(pl.col("floor") != 4)], how="diagonal")
                else:
                    self.lf = pl.concat([df, self.lf], how="diagonal")
            else:
                self.lf = df

        self._process_data(self.lf)

    # ------------------------------------------------------------------
    # Per-node aggregation
    # ------------------------------------------------------------------
    def _process_data(self, lf):
        base_data = (
            self.lf.select([
                "uid", "round_num", "has_sustain", "version",
                "estimated_min_cost", "estimated_max_cost", self.node_or_floor_col, "max_eidolon",
                'team_key',"is_full_clear"
            ])
        ).explode("team_key")

        base_data2 = (
            self.lf.select([
                "uid", "round_num", "has_sustain", "version",
                "estimated_min_cost", "estimated_max_cost", self.node_or_floor_col, "max_eidolon",
                "is_full_clear", 'char_cons_zipped_sorted',
            ])
        ).explode("char_cons_zipped_sorted")

        self.chars_max_eidolon_cost_individual_eidolon = base_data.group_by(["version", "estimated_min_cost", "estimated_max_cost", self.node_or_floor_col, "team_key", "max_eidolon"]).agg([
            pl.col("uid").count().alias("Samples"),
            pl.col("round_num").alias("Scores"),
            pl.col("uid").unique().alias("uids"),
            pl.col("has_sustain").sum().alias("Total_Sustains"),
            pl.col("is_full_clear").sum().alias("Total_Full_Clears")
        ]).collect()

        self.chars_by_cost_individual_eidolon = base_data2.group_by(["version", "estimated_min_cost", "estimated_max_cost", self.node_or_floor_col, "char_cons_zipped_sorted","max_eidolon"]).agg([
            pl.col("uid").count().alias("Samples"),
            pl.col("round_num").alias("Scores"),
            pl.col("uid").unique().alias("uids"),
            pl.col("has_sustain").sum().alias("Total_Sustains"),
            pl.col("is_full_clear").sum().alias("Total_Full_Clears")
        ]).collect()

        self.chars_by_cost = self.chars_max_eidolon_cost_individual_eidolon.group_by(["version", "estimated_min_cost", "estimated_max_cost", self.node_or_floor_col, "team_key"]).agg([
            pl.col('Samples').sum(),
            pl.col("Scores").explode(),
            pl.col("uids").explode(),
            pl.col("Total_Sustains").sum(),
            pl.col("Total_Full_Clears").sum()
        ])

        self.chars_by_individual_eidolons = self.chars_by_cost_individual_eidolon.group_by(["version", self.node_or_floor_col, "char_cons_zipped_sorted"]).agg([
            pl.col('Samples').sum(),
            pl.col("Scores").explode(),
            pl.col("uids").explode(),
            pl.col("Total_Sustains").sum(),
            pl.col("Total_Full_Clears").sum()
        ])

        self.teams = self.lf.group_by(["version", "estimated_min_cost", "estimated_max_cost", self.node_or_floor_col, "char_cons_zipped_sorted", "team_key", "archetypes_pairs_zipped_sorted", "archetype_key", "max_eidolon"]).agg([
            pl.count("uid").alias("Samples"),
            pl.col("round_num").alias("Scores"),
            pl.col("uid").unique().alias("uids"),
            pl.col("has_sustain").sum().alias("Total_Sustains"),
            pl.col("is_full_clear").sum().alias("Total_Full_Clears")
        ]).collect()

        self.archetypes = self.teams.group_by(["version", "estimated_min_cost", "estimated_max_cost", self.node_or_floor_col, "archetypes_pairs_zipped_sorted", "archetype_key", "max_eidolon"]).agg([
            pl.sum("Samples"),
            pl.col("Scores").explode(),
            pl.col("uids").explode().unique(),
            pl.col("Total_Full_Clears").sum(),
            (pl.col("Total_Sustains")).sum()
        ])

        new = self.teams.select("version", self.node_or_floor_col, 'char_cons_zipped_sorted', "Samples", "Scores", "Total_Full_Clears","Total_Sustains")
        new = new.with_columns(pl.col('char_cons_zipped_sorted').alias("Consequent"))
        result = new.explode('char_cons_zipped_sorted').explode('Consequent')
        result = result.filter(pl.col("char_cons_zipped_sorted") != pl.col("Consequent"))

        self.duos = result.group_by(
            ["version", self.node_or_floor_col, pl.col('char_cons_zipped_sorted').alias('Antecedent'), "Consequent"]
        ).agg([
            pl.sum("Samples"),
            pl.col("Scores").list.explode().alias("Scores"),
            pl.col("Total_Full_Clears").sum(),
            (pl.col("Total_Sustains")).sum()
        ])

        self.total_samples_df = self.lf.group_by(
            "version", self.node_or_floor_col
        ).agg(pl.col("uid").n_unique().alias("version_total_samples")).collect()

    # ------------------------------------------------------------------
    # Combined (cross-node) aggregation
    # ------------------------------------------------------------------
    def _process_combined_data(self, combined):
        node_char_cols = [c for c in combined.collect_schema().names() if c.endswith("_zipped_sorted") or c.endswith("_key")] 

        self.combined_team_stats = (
            combined.group_by(["version", "combined_max_ed", "total_min_estimated_cost", "total_max_estimated_cost"] + node_char_cols)
            .agg([
                pl.count("uid").alias("Samples"),
                pl.col("total_metric").alias("Scores"),
                pl.col("uid").unique().alias("uids"),
                
                cs.ends_with("_has_sustain").sum(),
           
                cs.ends_with("_full_clear").sum()
            ])
            .collect()
        )

        agg_cols = ["Samples", "Scores", "uids"]

        group_keys = [
            c for c in self.combined_team_stats.collect_schema().names() 
            if not c.endswith(("_char_cons_zipped_sorted", "_team_key")) 
            and c not in agg_cols
        ]

        self.combined_archetype_stats = (
            self.combined_team_stats.group_by(group_keys)
            .agg([
                pl.col("Samples").sum(),
                pl.col("Scores").explode(),
                pl.col("uids").explode().unique(),
                cs.ends_with("_has_sustain").sum(),
                
                cs.ends_with("_full_clear").sum(),
                
            ])
        )

        self.combined_total_samples_df = combined.group_by(
            "version"
        ).agg(pl.col("uid").n_unique().alias("combined_version_total_samples")).collect()

        self._node_char_cols = node_char_cols


   
    # ------------------------------------------------------------------
    # Public getters (Display Functions)
    # ------------------------------------------------------------------
    def get_teams_df(self):
        df = self.teams.join(
            self.total_samples_df, on=["version", self.node_or_floor_col], how="left"
        ).with_columns([
            pl.col("char_cons_zipped_sorted").list.join(", ").map_elements(lambda s: f"({s})", return_dtype=pl.String).alias("Team"),
            pl.col("archetypes_pairs_zipped_sorted").list.join(" + ").map_elements(lambda s: s if s != "" else "Other / No DPS", return_dtype=pl.String).alias("Archetype Core"),
            (pl.col("Samples") / pl.col("version_total_samples") * 100).round(2).alias("Appearance Rate (%)"),
           (pl.col("Total_Full_Clears")/pl.col("Samples")* 100).round(2).alias("Full_Clear_Rate"),
            (pl.col("Total_Sustains") == pl.col("Samples")).alias("has_sustain"),
            pl.col("Scores").list.eval(pl.element().quantile(0.25)).list.first().round(2).alias(f"25th Percentile {self.metric_name}"),
            pl.col("Scores").list.median().round(2).alias(f"Median {self.metric_name}"),
            pl.col("Scores").list.eval(pl.element().quantile(0.75)).list.first().round(2).alias(f"75th Percentile {self.metric_name}"),
            pl.col("Scores").list.eval(pl.element().std(ddof=1)).list.first().round(2).alias(f"Std Dev {self.metric_name}"),
            pl.col("Scores").list.min().alias(f"Min {self.metric_name}"),
            pl.col("Scores").list.mean().round(2).alias(f"Average {self.metric_name}"),
            pl.col("Scores").list.max().alias(f"Max {self.metric_name}")
        ]).sort(["version", self.node_or_floor_col, "Samples"], descending=[True, False, True])

        return df.with_row_index("Rank", offset=1).select([
            "Rank", "version", "estimated_min_cost", "estimated_max_cost", self.node_or_floor_col, "max_eidolon",
            "Team", "Archetype Core", "has_sustain",
            "Appearance Rate (%)", "Samples", "Total_Full_Clears", "Full_Clear_Rate",
            f"Min {self.metric_name}", f"25th Percentile {self.metric_name}", f"Median {self.metric_name}",
            f"75th Percentile {self.metric_name}", f"Average {self.metric_name}", f"Std Dev {self.metric_name}", f"Max {self.metric_name}"
        ])

    def get_archetypes_df(self):
        df = self.archetypes.join(
            self.total_samples_df, on=["version", self.node_or_floor_col], how="left"
        ).with_columns([
            pl.col("archetypes_pairs_zipped_sorted").list.join(" + ").map_elements(lambda s: s if s != "" else "Other / No DPS", return_dtype=pl.String).alias("Archetype Core"),
            (pl.col("Samples") / pl.col("version_total_samples") * 100).round(2).alias("Usage %"),
            (pl.col("Total_Sustains") / pl.col("Samples") * 100).round(2).alias("Sustain_Percentage"),
            (pl.col("Total_Full_Clears")/pl.col("Samples")* 100).round(2).alias("Full_Clear_Rate"),
            pl.col("Scores").list.eval(pl.element().quantile(0.25)).list.first().round(2).alias(f"25th Percentile {self.metric_name}"),
            pl.col("Scores").list.median().round(2).alias(f"Median {self.metric_name}"),
            pl.col("Scores").list.eval(pl.element().quantile(0.75)).list.first().round(2).alias(f"75th Percentile {self.metric_name}"),
            pl.col("Scores").list.eval(pl.element().std(ddof=1)).list.first().round(2).alias(f"Std Dev {self.metric_name}"),
            pl.col("Scores").list.min().alias(f"Min {self.metric_name}"),
            pl.col("Scores").list.mean().round(2).alias(f"Average {self.metric_name}"),
            pl.col("Scores").list.max().alias(f"Max {self.metric_name}")
        ]).sort(["version", self.node_or_floor_col, "Samples"], descending=[True, False, True])

        return df.with_row_index("Rank", offset=1).select([
            "Rank", "version", "estimated_min_cost", "estimated_max_cost", self.node_or_floor_col, "max_eidolon",
            "Archetype Core", "Usage %", "Samples", "Total_Full_Clears", "Full_Clear_Rate", pl.col("Total_Sustains").alias("Sustain_Samples"), "Sustain_Percentage",
            f"Min {self.metric_name}", f"25th Percentile {self.metric_name}", f"Median {self.metric_name}",
            f"75th Percentile {self.metric_name}", f"Average {self.metric_name}", f"Std Dev {self.metric_name}", f"Max {self.metric_name}"
        ])
        
        
    def get_chars_by_cost_df(self):
        df = self.chars_by_cost_individual_eidolon.join(
            self.total_samples_df, on=["version", self.node_or_floor_col], how="left"
        ).with_columns([
            (pl.col("Samples") / pl.col("version_total_samples") * 100).round(2).alias("Appearance Rate (%)"),
            
            (pl.col("Total_Sustains") / pl.col("Samples") * 100).round(2).alias("Sustain_Percentage"),
            (pl.col("Total_Full_Clears")/pl.col("Samples")* 100).round(2).alias("Full_Clear_Rate"),
            pl.col("Scores").list.eval(pl.element().quantile(0.25)).list.first().round(2).alias(f"25th Percentile {self.metric_name}"),
            pl.col("Scores").list.median().round(2).alias(f"Median {self.metric_name}"),
            pl.col("Scores").list.eval(pl.element().quantile(0.75)).list.first().round(2).alias(f"75th Percentile {self.metric_name}"),
            pl.col("Scores").list.eval(pl.element().std(ddof=1)).list.first().round(2).alias(f"Std Dev {self.metric_name}"),
            pl.col("Scores").list.min().alias(f"Min {self.metric_name}"),
            pl.col("Scores").list.mean().round(2).alias(f"Average {self.metric_name}"),
            pl.col("Scores").list.max().alias(f"Max {self.metric_name}")
        ]).sort(["version", self.node_or_floor_col, "Samples"], descending=[True, False, True])

        return df.with_row_index("Rank", offset=1).select([
            "Rank", "version", "estimated_min_cost", "estimated_max_cost","max_eidolon", self.node_or_floor_col,
            pl.col("char_cons_zipped_sorted").alias("Character"),
            "Appearance Rate (%)", "Samples", "Total_Full_Clears", "Full_Clear_Rate", pl.col("Total_Sustains").alias("Sustain_Samples"), "Sustain_Percentage",
            f"Min {self.metric_name}", f"25th Percentile {self.metric_name}", f"Median {self.metric_name}",
            f"75th Percentile {self.metric_name}", f"Average {self.metric_name}", f"Std Dev {self.metric_name}", f"Max {self.metric_name}"
        ])

    def get_chars_by_individual_eidolons_df(self):
        df = self.chars_by_individual_eidolons.join(
            self.total_samples_df, on=["version", self.node_or_floor_col], how="left"
        ).with_columns([
            (pl.col("Samples") / pl.col("version_total_samples") * 100).round(2).alias("Appearance Rate (%)"),
           
            (pl.col("Total_Sustains") / pl.col("Samples") * 100).round(2).alias("Sustain_Percentage"),
            (pl.col("Total_Full_Clears")/pl.col("Samples")* 100).round(2).alias("Full_Clear_Rate"),
            pl.col("Scores").list.eval(pl.element().quantile(0.25)).list.first().round(2).alias(f"25th Percentile {self.metric_name}"),
            pl.col("Scores").list.median().round(2).alias(f"Median {self.metric_name}"),
            pl.col("Scores").list.eval(pl.element().quantile(0.75)).list.first().round(2).alias(f"75th Percentile {self.metric_name}"),
            pl.col("Scores").list.eval(pl.element().std(ddof=1)).list.first().round(2).alias(f"Std Dev {self.metric_name}"),
            pl.col("Scores").list.min().alias(f"Min {self.metric_name}"),
            pl.col("Scores").list.mean().round(2).alias(f"Average {self.metric_name}"),
            pl.col("Scores").list.max().alias(f"Max {self.metric_name}")
        ]).sort(["version", self.node_or_floor_col, "Samples"], descending=[True, False, True])

        return df.with_row_index("Rank", offset=1).select([
            "Rank", "version", self.node_or_floor_col,
            pl.col("char_cons_zipped_sorted").alias("Character (Eidolon)"),
            "Appearance Rate (%)", "Samples", "Total_Full_Clears", "Full_Clear_Rate", pl.col("Total_Sustains").alias("Sustain_Samples"), "Sustain_Percentage",
            f"Min {self.metric_name}", f"25th Percentile {self.metric_name}", f"Median {self.metric_name}",
            f"75th Percentile {self.metric_name}", f"Average {self.metric_name}", f"Std Dev {self.metric_name}", f"Max {self.metric_name}"
        ])

    def get_duos_stats(self):
        char_freq = (
            self.chars_by_individual_eidolons.lazy()
            .join(self.total_samples_df.lazy(), on=["version", self.node_or_floor_col], how="left")
            .select([
                "version", self.node_or_floor_col, pl.col("char_cons_zipped_sorted").alias("Character"),
                (pl.col("Samples") / pl.col("version_total_samples")).alias("char_support")
            ])
        )

        rules = (
            self.duos.lazy()
            .join(char_freq, left_on=["version", self.node_or_floor_col, "Antecedent"], right_on=["version", self.node_or_floor_col, "Character"], how="left")
            .rename({"char_support": "support_A"})
            .join(char_freq, left_on=["version", self.node_or_floor_col, "Consequent"], right_on=["version", self.node_or_floor_col, "Character"], how="left")
            .rename({"char_support": "support_C"})
            .join(self.total_samples_df.lazy(), on=["version", self.node_or_floor_col], how="left")
        )

        return rules.with_columns([
            (pl.col("Samples") / pl.col("version_total_samples")).alias("support"),
        ]).with_columns([
            (pl.col("support") / pl.col("support_A")).alias("confidence"),
        ]).with_columns([
            (pl.col("confidence") / pl.col("support_C")).alias("lift"),
            (pl.col("support") - (pl.col("support_A") * pl.col("support_C"))).alias("leverage"),
            ((1 - pl.col("support_C")) / (1 - pl.col("confidence") + 1e-7)).alias("conviction"),
            (
                (pl.col("support") - pl.col("support_A") * pl.col("support_C")) /
                pl.max_horizontal(
                    pl.col("support") * (1 - pl.col("support_A")),
                    pl.col("support_A") * (pl.col("support_C") - pl.col("support")),
                    1e-7
                )
            ).alias("zhang"),
            (
                pl.when(pl.col("confidence") >= pl.col("support_C"))
                .then((pl.col("confidence") - pl.col("support_C")) / pl.max_horizontal(1 - pl.col("support_C"), 1e-7))
                .otherwise((pl.col("confidence") - pl.col("support_C")) / pl.max_horizontal(pl.col("support_C"), 1e-7))
            ).alias("certainty"),
            (
                pl.col("support") / (pl.col("support_A") + pl.col("support_C") - pl.col("support") + 1e-7)
            ).alias("jaccard"),
            (pl.col("Samples") / pl.col("version_total_samples") * 100).round(2).alias("Appearance Rate (%)"),
            pl.col("Total_Sustains"),
            (pl.col("Total_Sustains") / pl.col("Samples") * 100).round(2).alias("Sustain_Percentage"),
            pl.col("Total_Full_Clears"),
            (pl.col("Total_Full_Clears")/pl.col("Samples")* 100).round(2).alias("Full_Clear_Rate"),
            pl.col("Scores").list.eval(pl.element().quantile(0.25)).list.first().round(2).alias(f"25th Percentile {self.metric_name}"),
            pl.col("Scores").list.median().round(2).alias(f"Median {self.metric_name}"),
            pl.col("Scores").list.eval(pl.element().quantile(0.75)).list.first().round(2).alias(f"75th Percentile {self.metric_name}"),
            pl.col("Scores").list.eval(pl.element().std(ddof=1)).list.first().round(2).alias(f"Std Dev {self.metric_name}"),
            pl.col("Scores").list.min().alias(f"Min {self.metric_name}"),
            pl.col("Scores").list.mean().round(2).alias(f"Average {self.metric_name}"),
            pl.col("Scores").list.max().alias(f"Max {self.metric_name}")
        ]).select([
            "version", self.node_or_floor_col, "Antecedent", "Consequent", "Samples", "Appearance Rate (%)", 
            "Total_Full_Clears", "Full_Clear_Rate", "Total_Sustains", "Sustain_Percentage",
            pl.col("confidence").round(3).alias("Confidence"),
            pl.col("lift").round(3).alias("Lift"),
            pl.col("leverage").round(4).alias("Leverage"),
            pl.col("conviction").round(3).alias("Conviction"),
            pl.col("zhang").round(3).alias("Zhang"),
            pl.col("certainty").round(3).alias("Certainty"),
            pl.col("jaccard").round(3).alias("Jaccard"),
            f"Min {self.metric_name}", f"25th Percentile {self.metric_name}", f"Median {self.metric_name}",
            f"75th Percentile {self.metric_name}", f"Average {self.metric_name}", f"Std Dev {self.metric_name}", f"Max {self.metric_name}"
        ]).sort(["version", self.node_or_floor_col, "Lift"], descending=[True, False, True]).collect()

    def get_combined_team_df(self):
        node_char_cols = self.combined_team_stats.collect_schema().names()
        
        team_cols = [c for c in node_char_cols if c.endswith("_char_cons_zipped_sorted")]
        arch_cols = [c for c in node_char_cols if c.endswith("_archetypes_zipped_sorted")]
        sustain_cols = [c for c in node_char_cols if c.endswith("_has_sustain")]
     
        label_prefix = "Floor" if self.mode == "anomaly" else "Node"

        # 1. Build the dynamic columns based on mode
        if self.mode == "anomaly":
            # FIX: Explicitly build these names using the count of your team columns 
            # instead of searching inside node_char_cols
            full_clear_cols = [f"f{i+1}_is_full_clear" for i in range(len(team_cols))]
            full_clear_label_cols = [f"f{i+1}_Full_Clear_Rate" for i in range(len(team_cols))]
            
            mode_specific_expressions = [
                (pl.col(c) / pl.col("Samples") * 100).round(2).alias(full_clear_label_cols[i])
                for i, c in enumerate(full_clear_cols)
            ]
            mode_specific_select = [*full_clear_cols, *full_clear_label_cols]
        else:
            mode_specific_expressions = [
                pl.col("is_full_clear").alias("Total_Full_Clears"), 
                (pl.col("is_full_clear") / pl.col("Samples") * 100).round(2).alias("Full_Clear_Rate")
            ]
            mode_specific_select = ["Total_Full_Clears", "Full_Clear_Rate"]

        # 2. Build the baseline columns list
        columns_to_add = [
            *[
                pl.col(c).list.join(", ").map_elements(lambda s: f"({s})", return_dtype=pl.String).alias(f"Team {label_prefix} {c.split('_')[0][1:]}")
                for c in team_cols
            ],
            *[
                pl.col(c).list.join(" + ").alias(f"Archetype {label_prefix} {c.split('_')[0][1:]}").map_elements(lambda s: s if s != "" else "Other / No DPS", return_dtype=pl.String)
                for c in arch_cols
            ],
            *[
                # FIX: Convert the boolean check back to UInt32 (or Int32) so it displays perfectly
                (pl.col(c) == pl.col("Samples")).cast(pl.UInt32).alias(c)
                for c in sustain_cols
            ],
            *mode_specific_expressions,
            (pl.col("Samples") / pl.col("combined_version_total_samples") * 100).round(2).alias("Appearance Rate (%)"),
            pl.col("Scores").list.eval(pl.element().quantile(0.25)).list.first().round(2).alias(f"25th Percentile {self.metric_name}"),
            pl.col("Scores").list.median().round(2).alias(f"Median {self.metric_name}"),
            pl.col("Scores").list.eval(pl.element().quantile(0.75)).list.first().round(2).alias(f"75th Percentile {self.metric_name}"),
            pl.col("Scores").list.eval(pl.element().std(ddof=1)).list.first().round(2).alias(f"Std Dev {self.metric_name}"),
            pl.col("Scores").list.min().alias(f"Min {self.metric_name}"),
            pl.col("Scores").list.mean().round(2).alias(f"Average {self.metric_name}"),
            pl.col("Scores").list.max().alias(f"Max {self.metric_name}")
        ]

        df = (
            self.combined_team_stats.join(
                self.combined_total_samples_df, on=["version"], how="left" 
            )
            .with_columns(columns_to_add)
            .sort(["version", "Samples"], descending=[True, True])
        )

        display_pairs = []
        for c in team_cols:
            node_num = c.split('_')[0][1:]
            display_pairs.extend([f"Team {label_prefix} {node_num}", f"Archetype {label_prefix} {node_num}"])

        # 3. Assemble the final selective column list safely
        final_selection = [
            "Rank", "version", "combined_max_ed", "total_min_estimated_cost", "total_max_estimated_cost",
            *display_pairs, *sustain_cols,
            *mode_specific_select,
            "Appearance Rate (%)", "Samples",
            f"Min {self.metric_name}", f"25th Percentile {self.metric_name}", f"Median {self.metric_name}",
            f"75th Percentile {self.metric_name}", f"Average {self.metric_name}", f"Std Dev {self.metric_name}", f"Max {self.metric_name}"
        ]

        return df.with_row_index("Rank", offset=1).select(final_selection)

    def get_combined_archetype_df(self):
        list_cols = [c for c in self.combined_archetype_stats.columns if c.endswith("archetypes_zipped_sorted")]
        sustain_cols = [c for c in self.combined_archetype_stats.columns if c.endswith("_has_sustain")]
        label_prefix = "Floor" if self.mode == "anomaly" else "Node"
        
        

        # 1. Build the dynamic columns based on mode
        if self.mode == "anomaly":
            # Use list_cols to get the count safely instead of the missing node_char_cols variable
            full_clear_cols = [f"f{i+1}_is_full_clear" for i in range(len(list_cols))]
            full_clear_label_cols = [f"f{i+1}_Full_Clear_Rate" for i in range(len(list_cols))]
            mode_specific_expressions = [
                (pl.col(c) / pl.col("Samples") * 100).round(2).alias(full_clear_label_cols[i])
                for i, c in enumerate(full_clear_cols)
            ]
            mode_specific_select = [*full_clear_cols, *full_clear_label_cols]
        else:
            mode_specific_expressions = [
                pl.col("is_full_clear"), 
                (pl.col("is_full_clear") / pl.col("Samples") * 100).round(2).alias("Full_Clear_Rate")
            ]
            # Columns to select at the end for normal mode
            mode_specific_select = [pl.col("is_full_clear").alias("Total_Full_Clears"), "Full_Clear_Rate"]

        # 2. Build the columns to transform/calculate
        columns_to_add = [
            *[
                pl.col(c).list.join(" + ").alias(f"Archetype {label_prefix} {c.split('_')[0][1:]}").map_elements(lambda s: s if s != "" else "Other / No DPS", return_dtype=pl.String)
                for c in list_cols
            ],
            *[
                pl.col(c).alias(f"Sustain {label_prefix} {c.split('_')[0][1:]} Count")
                for c in sustain_cols
            ],
            *[
                (pl.col(c) / pl.col("Samples") * 100).round(2).alias(f"Sustain {label_prefix} {c.split('_')[0][1:]} (%)")
                for c in sustain_cols
            ],
            *mode_specific_expressions,
            (pl.col("Samples") / pl.col("combined_version_total_samples") * 100).round(2).alias("Appearance Rate (%)"),
            pl.col("Scores").list.eval(pl.element().quantile(0.25)).list.first().round(2).alias(f"25th Percentile {self.metric_name}"),
            pl.col("Scores").list.median().round(2).alias(f"Median {self.metric_name}"),
            pl.col("Scores").list.eval(pl.element().quantile(0.75)).list.first().round(2).alias(f"75th Percentile {self.metric_name}"),
            pl.col("Scores").list.eval(pl.element().std(ddof=1)).list.first().round(2).alias(f"Std Dev {self.metric_name}"),
            pl.col("Scores").list.min().alias(f"Min {self.metric_name}"),
            pl.col("Scores").list.mean().round(2).alias(f"Average {self.metric_name}"),
            pl.col("Scores").list.max().alias(f"Max {self.metric_name}")
        ]

        df = (
            self.combined_archetype_stats.join(
                self.combined_total_samples_df, on=["version"], how="left"
            )
            .with_columns(columns_to_add)
            .sort(["version", "Samples"], descending=[True, True])
        )

        arch_aliases = [f"Archetype {label_prefix} {c.split('_')[0][1:]}" for c in list_cols]
        sustain_count_cols = [f"Sustain {label_prefix} {c.split('_')[0][1:]} Count" for c in sustain_cols]
        sustain_perc_cols = [f"Sustain {label_prefix} {c.split('_')[0][1:]} (%)" for c in sustain_cols]

        # Interleave counts and percentages for each node's sustain data
        sustain_display = []
        for count, perc in zip(sustain_count_cols, sustain_perc_cols):
            sustain_display.extend([count, perc])

        # 3. Assemble final selective schema based on mode
        final_selection = [
            "Rank", "version", "combined_max_ed", "total_min_estimated_cost", "total_max_estimated_cost",
            *arch_aliases, "Appearance Rate (%)", "Samples", 
            *mode_specific_select, 
            *sustain_display,
            f"Min {self.metric_name}", f"25th Percentile {self.metric_name}", f"Median {self.metric_name}",
            f"75th Percentile {self.metric_name}", f"Average {self.metric_name}", f"Std Dev {self.metric_name}", f"Max {self.metric_name}"
        ]

        return df.with_row_index("Rank", offset=1).select(final_selection)



    