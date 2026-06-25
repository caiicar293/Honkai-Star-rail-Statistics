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


class HonkaiStatistics_V2_APOC_Batch:
    def __init__(self, version, floor, node=0, by_ed=None, by_ed_start=0, by_ed_end=6, by_Scores=0, by_ed_inclusive=False,
                 by_ed_inclusive_combined=False, by_char=None, by_Scores_combined=0,
                 not_char=False, sustain_condition=None, star_num=None, is_starward=True):

        self.version, self.floor, self.node, self.star_num, self.is_starward = version, floor, node, star_num, is_starward
        self.by_ed_start, self.by_ed_end, self.by_ed = by_ed_start, by_ed_end, by_ed
        self.by_Scores = by_Scores
        self.by_ed_inclusive = by_ed_inclusive
        self.by_ed_inclusive_combined = by_ed_inclusive_combined
        self.by_Scores_combined = by_Scores_combined
        self.key = "APOC_VERSIONS"
        self.folder = "raw_data"

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
        path = os.path.join(self.folder, f"{version}_as.parquet")
        path2 = os.path.join(self.folder, f"{version}_char.parquet")

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

        # ------------------------------------------------------------------
        # Apply is_starward filter early — no scattered if-checks downstream
        # ------------------------------------------------------------------
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
            pl.when(pl.col("is_starward").is_not_null())
        .then(pl.lit(4))
        .otherwise(pl.lit(3))
        .alias("row_max_stars")

        ])
   

        # Use pl.struct inside a list concatenation to map them element-wise
        df_struct = lf.with_columns(
            char_cons_pairs = pl.concat_list([
                pl.struct([
                    pl.col("temp_team").list.get(i).alias("char"),
                    pl.col("temp_cons").list.get(i).alias("eidolon")
                ])
                for i in range(4) # Number of elements in your sublists
            ])
        )

        

        
        
        # 1. Create and sort the pairs
        df_struct = df_struct.with_columns(
            pl.col("char_cons_pairs").list.eval(
                pl.element().sort_by(pl.element().struct.field('char').replace_strict(char_to_index, default=999))
            ).alias("char_cons_pairs_sorted")
        )
        
        # 2. Extract archetypes using the fully created sorted column
        #    (Notice the added .struct.field('char') in the filter!)
        df_struct = df_struct.with_columns(
            pl.col("char_cons_pairs_sorted").list.eval(
                pl.element().filter(
                    pl.element().struct.field('char').is_in(dps_names) & 
                    pl.element().struct.field('char').is_not_null()
                )
            ).alias("archetypes_pairs_sorted")
        )
        
        # 2. Second pass: Extract the team_key from the newly created column
        df_struct = df_struct.with_columns([
            pl.col("char_cons_pairs_sorted").list.eval(
                pl.element().struct.field('char')
            ).alias("team_key"),
            pl.col("archetypes_pairs_sorted").list.eval(
                pl.element().struct.field('char')
            ).alias("archetype_key") ]
        )
        
        df_struct = df_struct.with_columns([
            pl.col("char_cons_pairs_sorted").list.eval(
                pl.element().struct.field('char').cast(pl.String) +
                "(E" + pl.element().struct.field('eidolon').cast(pl.Int64).cast(pl.String)  + ")"
        ).alias("char_cons_zipped_sorted"),
            pl.col("archetypes_pairs_sorted").list.eval(
                pl.element().struct.field('char').cast(pl.String) +
                "(E" + pl.element().struct.field('eidolon').cast(pl.Int64).cast(pl.String)  + ")"
        ).alias("archetypes_pairs_zipped_sorted")])
        
        
         

        self.lf = df_struct
        
        
        if self.node == 0 or self.node == "all":
            base_cols_for_n = ["uid", "version", "node", "round_num", "max_eidolon","char_cons_zipped_sorted","archetypes_pairs_zipped_sorted","estimated_min_cost","estimated_max_cost","has_sustain","star_num","row_max_stars","team_key",'archetype_key'] 
            lf_base_for_combined = self.lf.select(base_cols_for_n)

            def _node_lf(node_num, scores_alias, ed_alias,min_cost_alias,max_cost_alias):
                return lf_base_for_combined.filter(pl.col("node") == node_num).select([
                    pl.col("char_cons_zipped_sorted").alias(f"n{node_num}_char_cons_zipped_sorted"),
                    pl.col("archetypes_pairs_zipped_sorted").alias(f"n{node_num}_archetypes_zipped_sorted"),
                    pl.col("team_key").alias(f"n{node_num}_team_key"),
                    pl.col("archetype_key").alias(f"n{node_num}_archetype_key"),
                    pl.col("uid"), pl.col("version"),
                    pl.col("round_num").alias(scores_alias),
                    pl.col("max_eidolon").alias(ed_alias),
                    pl.col("star_num"),
                    pl.col("row_max_stars"), 
                    pl.col("has_sustain").alias(f"n{node_num}_has_sustain"),
                    pl.col("estimated_min_cost").alias(min_cost_alias),
                    pl.col("estimated_max_cost").alias(max_cost_alias)
                ])

            n1 = _node_lf(1, "n1_Scores", "n1_max_ed", 'n1_min_estimated_cost','n1_max_estimated_cost')
            n2 = _node_lf(2, "n2_Scores", "n2_max_ed", 'n2_min_estimated_cost','n2_max_estimated_cost')

            join_keys = ["uid", "version", "star_num","row_max_stars"]
            combined = n1.join(n2, on=join_keys, how="inner")

            # FIXED: Change how="inner" to how="left" so older versions lacking Node 3 data are preserved.
            # Fixed addition: Use pl.sum_horizontal and pl.max_horizontal to smoothly handle nulls from older versions.
            if self._has_starward_col and self.is_starward:
                n3 = _node_lf(3, "n3_Scores", "n3_max_ed",'n3_min_estimated_cost','n3_max_estimated_cost')
                combined = combined.join(n3, on=join_keys, how="left")
                combined = combined.with_columns([
                    pl.sum_horizontal(["n1_Scores", "n2_Scores", "n3_Scores"]).alias("total_Scores"),
                    pl.sum_horizontal(["n1_min_estimated_cost", "n2_min_estimated_cost", "n3_min_estimated_cost"]).alias("total_min_estimated_cost"),
                    pl.sum_horizontal(["n1_max_estimated_cost", "n2_max_estimated_cost", "n3_max_estimated_cost"]).alias("total_max_estimated_cost"),
                    pl.max_horizontal(["n1_max_ed", "n2_max_ed", "n3_max_ed"]).alias("combined_max_ed")
                ])
            else:
                # Pre-Starward files, or is_starward=False players in a Starward-era file
                combined = combined.with_columns([
                    (pl.col("n1_Scores") + pl.col("n2_Scores")).alias("total_Scores"),
                    pl.max_horizontal(["n1_max_ed", "n2_max_ed"]).alias("combined_max_ed"),
                    (pl.col("n1_min_estimated_cost") + pl.col("n2_min_estimated_cost")).alias("total_min_estimated_cost"),
                    (pl.col("n1_max_estimated_cost") + pl.col("n2_max_estimated_cost")).alias("total_max_estimated_cost"),
                ])

           

            self.combined = combined.filter(pl.col("total_Scores") >= self.by_Scores_combined)
            self._process_combined_data(self.combined)

            # Collapse standard nodes into node=0 for the per-node aggregation.
            # Same condition: only include node 3 in the collapse when it actually exists for this query.
            standard_nodes = [1, 2, 3] if (self._has_starward_col and self.is_starward) else [1, 2]
            df = self.lf.with_columns(
                pl.when(pl.col("node").is_in(standard_nodes))
                .then(0)
                .otherwise(pl.col("node"))
                .alias('node')
            )
            if self.node == "all":
                self.lf = pl.concat([df, self.lf], how="diagonal")
            else:
                self.lf = df

        self._process_data(self.lf)
        
    def _process_data(self,lf):
            base_data = (
                self.lf.select([
                    "uid", "round_num", "has_sustain", "version",
                    "estimated_min_cost", "estimated_max_cost", "node","max_eidolon",
                    'team_key',"row_max_stars","star_num",
                    
                ])
            ).explode("team_key")
            
            base_data2 = (
                self.lf.select([
                    "uid", "round_num", "has_sustain", "version",
                    "estimated_min_cost", "estimated_max_cost", "node","max_eidolon",
                    "row_max_stars","star_num",'char_cons_zipped_sorted',
                    
                ])
            ).explode("char_cons_zipped_sorted")
            
            self.chars_max_eid = base_data.group_by(["version", "estimated_min_cost", "estimated_max_cost", "node", "team_key","max_eidolon"]).agg([
                pl.col("uid").count().alias("Samples"),
                pl.col("round_num").alias("Scores"),
                pl.col("uid").unique().alias("uids"),
                pl.col("has_sustain").sum().alias("Total_Sustains"),
                (pl.col("star_num") == pl.col("row_max_stars")).sum().alias("total_full_star_clears")
            ]).collect()
            
            self.chars_eidolon_char= base_data2.group_by(["version", "estimated_min_cost", "estimated_max_cost", "node", "char_cons_zipped_sorted"]).agg([
                pl.col("uid").count().alias("Samples"),
                pl.col("round_num").alias("Scores"),
                pl.col("uid").unique().alias("uids"),
                pl.col("has_sustain").sum().alias("Total_Sustains"),
                (pl.col("star_num") == pl.col("row_max_stars")).sum().alias("total_full_star_clears")
            ]).collect()
            
            
            self.chars= self.chars_max_eid.group_by(["version", "estimated_min_cost", "estimated_max_cost", "node", "team_key"]).agg([
                pl.col('Samples').sum(),
                pl.col("Scores").explode(),
                pl.col("uids").explode(),
                pl.col("Total_Sustains").sum(),
                pl.col("total_full_star_clears").sum()
            ])

            
            self.teams = self.lf.group_by(["version", "estimated_min_cost", "estimated_max_cost", "node", "char_cons_zipped_sorted","team_key", "archetypes_pairs_zipped_sorted","archetype_key","max_eidolon","has_sustain"]).agg([
    
                pl.count("uid").alias("Samples"),
                pl.col("round_num").alias("Scores"),
                pl.col("uid").unique().alias("uids"),
                (pl.col("star_num") == pl.col("row_max_stars")).sum().alias("total_full_star_clears")
            ]).collect()
        
           
            self.archetypes = self.teams.group_by(["version", "estimated_min_cost", "estimated_max_cost", "node", "archetypes_pairs_zipped_sorted","archetype_key","max_eidolon"]).agg([
    
                pl.sum("Samples"),
                pl.col("Scores").explode(),
                pl.col("uids").explode().unique(),
                pl.col("total_full_star_clears").sum(),
                (pl.col("has_sustain") * pl.col('Samples')).sum()
            ])

            
            new = self.teams.select("version","node",'char_cons_zipped_sorted',"Samples","Scores","total_full_star_clears","has_sustain")
            new = new.with_columns(pl.col('char_cons_zipped_sorted').alias("Consequent"))
            result = new.explode('char_cons_zipped_sorted').explode('Consequent')
            result = result.filter(pl.col("char_cons_zipped_sorted") != pl.col("Consequent"))

            self.duos = result.group_by(
                ["version", "node", pl.col('char_cons_zipped_sorted').alias('Antecedent'), "Consequent"]
            ).agg([
                pl.sum("Samples"),
                pl.col("Scores").list.explode().alias("Scores"),
                pl.col("total_full_star_clears").sum(),
                (pl.col("has_sustain") * pl.col('Samples')).sum()
            ])
        
        
        
            self.total_samples_df = self.lf.group_by(
            "version", "node"
            ).agg(pl.col("uid").n_unique().alias("version_total_samples")).collect() 
            
            
    def _process_combined_data(self, combined):
        # Determine which node char columns exist in the combined frame
        # Starward: n1_chars, n2_chars, n3_chars  |  Standard: n1_chars, n2_chars
        node_char_cols = [c for c in combined.collect_schema().names() if c.endswith("_zipped_sorted") or c.endswith("_has_sustain") or c.endswith("_key")]
      
        self.combined_team_stats = (
            combined.group_by(["version", "combined_max_ed","total_min_estimated_cost","total_max_estimated_cost"] + node_char_cols)
            .agg([
                pl.count("uid").alias("Samples"),
                pl.col("total_Scores").alias("Scores"),
                pl.col("uid").unique().alias("uids"),
                (pl.col("star_num") == pl.col("row_max_stars")).sum().alias("total_full_star_clears")
            ])
            .collect()
        )
        
        # 1. Explicitly list the columns you are aggregating
        agg_cols = ["Samples", "Scores", "uids", "total_full_star_clears"]

        # 2. Build the grouping keys by excluding both the unwanted strings AND the agg columns
        group_keys = [
            c for c in self.combined_team_stats.collect_schema().names() 
            if not c.endswith(("_char_cons_zipped_sorted", "_team_key","_has_sustain")) 
            and c not in agg_cols
        ]
        

        # 3. Perform the group_by and the aggregations safely
        self.combined_archetype_stats = (
            self.combined_team_stats.group_by(group_keys)
            .agg([
                pl.col("Samples").sum(),        # Sum the previous counts
                pl.col("Scores").explode() ,          # Average the scores
                pl.col("uids").explode().unique(),  # Flatten the UIDs and get unique
                pl.col("total_full_star_clears").sum().alias("total_full_star_clears"),
                (cs.ends_with("_has_sustain") * pl.col("Samples")).sum()
            ])
        )


        self.combined_total_samples_df = combined.group_by(
            "version", 
        ).agg(pl.col("uid").n_unique().alias("combined_version_total_samples")).collect()

        # Store node_char_cols for use in get_combined_* methods
        self._node_char_cols = node_char_cols