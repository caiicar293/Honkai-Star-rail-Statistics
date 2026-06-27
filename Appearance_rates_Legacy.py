"""
Appearance_rates_Legacy.py
==========================
Unified legacy statistics handler for Honkai Star Rail parquet files up to
version 2.2.2, supporting both Memory of Chaos (MoC) and Pure Fiction (PF).

Key properties
--------------
- No cons1/cons2/cons3/cons4 columns in the main parquet (pre-2.2.2).
- Eidolons come exclusively from the _char parquet and are surfaced only in
  display_top_gear / display_single_char_full / get_gear_usage_df.
- get_char_df has no per-eidolon breakdown.
- Floor labels stored as strings (e.g. versions <= 1.4.x) are auto-detected
  and converted to integers with a native Polars expression — no map_elements.
- Whole-row duplicate removal on load (needed for versions <= 1.4.1).
- v1.0.3 edge case: artifacts / relics / phase may be absent from the char
  file entirely; handled gracefully via schema inspection.
- score_col / score direction are set automatically by mode:
    mode="moc"  ->  score_col="round_num", filter  <= by_cycle   (lower = better)
    mode="pf"   ->  score_col="round_num", filter  >= by_points  (higher = better)
  Pass score_col explicitly to override.
"""

import polars as pl
import os
import orjson
import matplotlib.pyplot as plt
import polars.selectors as cs

pl.Config.set_tbl_rows(100)
pl.Config.set_tbl_cols(-1)
pl.Config.set_fmt_str_lengths(100)


class HonkaiStatistics_Legacy:
    """
    Parameters
    ----------
    version : str
        Dataset version string, e.g. "2.1.2".
    floor : int | str
        Floor / stage value to filter on.
    node : int
        0 = both nodes combined, 1 = node 1 only, 2 = node 2 only.
    mode : {"moc", "pf"}
        "moc" — Memory of Chaos. Loads {version}.parquet.
                Score filter: round_num <= by_cycle.
        "pf"  — Pure Fiction.   Loads {version}_pf.parquet.
                Score filter: round_num >= by_points.
    score_col : str | None
        Override the score column name. If None, defaults to "round_num".
    by_cycle : int
        MoC: rows with score > this are excluded (lower cycles = better).
    by_points : int | float
        PF: rows with score < this are excluded (higher points = better).
    by_cycle_combined : int
        MoC combined filter: total score across both nodes <= this.
    by_points_combined : int | float
        PF combined filter: total score across both nodes >= this.
    sustain_condition : ignored
        Reserved for API compatibility.
    """

    def __init__(
        self,
        version: str,
        floor,
        node: int = 0,
        mode: str = "moc",
        score_col: str | None = None,
        by_cycle: int = 10_000,
        by_points: int | float = 0,
        by_cycle_combined: int = 10_000,
        by_points_combined: int | float = 0,
        sustain_condition=None,
    ):
        self.version = version
        self.node    = node
        self.mode    = mode.lower()

        if self.mode not in ("moc", "pf"):
            raise ValueError(f"mode must be 'moc' or 'pf', got {mode!r}")

        self.score_col = score_col if score_col else "round_num"
        self.by_cycle            = by_cycle
        self.by_points           = by_points
        self.by_cycle_combined   = by_cycle_combined
        self.by_points_combined  = by_points_combined

        # ------------------------------------------------------------------
        # 1. LOAD + DEDUPLICATE
        # ------------------------------------------------------------------
        folder = "raw_data"
        suffix = "_pf" if self.mode == "pf" else ""
        path   = os.path.join(folder, f"{version}{suffix}.parquet")
        path2  = os.path.join(folder, f"{version}_char.parquet")

        if os.path.exists(path):
            self.df = pl.read_parquet(path)
        else:
            fname = f"{version}{suffix}.csv"
            url   = f"https://huggingface.co/datasets/LvlUrArti/MocData/resolve/main/{fname}"
            self.df = pl.read_csv(url)
            os.makedirs(folder, exist_ok=True)
            self.df.write_parquet(path)

        # Drop fully-duplicate rows (affects versions <= 1.4.1)
        self.df = self.df.unique()

        if os.path.exists(path2):
            self.char_df = pl.read_parquet(path2)
        else:
            url = f"https://huggingface.co/datasets/LvlUrArti/MocData/resolve/main/{version}_char.csv"
            self.char_df = pl.read_csv(url)
            os.makedirs(folder, exist_ok=True)
            self.char_df.write_parquet(path2)

        # ------------------------------------------------------------------
        # 2. CHARACTER METADATA
        # ------------------------------------------------------------------
        with open("characters.json", "rb") as f:
            info = orjson.loads(f.read())

        rol_df = pl.DataFrame([
            {
                "char":       k,
                "is_sustain": "sustain" in v.get("role", []),
                "is_dps":     bool(set(v.get("role", [])).intersection({"dps", "specialist"})),
                "sort_index": i,
            }
            for i, (k, v) in enumerate(info.items())
        ])

        sustain_names  = rol_df.filter(pl.col("is_sustain")).get_column("char").to_list()
        dps_names      = rol_df.filter(pl.col("is_dps")).get_column("char").to_list()
        char_to_index  = dict(zip(rol_df["char"], rol_df["sort_index"]))

        # ------------------------------------------------------------------
        # 3. SCHEMA INSPECTION — eager, once, no LazyFrame warnings
        # ------------------------------------------------------------------
        char_cols      = ["ch1", "ch2", "ch3", "ch4"]
        char_file_cols = self.char_df.columns   # plain list

        # ------------------------------------------------------------------
        # 4. FLOOR NORMALISATION + FILTERING
        # ------------------------------------------------------------------
         # Convert main DF to Lazy for optimization pipeline
        corrupt_bullets = ["â€¢", "Ã¢â‚¬Â¢"]
        clean_bullets = ["•", "•"]
        lf      = self.df.lazy().with_columns([
            cs.string()
            .str.replace_many(corrupt_bullets, clean_bullets)
            .str.replace_all(r"\band\b", "&")
            .str.replace_all(r"^March 7th$", "Ice March 7th"),  # Strict full-string match
        ])
        char_lf = self.char_df.lazy().with_columns([
            cs.string()
            .str.replace_many(corrupt_bullets, clean_bullets)
            .str.replace_all(r"\band\b", "&")
            .str.replace_all(r"^March 7th$", "Ice March 7th"),  # Strict full-string match
        ])

        # Auto-detect string floors (e.g. "Ethereal Shipcraft Stage 6")
        if lf.collect_schema()["floor"] == pl.String:
            lf = lf.with_columns(
                pl.col("floor")
                .str.replace_all(r"<[^>]+>", "")   # strip XML tags
                .str.extract(r".*?(\d+)\D*$", group_index=1)
                .cast(pl.Int64)
                .alias("floor")
            )

        self.floor = floor

        if self.node != 0:
            lf = lf.filter(
                (pl.col("floor") == self.floor) & (pl.col("node") == self.node)
            )
        else:
            lf = lf.filter(pl.col("floor") == self.floor)

        # ------------------------------------------------------------------
        # 5. SUSTAIN FLAG
        # ------------------------------------------------------------------
        lf = lf.with_columns(
            pl.any_horizontal(
                [pl.col(c).is_in(sustain_names) for c in char_cols]
            ).alias("has_sustain")
        )

        # ------------------------------------------------------------------
        # 6. SCORE FILTER  (direction depends on mode)
        # ------------------------------------------------------------------
        if self.mode == "moc":
            self.lf = lf.filter(pl.col(self.score_col) <= self.by_cycle)
            self.metric = "Cycles"
        else:   # pf
            self.lf = lf.filter(pl.col(self.score_col) >= self.by_points)
            self.metric = "Points"

        # ------------------------------------------------------------------
        # 7. SINGLE-NODE PROCESSING
        # ------------------------------------------------------------------
        self._process_data(self.lf, char_lf, char_cols, char_file_cols,
                           dps_names, char_to_index)

        # ------------------------------------------------------------------
        # 8. COMBINED (both nodes) — only when node == 0
        # ------------------------------------------------------------------
        if self.node == 0:
            base_cols = ["uid", "node", self.score_col] + char_cols
            lf_base   = lf.select(base_cols)

            n1 = lf_base.filter(pl.col("node") == 1).select([
                pl.col("uid"),
                pl.concat_list(char_cols)
                  .list.eval(pl.element().sort_by(
                      pl.element().replace_strict(char_to_index, default=999)
                  ))
                  .alias("n1_chars"),
                pl.col(self.score_col).alias("n1_score"),
            ])
            n2 = lf_base.filter(pl.col("node") == 2).select([
                pl.col("uid"),
                pl.concat_list(char_cols)
                  .list.eval(pl.element().sort_by(
                      pl.element().replace_strict(char_to_index, default=999)
                  ))
                  .alias("n2_chars"),
                pl.col(self.score_col).alias("n2_score"),
            ])

            combined = (
                n1.join(n2, on="uid", how="inner")
                  .with_columns(
                      ((pl.col("n1_score") + pl.col("n2_score")) if mode == "pf" else pl.col("n1_score")).alias("total_score")
                  )
            )

            if self.mode == "moc":
                self.combined = combined.filter(
                    pl.col("total_score") <= self.by_cycle_combined
                )
            else:
                self.combined = combined.filter(
                    pl.col("total_score") >= self.by_points_combined
                )

            self._process_combined_data(self.combined, dps_names, char_to_index)

    # -----------------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------------

    @staticmethod
    def _safe_drop(lf: pl.LazyFrame, candidates: list[str],
                   schema_names: list[str]) -> pl.LazyFrame:
        to_drop = [c for c in candidates if c in schema_names]
        return lf.drop(to_drop) if to_drop else lf

    def _build_gear_cols(self, base_data: pl.LazyFrame,
                         present_gear: list[str],
                         group_keys: list[str]) -> list[pl.LazyFrame]:
        """
        Build per-gear rollup LazyFrames for whichever of
        weapon / artifacts / relics actually exist in base_data.
        Returns a list of (LazyFrame, alias) tuples in a consistent order.
        """
        gear_map = [
            ("weapon",    "Lightcones"),
            ("artifacts", "Relics"),
            ("relics",    "Planar_Set"),
        ]
        rollups = []
        for col, alias in gear_map:
            if col in present_gear:
                rollups.append(
                    base_data.group_by(group_keys + [col])
                    .agg([
                        pl.count("uid").alias("count"),
                        pl.col(self.score_col).alias("scores"),
                    ])
                    .group_by(group_keys)
                    .agg(
                        pl.struct([
                            pl.col(col).alias("name"),
                            "count",
                            "scores",
                        ]).alias(alias)
                    )
                )
            else:
                # Placeholder so joins don't silently drop characters
                rollups.append(None)
        return rollups  # [Lightcones_lf_or_None, Relics_lf_or_None, Planar_Set_lf_or_None]

    def _process_data(self, lf, char_lf, char_cols, char_file_cols,
                      dps_names, char_to_index):

        # ------------------------------------------------------------------
        # Unpivot + join char file
        # ------------------------------------------------------------------
        chars_long = (
            lf.unpivot(
                index=["uid", self.score_col, "has_sustain"],
                on=char_cols,
                value_name="Character",
            )
            .drop("variable")
            .with_columns(pl.col("Character").fill_null("Empty Slot"))
        )

        joined        = chars_long.join(char_lf, left_on=["uid", "Character"],
                                        right_on=["uid", "name"], how="left")
        joined_schema = joined.collect_schema().names()

        # Gear columns that actually exist in this version's char file
        gear_candidates = ["weapon", "artifacts", "relics"]
        present_gear    = [c for c in gear_candidates if c in joined_schema]
        missing_gear    = [c for c in gear_candidates if c not in joined_schema]

        # Fill present gear nulls; add literal placeholder for absent ones
        fill_exprs = [pl.col(c).fill_null("Info_not_found") for c in present_gear]
        fill_exprs += [pl.lit("Info_not_found").alias(c) for c in missing_gear]

        base_data = joined.with_columns(fill_exprs)
        base_data = self._safe_drop(base_data, ["phase", "cons", "level"], joined_schema)

        # Re-resolve schema after drops so gear rollup sees correct names
        base_schema = base_data.collect_schema().names()
        actual_gear = [c for c in gear_candidates if c in base_schema]

        # ------------------------------------------------------------------
        # Gear rollup (only for columns that actually exist)
        # ------------------------------------------------------------------
        group_keys = ["Character"]
        rollups    = self._build_gear_cols(base_data, actual_gear, group_keys)
        aliases    = ["Lightcones", "Relics", "Planar_Set"]

        agg_base = (
            base_data.group_by(group_keys)
            .agg([
                pl.count("uid").alias("Total_Samples"),
                pl.col(self.score_col).alias("Total_Scores"),
                pl.col("has_sustain").sum().alias("Total_Sustains"),
                pl.col("uid").unique().alias("uids"),
            ])
        )
        for lf_rollup, alias in zip(rollups, aliases):
            if lf_rollup is not None:
                agg_base = agg_base.join(lf_rollup, on=group_keys, how="left")

        self.char_stats = agg_base.collect()

        # Gear columns actually present in char_stats for downstream use
        self._gear_aliases = [a for a, r in zip(aliases, rollups) if r is not None]

        # Build _charstats with eidolons for gear-display functions
        self._build_charstats_with_eidolons(lf, char_lf, char_cols,
                                            char_file_cols, actual_gear)

        # ------------------------------------------------------------------
        # Team stats
        # ------------------------------------------------------------------
        self.team_stats = (
            lf.with_columns(
                pl.concat_list(char_cols)
                  .list.eval(pl.element().sort_by(
                      pl.element().replace_strict(char_to_index, default=999)
                  ))
                  .alias("team_key")
            )
            .group_by("team_key")
            .agg([
                pl.count("uid").alias("Samples"),
                pl.col(self.score_col).alias("Scores"),
                pl.col("uid").unique().alias("uids"),
                pl.col("has_sustain").sum().alias("Total_Sustains"),
            ])
            .collect()
        )

        # ------------------------------------------------------------------
        # Archetype stats
        # ------------------------------------------------------------------
        self.archetypes_stats = (
            self.team_stats.with_columns(
                pl.col("team_key")
                  .list.eval(
                      pl.element().filter(
                          pl.element().is_in(dps_names) & pl.element().is_not_null()
                      )
                  )
                  .alias("archetype_key")
            )
            .group_by("archetype_key")
            .agg([
                pl.col("Samples").sum(),
                pl.col("Scores").list.explode().alias("Scores"),
                pl.col("uids").list.explode().unique().alias("uids"),
                pl.col("Total_Sustains").sum(),
            ])
        )

        # ------------------------------------------------------------------
        # Duos
        # ------------------------------------------------------------------
        exploded = (
            self.team_stats
            .with_columns(pl.col("team_key").alias("Consequent"))
            .explode("team_key")
            .explode("Consequent")
            .filter(pl.col("team_key") != pl.col("Consequent"))
        )
        self.duos = exploded.group_by(
            pl.col("team_key").alias("Antecedent"), "Consequent"
        ).agg([
            pl.col("Samples").sum(),
            pl.col("Scores").list.explode().alias("Scores"),
            pl.col("uids").list.explode().unique().alias("uids"),
            pl.col("Total_Sustains").sum(),
        ])

        self.total_samples = lf.select(pl.col("uid").n_unique()).collect().item()

    def _build_charstats_with_eidolons(self, lf, char_lf, char_cols,
                                        char_file_cols: list[str],
                                        actual_gear: list[str]):
        """
        Build self._charstats: char_stats with per-eidolon gear data from the
        _char parquet, used only by display_top_gear.
        Handles missing gear columns (e.g. v1.0.3 has no artifacts/relics).
        """
        has_cons = "cons" in char_file_cols

        chars_long = (
            lf.unpivot(
                index=["uid", self.score_col, "has_sustain"],
                on=char_cols,
                value_name="Character",
            )
            .drop("variable")
            .with_columns(pl.col("Character").fill_null("Empty Slot"))
        )

        joined        = chars_long.join(char_lf, left_on=["uid", "Character"],
                                        right_on=["uid", "name"], how="left")
        joined_schema = joined.collect_schema().names()

        gear_candidates = ["weapon", "artifacts", "relics"]
        present_gear    = [c for c in gear_candidates if c in joined_schema]
        missing_gear    = [c for c in gear_candidates if c not in joined_schema]

        fill_exprs = [pl.col(c).fill_null("Info_not_found") for c in present_gear]
        fill_exprs += [pl.lit("Info_not_found").alias(c) for c in missing_gear]
        fill_exprs.append(
            (pl.col("cons").fill_null(0) if has_cons else pl.lit(0)).alias("eidolon")
        )

        enriched = joined.with_columns(fill_exprs)
        enriched = self._safe_drop(enriched, ["phase", "cons", "level"], joined_schema)

        enriched_schema = enriched.collect_schema().names()
        actual_gear_ed  = [c for c in gear_candidates if c in enriched_schema]

        group_keys_ed = ["Character", "eidolon"]
        rollups_ed    = self._build_gear_cols(enriched, actual_gear_ed, group_keys_ed)
        aliases       = ["Lightcones", "Relics", "Planar_Set"]

        agg_ed = (
            enriched.group_by(group_keys_ed)
            .agg([
                pl.count("uid").alias("Samples"),
                pl.col(self.score_col).alias("Scores"),
                pl.col("has_sustain").sum().alias("Sustains"),
                pl.col("uid").unique().alias("uids"),
            ])
        )
        for lf_rollup, alias in zip(rollups_ed, aliases):
            if lf_rollup is not None:
                agg_ed = agg_ed.join(lf_rollup, on=group_keys_ed, how="left")

        per_eidolon = (
            agg_ed
            .with_columns(
                ("Eidolon " + pl.col("eidolon").cast(pl.String)).alias("Eidolon_Level")
            )
            .collect()
        )

        pivot_values = (
            ["Samples", "Scores", "Sustains"]
            + [a for a, r in zip(aliases, rollups_ed) if r is not None]
        )

        pivoted = per_eidolon.pivot(
            on="Eidolon_Level",
            index="Character",
            values=pivot_values,
            aggregate_function="first",
        )

        self._charstats = self.char_stats.join(pivoted, on="Character", how="left")

    def _process_combined_data(self, combined, dps_names, char_to_index):

        self.combined_team_stats = (
            combined.group_by(["n1_chars", "n2_chars"])
            .agg([
                pl.count("uid").alias("Samples"),
                pl.col("total_score").alias("Scores"),
                pl.col("uid").unique().alias("uids"),
            ])
            .collect()
        )

        self.combined_archetypes_stats = (
            self.combined_team_stats.with_columns([
                pl.col("n1_chars")
                  .list.eval(pl.element().filter(
                      pl.element().is_in(dps_names) & pl.element().is_not_null()
                  )).alias("n1_archetype"),
                pl.col("n2_chars")
                  .list.eval(pl.element().filter(
                      pl.element().is_in(dps_names) & pl.element().is_not_null()
                  )).alias("n2_archetype"),
            ])
            .group_by(["n1_archetype", "n2_archetype"])
            .agg([
                pl.col("Samples").sum(),
                pl.col("Scores").list.explode().alias("Scores"),
                pl.col("uids").list.explode().unique().alias("uids"),
            ])
        )

        self.combined_char_stats = (
            combined.select(["uid", "total_score", "n1_chars", "n2_chars"])
            .explode("n1_chars")
            .explode("n2_chars")
            .group_by(["n1_chars", "n2_chars"])
            .agg([
                pl.count("uid").alias("Samples"),
                pl.col("total_score").alias("Scores"),
            ])
            .collect()
        )

    # -----------------------------------------------------------------------
    # Public display methods
    # -----------------------------------------------------------------------

    def _score_stats_exprs(self, col: str) -> list[pl.Expr]:
        """Standard set of score-distribution expressions for a list column."""
        return [
            pl.col(col).list.min().alias("Min"),
            pl.col(col).list.eval(pl.element().quantile(0.25)).list.first().round(2).alias("25th Percentile"),
            pl.col(col).list.median().round(2).alias("Median"),
            pl.col(col).list.eval(pl.element().quantile(0.75)).list.first().round(2).alias("75th Percentile"),
            pl.col(col).list.mean().round(2).alias(f"Average {self.metric}"),
            pl.col(col).list.eval(pl.element().std()).list.first().round(2).alias("Std Dev"),
            pl.col(col).list.max().alias("Max"),
        ]

    def get_char_df(self):
        df = self.char_stats.with_columns([
            (pl.col("Total_Samples") / self.total_samples * 100).round(3).alias("Appearance Rate (%)"),
            (pl.col("Total_Sustains") / pl.col("Total_Samples") * 100).round(2).alias("Sustain_Percentage"),
            *self._score_stats_exprs("Total_Scores"),
        ])
        return (
            df.sort("Total_Samples", descending=True)
            .with_row_index("Rank", offset=1)
            .select([
                "Rank", "Character", "Appearance Rate (%)",
                pl.col("Total_Samples").alias("Samples"),
                "Min", "25th Percentile", "Median", "75th Percentile",
                f"Average {self.metric}", "Std Dev", "Max",
                pl.col("Total_Sustains").alias("Sustain Samples"),
                "Sustain_Percentage",
            ])
        )

    def get_team_df(self):
        df = self.team_stats.with_columns([
            pl.col("team_key").list.join(", ")
              .map_elements(lambda s: f"({s})", return_dtype=pl.String).alias("Team"),
            (pl.col("Samples") / self.total_samples * 100).round(2).alias("Appearance Rate (%)"),
            (pl.col("Total_Sustains") == pl.col("Samples")).alias("Sustain?"),
            *self._score_stats_exprs("Scores"),
        ]).sort("Samples", descending=True)
        return df.with_row_index("Rank", offset=1).select([
            "Rank", "Team", "Appearance Rate (%)", "Samples",
            "Min", "25th Percentile", "Median", "75th Percentile",
            f"Average {self.metric}", "Std Dev", "Max", "Sustain?",
        ])

    def get_archetype_df(self):
        df = self.archetypes_stats.with_columns([
            pl.col("archetype_key").list.join(" + ")
              .map_elements(lambda s: s if s != "" else "Other / No DPS", return_dtype=pl.String)
              .alias("Archetype Core"),
            (pl.col("Samples") / self.total_samples * 100).round(2).alias("Usage %"),
            (pl.col("Total_Sustains") / pl.col("Samples") * 100).round(2).alias("Sustain_Percentage"),
            *self._score_stats_exprs("Scores"),
        ]).sort("Samples", descending=True)
        return df.with_row_index("Rank", offset=1).select([
            "Rank", "Archetype Core", "Usage %", "Samples", "Sustain_Percentage",
            pl.col("Total_Sustains").alias("Sustain Samples"),
            "Min", "25th Percentile", "Median", "75th Percentile",
            f"Average {self.metric}", "Max", "Std Dev",
        ])

    def get_duos_stats(self):
        char_freq = self.char_stats.select([
            "Character",
            (pl.col("Total_Samples") / self.total_samples).alias("char_support"),
        ])
        rules = (
            self.duos
            .join(char_freq, left_on="Antecedent", right_on="Character", how="left")
            .rename({"char_support": "support_A"})
            .join(char_freq, left_on="Consequent", right_on="Character", how="left")
            .rename({"char_support": "support_C"})
        )
        return (
            rules
            .with_columns((pl.col("Samples") / self.total_samples).alias("support"))
            .with_columns((pl.col("support") / pl.col("support_A")).alias("confidence"))
            .with_columns([
                (pl.col("confidence") / pl.col("support_C")).alias("lift"),
                (pl.col("support") - pl.col("support_A") * pl.col("support_C")).alias("leverage"),
                ((1 - pl.col("support_C")) / (1 - pl.col("confidence") + 1e-7)).alias("conviction"),
            ])
            .select([
                "Antecedent", "Consequent", "Samples",
                (pl.col("support") * 100).round(2).alias("Appearance Rate (%)"),
                pl.col("confidence").round(3).alias("Confidence"),
                pl.col("lift").round(3).alias("Lift"),
                pl.col("leverage").round(4).alias("Leverage"),
                pl.col("conviction").round(3).alias("Conviction"),
                (pl.col("Total_Sustains") / pl.col("Samples") * 100).round(2).alias("Sustain_Percentage"),
                *self._score_stats_exprs("Scores"),
            ])
            .sort("Lift", descending=True)
        )

    # --- Combined ---

    def get_combined_team_df(self):
        total = self.combined_team_stats.select(pl.col("Samples").sum()).item()
        df = self.combined_team_stats.with_columns([
            pl.col("n1_chars").list.join(", ")
              .map_elements(lambda s: f"({s})", return_dtype=pl.String).alias("Team Node 1"),
            pl.col("n2_chars").list.join(", ")
              .map_elements(lambda s: f"({s})", return_dtype=pl.String).alias("Team Node 2"),
            (pl.col("Samples") / total * 100).round(2).alias("Appearance Rate (%)"),
            *self._score_stats_exprs("Scores"),
        ]).sort("Samples", descending=True)
        return df.with_row_index("Rank", offset=1).select([
            "Rank", "Team Node 1", "Team Node 2", "Appearance Rate (%)", "Samples",
            "Min", "25th Percentile", "Median", "75th Percentile",
            f"Average {self.metric}", "Std Dev", "Max",
        ])

    def get_combined_archetype_df(self):
        total = self.combined_archetypes_stats.select(pl.col("Samples").sum()).item()
        df = self.combined_archetypes_stats.with_columns([
            pl.col("n1_archetype").list.join(" + ")
              .map_elements(lambda s: f"[{s}]" if s != "" else "[Other]", return_dtype=pl.String)
              .alias("Core Node 1"),
            pl.col("n2_archetype").list.join(" + ")
              .map_elements(lambda s: f"[{s}]" if s != "" else "[Other]", return_dtype=pl.String)
              .alias("Core Node 2"),
            (pl.col("Samples") / total * 100).round(2).alias("Appearance Rate (%)"),
            *self._score_stats_exprs("Scores"),
        ]).sort("Samples", descending=True)
        return df.with_row_index("Rank", offset=1).select([
            "Rank", "Core Node 1", "Core Node 2", "Appearance Rate (%)", "Samples",
            "Min", "25th Percentile", "Median", "75th Percentile",
            f"Average {self.metric}", "Std Dev", "Max",
        ])

    def get_combined_char_df(self):
        total = self.combined.select(pl.col("uid").n_unique()).collect().item()
        df = self.combined_char_stats.with_columns([
            pl.col("n1_chars").alias("Character Node 1"),
            pl.col("n2_chars").alias("Character Node 2"),
            (pl.col("Samples") / total * 100).round(2).alias("Appearance Rate (%)"),
            *self._score_stats_exprs("Scores"),
        ]).sort("Samples", descending=True)
        return df.with_row_index("Rank", offset=1).select([
            "Rank", "Character Node 1", "Character Node 2",
            "Samples", "Appearance Rate (%)",
            "Min", "25th Percentile", "Median", "75th Percentile",
            f"Average {self.metric}", "Std Dev", "Max",
        ])

    # -----------------------------------------------------------------------
    # Gear-usage display  (eidolons from _char file — only place they appear)
    # -----------------------------------------------------------------------

    def display_top_gear(self):
        df = self._charstats

        eidolon_levels = sorted(
            {c.split("_")[-1] for c in df.columns if "Eidolon" in c}
        )

        results = []
        for level in eidolon_levels:
            for gear_type in ["Lightcones", "Relics", "Planar_Set"]:
                col_name = f"{gear_type}_{level}"
                if col_name not in df.columns:
                    continue

                temp = (
                    df.select(["Character", col_name])
                    .explode(col_name)
                    .drop_nulls(col_name)
                    .with_columns([
                        pl.col(col_name).struct.field("name").alias("Gear_Name"),
                        pl.col(col_name).struct.field("count").alias("Usage"),
                        pl.col(col_name).struct.field("scores").alias("_scores"),
                    ])
                    .filter(pl.col("Gear_Name") != "Info_not_found")
                )
                if temp.is_empty():
                    continue

                processed = (
                    temp
                    .with_columns(pl.col("Usage").sum().over("Character").alias("_total_usage"))
                    .with_columns([
                        (pl.col("Usage") / pl.col("_total_usage")).alias("Usage_Rate"),
                        pl.col("_scores").list.mean().round(2).alias(f"Avg {self.metric}"),
                        pl.col("_scores").list.median().alias("Median"),
                        pl.col("_scores").list.min().alias(f"Min_{self.metric}"),
                        pl.col("_scores").list.max().alias(f"Max_{self.metric}"),
                        pl.col("_scores").list.std().round(2).alias("Std"),
                        pl.col("_scores").list.eval(pl.element().quantile(0.25)).list.first().alias("25th Percentile"),
                        pl.col("_scores").list.eval(pl.element().quantile(0.75)).list.first().alias("75th Percentile"),
                        pl.lit(level).alias("Eidolon"),
                        pl.lit(gear_type).alias("Category"),
                    ])
                )
                results.append(processed.select([
                    "Character", "Eidolon", "Category", "Gear_Name",
                    "Usage", "Usage_Rate", f"Avg {self.metric}", "25th Percentile",
                    "Median", "75th Percentile", f"Min_{self.metric}", f"Max_{self.metric}", "Std",
                ]))

        if not results:
            return pl.DataFrame()

        return (
            pl.concat(results)
            .sort(
                ["Character", "Eidolon", "Category", "Usage"],
                descending=[False, False, False, True],
            )
        )

    def display_single_char_full(self, char_name: str):
        full_df = self.display_top_gear()
        char_data = full_df.filter(pl.col("Character") == char_name)
        if char_data.is_empty():
            return f"Character '{char_name}' not found or has no valid gear data."
        return char_data.drop("Character")

    def get_gear_usage_df(self):
        """Alias for display_top_gear()."""
        return self.display_top_gear()

    # -----------------------------------------------------------------------
    # Plotting
    # -----------------------------------------------------------------------

    def _plot_score_distribution(self, df, score_col: str, output: bool, title: str):
        if hasattr(df, "collect"):
            df = df.collect()
        
        df = df.drop_nulls(subset=[score_col])
        sample_size = len(df)
        
        if sample_size == 0:
            print("No data available.")
            return None

        # Scalar calculations
        mean    = df.select(pl.col(score_col).mean()).item()
        median  = df.select(pl.col(score_col).median()).item()
        mode    = df.select(pl.col(score_col).mode().first()).item()
        std_dev = df.select(pl.col(score_col).std(ddof=1).fill_null(0)).item()

        # Fixed Stats Logic
        stats_df = (
            df.group_by(score_col)
            .agg(pl.len().alias("Count"))
            .sort(score_col, descending=False if self.mode == "moc" else True)
            .with_columns([
                pl.col("Count").cum_sum().alias("Cum_Count"),
                # Correctly rename the group_by column to your metric name
                pl.col(score_col).alias(f"{self.metric}") 
            ])
            .with_columns(
                # Higher is better: (1 - Cum_Count/Total) logic
                ((1 - pl.col("Cum_Count") / sample_size) * 100).round(2).alias("Percentile (%)")
            )
            .drop("Cum_Count")
            .select([f"{self.metric}", "Count", "Percentile (%)"]) # Keep clean order
        )

        if output:
            print(f"Sample Size: {sample_size}")
            with pl.Config(tbl_rows=-1):
                print(stats_df)

            import matplotlib.pyplot as plt
            plt.figure(figsize=(12, 6))
            plt.hist(df.get_column(score_col).to_list(), bins="auto", alpha=0.5,
                    color="blue", edgecolor="black")
            plt.axvline(mean,   color="orange", linestyle="dashed", linewidth=1, label=f"Mean: {mean:.2f}")
            plt.axvline(median, color="green",  linestyle="dashed", linewidth=1, label=f"Median: {median:.2f}")
            plt.axvline(mode,   color="red",    linestyle="dashed", linewidth=1, label=f"Mode: {mode:.2f}")
            plt.axvline(mean + std_dev, color="purple", linestyle="dashed", linewidth=1, label=f"±Std Dev: {std_dev:.2f}")
            plt.axvline(mean - std_dev, color="purple", linestyle="dashed", linewidth=1)
            plt.title(title)
            plt.xlabel(score_col)
            plt.ylabel("Frequency")
            plt.legend()
            plt.show()
            return

        return stats_df

    def plot_statistics_all(self, output: bool = True):
        return self._plot_score_distribution(
            df=self.lf, score_col=self.score_col, output=output,
            title=f"Score Distribution — v{self.version} [{self.mode.upper()}], floor {self.floor}, node {self.node}",
        )

    def plot_statistics_all_combined(self, output: bool = True):
        return self._plot_score_distribution(
            df=self.combined, score_col="total_score", output=output,
            title=f"Combined Score Distribution — v{self.version} [{self.mode.upper()}], floor {self.floor}",
        )


# =============================================================================
# BATCH CLASS  (lazy / concat-first approach — mirrors Appearance_rate_V2_batch)
# =============================================================================

from dotenv import load_dotenv
load_dotenv()

# ---------------------------------------------------------------------------
# Floor ranges per sub-mode
# MOC_LEGACY     : versions 1.0.3 – 1.5.2  → max floor 10, nodes 1 & 2
# MOC_LATE_LEGACY: versions 1.6.1 – 2.1.4  → max floor 12, nodes 1 & 2
# PF_LEGACY      : versions 1.6.2 – 2.1.3  → max floor  4, nodes 1 & 2
# ---------------------------------------------------------------------------
_LEGACY_SUBMODE_CONFIG = {
    "moc_legacy": {
        "env_key":   "MOC_VERSIONS_LEGACY",
        "mode":      "moc",
        "max_floor": 10,
        "nodes":     [1, 2],
    },
    "moc_late_legacy": {
        "env_key":   "MOC_VERSIONS_LATE_LEGACY",
        "mode":      "moc",
        "max_floor": 12,
        "nodes":     [1, 2],
    },
    "pf_legacy": {
        "env_key":   "PF_VERSIONS_LEGACY",
        "mode":      "pf",
        "max_floor": 4,
        "nodes":     [1, 2],
    },
}


class HonkaiStatistics_Legacy_Batch:
    """
    Batch version of HonkaiStatistics_Legacy — lazy / concat-first approach.

    Mirrors the architecture of Appearance_rate_V2_batch:
      1. _load_data()           — per version: download/cache parquet, apply
                                  schema corrections (floor normalisation,
                                  missing gear columns), return LazyFrames.
      2. _build_lazy_sequences() — loop versions, collect lazy lists, concat
                                  once, run the full pipeline once on the
                                  combined frame.
      3. get_*()                — return plain DataFrames segmented by
                                  [version, floor, node].

    Sub-modes
    ---------
    "moc_legacy"      — MOC_VERSIONS_LEGACY,      floors 1-10, nodes 1 & 2
    "moc_late_legacy" — MOC_VERSIONS_LATE_LEGACY,  floors 1-12, nodes 1 & 2
    "pf_legacy"       — PF_VERSIONS_LEGACY,         floors 1-4,  nodes 1 & 2
    "all_moc"         — both moc_legacy + moc_late_legacy combined

    Parameters
    ----------
    sub_mode : str
        One of the keys above (case-insensitive).
    floor : int | "all"
        Specific floor to include, or "all" for all floors in the sub-mode.
    node : int | "all"
        0 = combined only, 1/2 = single node,
        "all" = nodes 1, 2, and the synthetic node-0 combined rows.
    by_cycle : int
        MoC upper-cycle threshold.  Default 10 000 (no filter).
    by_points : int | float
        PF lower-points threshold.
    by_cycle_combined : int
        MoC combined threshold.
    by_points_combined : int | float
        PF combined threshold.
    """

    def __init__(
        self,
        sub_mode: str,
        floor="all",
        node="all",
        by_cycle: int = 10_000,
        by_points: int | float = 0,
        by_cycle_combined: int = 10_000,
        by_points_combined: int | float = 0,
    ):
        sub_mode = sub_mode.lower()

        if sub_mode == "all_moc":
            self._submodes = ["moc_legacy", "moc_late_legacy"]
        elif sub_mode in _LEGACY_SUBMODE_CONFIG:
            self._submodes = [sub_mode]
        else:
            raise ValueError(
                f"Unknown sub_mode {sub_mode!r}. "
                f"Choose from: {list(_LEGACY_SUBMODE_CONFIG)} + 'all_moc'"
            )

        # Determine mode (moc or pf) — all_moc is always moc
        self.mode = _LEGACY_SUBMODE_CONFIG[self._submodes[0]]["mode"]
        self.metric = "Cycles" if self.mode == "moc" else "Points"
        self.score_col = "round_num"
        self.folder = "raw_data"

        self.floor              = floor
        self.node               = node
        self.by_cycle           = by_cycle
        self.by_points          = by_points
        self.by_cycle_combined  = by_cycle_combined
        self.by_points_combined = by_points_combined

        self.lazy_frames      = []
        self.char_lazy_frames = []

        self._build_lazy_sequences()

    # ------------------------------------------------------------------
    # Per-version data loader with schema normalisation
    # ------------------------------------------------------------------

    def _load_data(self, version: str, mode: str):
        """
        Load a single version's parquet files, apply all schema corrections
        that vary per version, and return two fully normalised LazyFrames.

        Schema corrections done here (so the concat later can be vertical):
          - Floor: string → Int64 (versions ≤ 1.4.x)
          - Duplicate rows removed (versions ≤ 1.4.1)
          - Gear columns: weapon / artifacts / relics guaranteed present
            (filled with null if missing — struct col added as null col)
          - cons column in char file guaranteed present (0 if absent)
          - version tag stamped onto both frames
        """
        suffix = "_pf" if mode == "pf" else ""
        path       = os.path.join(self.folder, f"{version}{suffix}.parquet")
        path_char  = os.path.join(self.folder, f"{version}_char.parquet")

        corrupt_bullets = ["\u00e2\u20ac\u00a2", "\u00c3\u00a2\u00e2\u201a\u00ac\u00c2\u00a2"]
        clean_bullets   = ["\u2022", "\u2022"]

        # --- Stage file ---
        if not os.path.exists(path):
            url = f"https://huggingface.co/datasets/LvlUrArti/MocData/resolve/main/{version}{suffix}.csv"
            tmp = pl.read_csv(url).unique()   # deduplicate on CSV load too
            os.makedirs(self.folder, exist_ok=True)
            tmp.write_parquet(path)
        else:
            # Deduplicate eagerly on first access (<=1.4.1 parquets may have dupes)
            tmp = pl.read_parquet(path).unique()
            tmp.write_parquet(path)           # overwrite cleaned version

        stage_lf = pl.scan_parquet(path)

        # Floor: cast string → Int64 if needed (check schema without collecting)
        if stage_lf.collect_schema()["floor"] == pl.String:
            stage_lf = stage_lf.with_columns(
                pl.col("floor")
                .str.replace_all(r"<[^>]+>", "")
                .str.extract(r".*?(\d+)\D*$", group_index=1)
                .cast(pl.Int64)
                .alias("floor")
            )

        stage_lf = stage_lf.with_columns([
            pl.col("uid").cast(pl.String),
            pl.lit(version).alias("version"),
            cs.string().exclude("uid")
              .str.replace_many(corrupt_bullets, clean_bullets)
              .str.replace_all(r"\band\b", "&")
              .str.replace_all(r"^March 7th$", "Ice March 7th"),
        ])

        # --- Char file ---
        if not os.path.exists(path_char):
            url = f"https://huggingface.co/datasets/LvlUrArti/MocData/resolve/main/{version}_char.csv"
            tmp = pl.read_csv(url)
            os.makedirs(self.folder, exist_ok=True)
            tmp.write_parquet(path_char)

        char_schema = pl.scan_parquet(path_char).collect_schema()

        char_lf = pl.scan_parquet(path_char).with_columns([
            pl.col("uid").cast(pl.String),
            pl.lit(version).alias("version"),
            cs.string().exclude("uid")
              .str.replace_many(corrupt_bullets, clean_bullets)
              .str.replace_all(r"\band\b", "&")
              .str.replace_all(r"^March 7th$", "Ice March 7th"),
        ])

        # Guarantee gear columns exist (versions like 1.0.3 may be missing some)
        for gear_col in ["weapon", "artifacts", "relics"]:
            if gear_col not in char_schema.names():
                char_lf = char_lf.with_columns(pl.lit(None).cast(pl.String).alias(gear_col))

        # Guarantee cons column exists (not present in very early versions)
        if "cons" not in char_schema.names():
            char_lf = char_lf.with_columns(pl.lit(0).cast(pl.Int64).alias("cons"))

        # Drop columns that collide on join downstream
        for drop_col in ["phase", "level"]:
            if drop_col in char_schema.names():
                char_lf = char_lf.drop(drop_col)

        return stage_lf, char_lf

    # ------------------------------------------------------------------
    # Lazy sequence builder  (mirrors _build_lazy_sequences in V2 batch)
    # ------------------------------------------------------------------

    def _build_lazy_sequences(self):
        """
        1. Loop through every version across all selected sub-modes.
        2. Call _load_data() per version — schema is normalised there.
        3. Concat all stage frames vertically (schemas now match).
        4. Apply sustain flag, score filter, floor/node synthetic rows.
        5. Run _process_data() and _process_combined_data() once.
        """
        with open("characters.json", "rb") as f:
            info = orjson.loads(f.read())

        rol_df = pl.DataFrame([
            {
                "char":       k,
                "is_sustain": "sustain" in v.get("role", []),
                "is_dps":     bool(set(v.get("role", [])).intersection({"dps", "specialist"})),
                "sort_index": i,
            }
            for i, (k, v) in enumerate(info.items())
        ])
        sustain_names = rol_df.filter(pl.col("is_sustain")).get_column("char").to_list()
        dps_names     = rol_df.filter(pl.col("is_dps")).get_column("char").to_list()
        char_to_index = dict(zip(rol_df["char"], rol_df["sort_index"]))
        char_cols     = ["ch1", "ch2", "ch3", "ch4"]

        # --- collect lazy frames per version ---
        for sm in self._submodes:
            cfg      = _LEGACY_SUBMODE_CONFIG[sm]
            raw      = os.getenv(cfg["env_key"], "")
            versions = [v.strip() for v in raw.split(",") if v.strip()]
            for v in versions:
                stage_lf, char_lf = self._load_data(v, cfg["mode"])
                self.lazy_frames.append(stage_lf)
                self.char_lazy_frames.append(char_lf)

        if not self.lazy_frames:
            raise RuntimeError("No data loaded — check .env keys.")

        # --- concat once (vertical is safe: _load_data normalised schemas) ---
        lf      = pl.concat(self.lazy_frames,      how="diagonal")
        char_lf = pl.concat(self.char_lazy_frames, how="diagonal")

        # --- floor filter ---
        if self.floor != "all":
            lf = lf.filter(pl.col("floor") == self.floor)

        row_max_stars_expr = (
            pl.lit(3)
        )
        
        # --- sustain flag & star num---
        lf = lf.with_columns(
            pl.any_horizontal([pl.col(c).is_in(sustain_names) for c in char_cols])
              .alias("has_sustain"),
            (pl.col('star_num') == row_max_stars_expr).alias("is_full_clear"), 
        )

        # --- score filter ---
        if self.mode == "moc":
            lf = lf.filter(pl.col(self.score_col) <= self.by_cycle)
        else:
            lf = lf.filter(pl.col(self.score_col) >= self.by_points)

        self.lf = lf

        # --- combined (cross-node) data ---
        base_cols = ["uid", "version", "floor", "node", self.score_col,"has_sustain","is_full_clear"] + char_cols
        lf_base   = lf.select(base_cols)

        n1 = lf_base.filter(pl.col("node") == 1).select([
            "uid", "version", "floor",
            pl.concat_list(char_cols)
              .list.eval(pl.element().sort_by(pl.element().replace_strict(char_to_index, default=999)))
              .alias("n1_chars"),
            pl.col(self.score_col).alias("n1_score"),
            pl.col("has_sustain").alias("n1_has_sustain"),
            pl.col("is_full_clear")
        ])
        n2 = lf_base.filter(pl.col("node") == 2).select([
            "uid", "version", "floor",
            pl.concat_list(char_cols)
              .list.eval(pl.element().sort_by(pl.element().replace_strict(char_to_index, default=999)))
              .alias("n2_chars"),
            pl.col(self.score_col).alias("n2_score"),
            pl.col("has_sustain").alias("n2_has_sustain"),
            pl.col("is_full_clear")
        ])

        combined = n1.join(n2, on=["uid", "version", "floor","is_full_clear"], how="inner")

        if self.mode == "moc":
            combined = combined.with_columns(pl.col("n1_score").alias("total_score"))
            self.combined = combined.filter(pl.col("total_score") <= self.by_cycle_combined)
        else:
            combined = combined.with_columns((pl.col("n1_score") + pl.col("n2_score")).alias("total_score"))
            self.combined = combined.filter(pl.col("total_score") >= self.by_points_combined)

        self._process_combined_data(self.combined, dps_names, char_to_index)

        # --- synthetic node=0 rows (mirrors V2 batch node trick) ---
        lf_node0 = lf.with_columns(
            pl.when(pl.col("node").is_in([1, 2]))
              .then(0)
              .otherwise(pl.col("node"))
              .alias("node")
        )

        if self.node == "all":
            # node="all": keep original node rows PLUS the relabelled node-0 rows
            self.lf = pl.concat([lf_node0, lf], how="diagonal")
        elif self.node in (0, "0"):
            # combined only
            self.lf = lf_node0
        else:
            # single node filter
            self.lf = lf.filter(pl.col("node") == int(self.node))

        self._process_data(self.lf, char_lf, char_cols, dps_names, char_to_index)

    # ------------------------------------------------------------------
    # Core aggregation pipeline
    # ------------------------------------------------------------------

    def _process_data(self, lf, char_lf, char_cols, dps_names, char_to_index):
        seg_keys = ["version", "floor", "node"]

        # Unpivot chars + join char build data
        chars_long = (
            lf.unpivot(
                index=["uid", self.score_col, "has_sustain","is_full_clear"] + seg_keys,
                on=char_cols,
                value_name="Character",
            )
            .drop("variable")
            .with_columns(pl.col("Character").fill_null("Empty Slot"))
        )

        base_data = (
            chars_long
            .join(
                char_lf,
                left_on=["uid", "Character", "version"],
                right_on=["uid", "name",      "version"],
                how="left",
            )
            .with_columns([
                pl.col("weapon")   .fill_null("Info_not_found"),
                pl.col("artifacts").fill_null("Info_not_found"),
                pl.col("relics")   .fill_null("Info_not_found"),
                # Cast to Int32 so our range(7) filters match safely
                pl.col("cons")     .fill_null(0).cast(pl.Int32), 
            ])
        )
        
        # Drop any residual cols that collide
        for drop_col in ["phase", "level", "cons_right"]:
            schema = base_data.collect_schema().names()
            if drop_col in schema:
                base_data = base_data.drop(drop_col)

        group_keys = seg_keys + ["Character"]

        def rollup_gear(df, gear_col, alias):
            return (
                df.group_by(group_keys + [gear_col, "cons"])
                .agg([
                    pl.count("uid").alias("count"),
                    pl.col(self.score_col).alias("scores"),
                ])
                .group_by(group_keys + ["cons"])
                .agg(
                    pl.struct([pl.col(gear_col).alias("name"), "count", "scores"]).alias(alias)
                )
                .with_columns(
                    (pl.lit(f"{alias}_Eidolon ") + pl.col("cons").cast(pl.String)).alias("_col_label")
                )
                .collect()
                .pivot(on="_col_label", index=group_keys, values=alias, aggregate_function="first")
                .lazy()
            )

        w_df = rollup_gear(base_data, "weapon",    "Lightcones")
        a_df = rollup_gear(base_data, "artifacts", "Relics")
        r_df = rollup_gear(base_data, "relics",    "Planar_Set")

        agg_base = (
            base_data.group_by(group_keys)
            .agg([
                pl.count("uid").alias("Total_Samples"),
                pl.col(self.score_col).alias("Total_Scores"),
                pl.col("has_sustain").sum().alias("Total_Sustains"),
                pl.col("is_full_clear").sum().alias("Total_Full_Clears"),
                pl.col("uid").unique().alias("uids"),
            ])
            .join(w_df, on=group_keys, how="left")
            .join(a_df, on=group_keys, how="left")
            .join(r_df, on=group_keys, how="left")
        )

        self.char_stats = agg_base.collect()

        # Team stats
        self.team_stats = (
            lf.with_columns(
                pl.concat_list(char_cols)
                  .list.eval(pl.element().sort_by(pl.element().replace_strict(char_to_index, default=999)))
                  .alias("team_key")
            )
            .group_by(seg_keys + ["team_key"])
            .agg([
                pl.count("uid").alias("Samples"),
                pl.col(self.score_col).alias("Scores"),
                pl.col("uid").unique().alias("uids"),
                pl.col("is_full_clear").sum().alias("Total_Full_Clears"),
                pl.col("has_sustain").sum().alias("Total_Sustains"),
            ]).with_columns(
                pl.col("team_key")
                .list.eval(pl.element().filter(pl.element().is_in(dps_names) & pl.element().is_not_null()))
                .alias("archetype_key")
            )
            .collect()
        )

        # Archetypes
        self.archetypes_stats = (
            self.team_stats
            .group_by(seg_keys + ["archetype_key"])
            .agg([
                pl.col("Samples").sum(),
                pl.col("Scores").list.explode().alias("Scores"),
                pl.col("uids").list.explode().unique().alias("uids"),
                pl.col("Total_Full_Clears").sum(),
                pl.col("Total_Sustains").sum(),
            ])
        )

        # Duos
        exploded = (
            self.team_stats
            .with_columns(pl.col("team_key").alias("Consequent"))
            .explode("team_key")
            .explode("Consequent")
            .filter(pl.col("team_key") != pl.col("Consequent"))
        )
        self.duos = exploded.group_by(
            seg_keys + [pl.col("team_key").alias("Antecedent"), "Consequent"]
        ).agg([
            pl.col("Samples").sum(),
            pl.col("Scores").list.explode().alias("Scores"),
            pl.col("uids").list.explode().unique().alias("uids"),
            pl.col("Total_Sustains").sum(),
            pl.col("Total_Full_Clears").sum()
        ])

        self.total_samples_df = (
            lf.group_by(seg_keys)
            .agg(pl.col("uid").n_unique().alias("version_total_samples"))
            .collect()
        )

    def _process_combined_data(self, combined, dps_names, char_to_index):
        cmb_keys = ["version", "floor"]

        self.combined_team_stats = (
            combined.group_by(cmb_keys + ["n1_chars", "n2_chars"])
            .agg([
                pl.count("uid").alias("Samples"),
                pl.col("total_score").alias("Scores"),
                pl.col("uid").unique().alias("uids"),
                pl.col("is_full_clear").sum().alias("Total_Full_Clears"),
                cs.ends_with("_has_sustain").sum()
            ]).with_columns([
                pl.col("n1_chars").list.eval(pl.element().filter(pl.element().is_in(dps_names) & pl.element().is_not_null())).alias("n1_archetype"),
                pl.col("n2_chars").list.eval(pl.element().filter(pl.element().is_in(dps_names) & pl.element().is_not_null())).alias("n2_archetype"),
            ])
            .collect()
        )

        self.combined_archetypes_stats = (
            self.combined_team_stats
            .group_by(cmb_keys + ["n1_archetype", "n2_archetype"])
            .agg([
                pl.col("Samples").sum(),
                pl.col("Scores").list.explode().alias("Scores"),
                pl.col("uids").list.explode().unique().alias("uids"),
                pl.col("Total_Full_Clears").sum(),
                cs.ends_with("_has_sustain").sum()
            ])
        )

        self.combined_char_stats = (
            combined.select(cmb_keys + ["uid", "total_score", "n1_chars", "n2_chars"])
            .explode("n1_chars")
            .explode("n2_chars")
            .group_by(cmb_keys + ["n1_chars", "n2_chars"])
            .agg([
                pl.count("uid").alias("Samples"),
                pl.col("total_score").alias("Scores"),
            ])
            .collect()
        )

        self.combined_total_samples_df = (
            combined.group_by(cmb_keys)
            .agg(pl.col("uid").n_unique().alias("combined_version_total_samples"))
            .collect()
        )
        
    def _plot_score_distribution(self, df, score_col: str, output: bool, title: str):
        if hasattr(df, "collect"):
            df = df.collect()
        
        df = df.drop_nulls(subset=[score_col])
        
        if len(df) == 0:
            print("No data available.")
            return None

        # 1. Dynamic grouping columns determination
        group_cols = []
        if "version" in df.columns:
            group_cols.append("version")
        if "node" in df.columns:
            group_cols.append("node")    
        if "floor" in df.columns:
            group_cols.append("floor")

        # 2. Compute overall descriptive stats per group (or globally if ungrouped)
        stats_exprs = [
            pl.len().alias("sample_size"),
            pl.col(score_col).mean().alias("mean"),
            pl.col(score_col).median().alias("median"),
            pl.col(score_col).mode().first().alias("mode"),
            pl.col(score_col).std(ddof=1).fill_null(0).alias("std_dev")
        ]
        
        if group_cols:
            group_stats = df.group_by(group_cols).agg(stats_exprs)
        else:
            group_stats = df.select(stats_exprs)

        # 3. Process main distribution frequencies
        # MoC (cycles): Lower is better -> sort ascending. PF/AS (points): Higher is better -> sort descending.
        is_lower_better = (self.mode.lower() == "moc")
        
        main_group_keys = group_cols + [score_col]
        stats_df = df.group_by(main_group_keys).agg(pl.len().alias("Count"))
        
        # Sort correctly so that the best performing scores appear first
        if group_cols:
            stats_df = stats_df.sort(group_cols + [score_col], descending=[False] * len(group_cols) + [not is_lower_better])
            stats_df = stats_df.join(group_stats, on=group_cols, how="left")
            
            # Calculate rolling metrics safely isolated within each group partition
            stats_df = stats_df.with_columns([
                pl.col("Count").cum_sum().over(group_cols).alias("Cum_Count"),
                (pl.col("Count").cum_sum().over(group_cols) - pl.col("Count")).alias("Strictly_Better_Count")
            ])
        else:
            stats_df = stats_df.sort(score_col, descending=not is_lower_better)
            stats_df = stats_df.with_columns([
                pl.col("Count").cum_sum().alias("Cum_Count"),
                (pl.col("Count").cum_sum() - pl.col("Count")).alias("Strictly_Better_Count")
            ])

        # 4. Standard competitive percentile rank: 100 - (percentage of people who performed strictly better + mid-point of ties)
        stats_df = stats_df.with_columns(
            (100 - ((pl.col("Strictly_Better_Count") + (pl.col("Count"))) / pl.col("sample_size") * 100)).round(2).alias("Percentile (%)")
        )

        # Clean up structure and map score to your custom metric name alias
        metric_alias = getattr(self, "metric", score_col)
        final_df = stats_df.select(
            group_cols + [pl.col(score_col).alias(str(metric_alias)), "Count", "Percentile (%)"]
        )

        # 5. Safe Outputting & Plotting Block
        if output:
            import matplotlib.pyplot as plt
            unique_groups = df.select(group_cols).unique().to_dicts() if group_cols else [{}]

            for group in unique_groups:
                # Filter main data and stats frame for plotting loops
                filtered_df = df
                filtered_final = final_df
                for col, val in group.items():
                    filtered_df = filtered_df.filter(pl.col(col) == val)
                    filtered_final = filtered_final.filter(pl.col(col) == val)

                if len(filtered_df) == 0:
                    continue

                # Extract the specific row of summary stats for this unique group mapping
                if group_cols:
                    m_info = group_stats.filter([pl.col(c) == v for c, v in group.items()]).to_dicts()[0]
                else:
                    m_info = group_stats.to_dicts()[0]
                
                group_tag = " | ".join([f"{k}: {v}" for k, v in group.items()])
                display_title = f"{title} ({group_tag})" if group_tag else title

                print(f"\n--- {group_tag if group_tag else 'Global Score Distribution'} ---")
                print(f"Sample Size: {m_info['sample_size']}")
                with pl.Config(tbl_rows=-1):
                    print(filtered_final)

                # Matplotlib Visual Renderer
                scores_list = filtered_df.get_column(score_col).to_list()

                plt.figure(figsize=(12, 6))
                plt.hist(scores_list, bins='auto', alpha=0.5, color='blue', edgecolor='black', label='Score Frequency')

                # Render summary baseline parameters
                plt.axvline(m_info['mean'], color='orange', linestyle='dashed', linewidth=1, label=f"Mean: {m_info['mean']:.2f}")
                plt.axvline(m_info['median'], color='green', linestyle='dashed', linewidth=1, label=f"Median: {m_info['median']:.2f}")
                plt.axvline(m_info['mode'], color='red', linestyle='dashed', linewidth=1, label=f"Mode: {m_info['mode']:.2f}")
                plt.axvline(m_info['mean'] + m_info['std_dev'], color='purple', linestyle='dashed', linewidth=1, label=f"±Std Dev: {m_info['std_dev']:.2f}")
                plt.axvline(m_info['mean'] - m_info['std_dev'], color='purple', linestyle='dashed', linewidth=1)

                plt.title(display_title)
                plt.xlabel(str(metric_alias))
                plt.ylabel('Frequency')
                plt.legend()
                plt.show()
            
            return

        return final_df
    
    # ------------------------------------------------------------------
    # Shared stats expression helper
    # ------------------------------------------------------------------

    def _score_stats_exprs(self, col: str) -> list[pl.Expr]:
        m = self.metric
        return [
            pl.col(col).list.min().alias(f"Min {m}"),
            pl.col(col).list.eval(pl.element().quantile(0.25)).list.first().round(2).alias(f"25th Percentile {m}"),
            pl.col(col).list.median().round(2).alias(f"Median {m}"),
            pl.col(col).list.eval(pl.element().quantile(0.75)).list.first().round(2).alias(f"75th Percentile {m}"),
            pl.col(col).list.mean().round(2).alias(f"Average {m}"),
            pl.col(col).list.eval(pl.element().std()).list.first().round(2).alias(f"Std Dev {m}"),
            pl.col(col).list.max().alias(f"Max {m}"),
        ]

    # ------------------------------------------------------------------
    # Public getters
    # ------------------------------------------------------------------

    _VER_KEYS = ["version", "floor", "node"]
    _CMB_KEYS = ["version", "floor"]

    def get_char_df(self) -> pl.DataFrame:
        vk = self._VER_KEYS
        m  = self.metric
        df = (
            self.char_stats
            .join(self.total_samples_df, on=vk, how="left")
            .with_columns([
                (pl.col("Total_Samples") / pl.col("version_total_samples") * 100).round(3).alias("Appearance Rate (%)"),
                (pl.col("Total_Sustains") / pl.col("Total_Samples") * 100).round(2).alias("Sustain_Percentage"),
                (pl.col("Total_Full_Clears")/pl.col("Total_Samples")* 100).round(2).alias("Full_Clear_Rate"),
                *self._score_stats_exprs("Total_Scores"),
            ])
            .sort(vk + ["Total_Samples"], descending=[True, True, False, True])
            .with_row_index("Rank", offset=1)
        )
        return df.select([
            "Rank", *vk, "Character", "Appearance Rate (%)",
            pl.col("Total_Samples").alias("Samples"),
            f"Min {m}", f"25th Percentile {m}", f"Median {m}",
            f"75th Percentile {m}", f"Average {m}", f"Std Dev {m}", f"Max {m}",
            pl.col("Total_Sustains").alias("Sustain Samples"), "Sustain_Percentage",
            "Total_Full_Clears", "Full_Clear_Rate",
        ])

    def get_team_df(self) -> pl.DataFrame:
        vk = self._VER_KEYS
        m  = self.metric
        df = (
            self.team_stats
            .join(self.total_samples_df, on=vk, how="left")
            .with_columns([
                pl.col("team_key").list.join(", ")
                  .map_elements(lambda s: f"({s})", return_dtype=pl.String).alias("Team"),
                  pl.col("archetype_key").list.join(" + ")
                .map_elements(lambda s: s if s != "" else "Other / No DPS", return_dtype=pl.String)
                .alias("Archetype Core"),
                (pl.col("Samples") / pl.col("version_total_samples") * 100).round(2).alias("Appearance Rate (%)"),
                (pl.col("Total_Sustains") == pl.col("Samples")).alias("Sustain?"),
                (pl.col("Total_Full_Clears")/pl.col("Samples")* 100).round(2).alias("Full_Clear_Rate"),
                *self._score_stats_exprs("Scores"),
            ])
            .sort(vk + ["Samples"], descending=[True, True, False, True])
            .with_row_index("Rank", offset=1)
        )
        return df.select([
            "Rank", *vk, "Team","Archetype Core", "Appearance Rate (%)", "Samples",
            f"Min {m}", f"25th Percentile {m}", f"Median {m}",
            f"75th Percentile {m}", f"Average {m}", f"Std Dev {m}", f"Max {m}", "Sustain?"
            ,"Total_Full_Clears","Full_Clear_Rate"
        ])

    def get_archetype_df(self) -> pl.DataFrame:
        vk = self._VER_KEYS
        m  = self.metric
        df = (
            self.archetypes_stats
            .join(self.total_samples_df, on=vk, how="left")
            .with_columns([
                pl.col("archetype_key").list.join(" + ")
                  .map_elements(lambda s: s if s != "" else "Other / No DPS", return_dtype=pl.String)
                  .alias("Archetype Core"),
                (pl.col("Samples") / pl.col("version_total_samples") * 100).round(2).alias("Usage %"),
                (pl.col("Total_Sustains") / pl.col("Samples") * 100).round(2).alias("Sustain_Percentage"),
                (pl.col("Total_Full_Clears")/pl.col("Samples")* 100).round(2).alias("Full_Clear_Rate"),
                *self._score_stats_exprs("Scores"),
            ])
            .sort(vk + ["Samples"], descending=[True, True, False, True])
            .with_row_index("Rank", offset=1)
        )
        return df.select([
            "Rank", *vk, "Archetype Core", "Usage %", "Samples",
            "Sustain_Percentage", pl.col("Total_Sustains").alias("Sustain Samples"), "Full_Clear_Rate", "Total_Full_Clears",
            f"Min {m}", f"25th Percentile {m}", f"Median {m}",
            f"75th Percentile {m}", f"Average {m}", f"Max {m}", f"Std Dev {m}",
        ])

    def get_duos_stats(self) -> pl.DataFrame:
        vk = self._VER_KEYS
        m  = self.metric

        char_freq = (
            self.char_stats
            .join(self.total_samples_df, on=vk, how="left")
            .select(vk + ["Character",
                          (pl.col("Total_Samples") / pl.col("version_total_samples")).alias("char_support")])
        )
        rules = (
            self.duos
            .join(char_freq, left_on=vk + ["Antecedent"], right_on=vk + ["Character"], how="left")
            .rename({"char_support": "support_A"})
            .join(char_freq, left_on=vk + ["Consequent"], right_on=vk + ["Character"], how="left")
            .rename({"char_support": "support_C"})
            .join(self.total_samples_df, on=vk, how="left")
        )
        return (
            rules
            .with_columns((pl.col("Samples") / pl.col("version_total_samples")).alias("support"))
            .with_columns((pl.col("support") / pl.col("support_A")).alias("confidence"))
            .with_columns([
                (pl.col("confidence") / pl.col("support_C")).alias("lift"),
                (pl.col("support") - pl.col("support_A") * pl.col("support_C")).alias("leverage"),
                ((1 - pl.col("support_C")) / (1 - pl.col("confidence") + 1e-7)).alias("conviction"),
            ])
            .select(vk + [
                "Antecedent", "Consequent", "Samples",
                (pl.col("support") * 100).round(2).alias("Appearance Rate (%)"),
                pl.col("confidence").round(3).alias("Confidence"),
                pl.col("lift").round(3).alias("Lift"),
                pl.col("leverage").round(4).alias("Leverage"),
                pl.col("conviction").round(3).alias("Conviction"),
                 pl.col("Total_Sustains"),
                (pl.col("Total_Sustains") / pl.col("Samples") * 100).round(2).alias("Sustain_Percentage"),
                pl.col("Total_Full_Clears"),
                (pl.col("Total_Full_Clears")/pl.col("Samples")* 100).round(2).alias("Full_Clear_Rate"),
                *self._score_stats_exprs("Scores"),
            ])
            .sort(vk + ["Lift"], descending=[True, True, False, True])
        )
        
    def display_top_gear(self, char_name: str | None = None) -> pl.DataFrame:
        """
        Flat DataFrame: one row per (version, floor, node, Character, Eidolon, Category, Gear_Name).

        Columns
        -------
        version, floor, node, Character, Eidolon, Category, Gear_Name,
        Usage, Usage_Rate,
        Avg_{metric}, 25th Percentile {metric}, Median_{metric},
        75th Percentile {metric}, Min_{metric}, Max_{metric}, Std_{metric}

        Parameters
        ----------
        char_name : str | None
            If given, filters the output to that character only.
        """
        m   = self.metric
        df  = self.char_stats
        vk  = self._VER_KEYS  # ["version", "floor", "node"]

        # Gear col names are e.g. "Lightcones_Eidolon 0", "Relics_Eidolon 6"
        eidolon_levels = sorted(
            {c.split("_Eidolon ")[-1] for c in df.columns if "_Eidolon " in c},
            key=lambda x: int(x),
        )

        results = []
        for level in eidolon_levels:
            for gear_type in ["Lightcones", "Relics", "Planar_Set"]:
                col_name = f"{gear_type}_Eidolon {level}"
                if col_name not in df.columns:
                    continue

                temp = (
                    df.select(vk + ["Character", col_name])
                    .explode(col_name)
                    .drop_nulls(col_name)
                    .with_columns([
                        pl.col(col_name).struct.field("name").alias("Gear_Name"),
                        pl.col(col_name).struct.field("count").alias("Usage"),
                        pl.col(col_name).struct.field("scores").alias("_scores"),
                    ])
                    .filter(pl.col("Gear_Name") != "Info_not_found")
                    .drop(col_name)
                )
                if temp.is_empty():
                    continue

                processed = (
                    temp
                    .with_columns(
                        pl.col("Usage").sum().over(vk + ["Character"]).alias("_total_usage")
                    )
                    .with_columns([
                        (pl.col("Usage") / pl.col("_total_usage")).alias("Usage_Rate"),
                        pl.col("_scores").list.mean().round(2).alias(f"Avg_{m}"),
                        pl.col("_scores").list.eval(pl.element().quantile(0.25)).list.first().round(2).alias(f"25th Percentile {m}"),
                        pl.col("_scores").list.median().round(2).alias(f"Median_{m}"),
                        pl.col("_scores").list.eval(pl.element().quantile(0.75)).list.first().round(2).alias(f"75th Percentile {m}"),
                        pl.col("_scores").list.min().alias(f"Min_{m}"),
                        pl.col("_scores").list.max().alias(f"Max_{m}"),
                        pl.col("_scores").list.std(ddof=1).round(2).alias(f"Std_{m}"),
                        pl.lit(f"Eidolon {level}").alias("Eidolon"),
                        pl.lit(gear_type).alias("Category"),
                    ])
                    .drop(["_scores", "_total_usage"])
                )

                results.append(processed.select(
                    vk + [
                        "Character", "Eidolon", "Category", "Gear_Name",
                        "Usage", "Usage_Rate",
                        f"Avg_{m}", f"25th Percentile {m}", f"Median_{m}",
                        f"75th Percentile {m}", f"Min_{m}", f"Max_{m}", f"Std_{m}",
                    ]
                ))

        if not results:
            return pl.DataFrame()

        out = (
            pl.concat(results, how="diagonal")
            .sort(
                vk + ["Character", "Eidolon", "Category", "Usage"],
                descending=[True, True, False, False, False, False, True],
            )
        )

        if char_name is not None:
            out = out.filter(pl.col("Character") == char_name)

        return out

    def display_single_char_full(self, char_name: str) -> pl.DataFrame | str:
        """Gear rows for a single character, without the Character column."""
        out = self.display_top_gear(char_name=char_name)
        if out.is_empty():
            return f"Character '{char_name}' not found or has no valid gear data."
        return out.drop("Character")

        
    # --- Combined getters ---

    def _assert_combined(self):
        if not hasattr(self, "combined_team_stats"):
            raise RuntimeError("No combined data — check node setting.")

    def get_combined_team_df(self) -> pl.DataFrame:
        self._assert_combined()
        ck = self._CMB_KEYS
        m  = self.metric
        node_char_cols = ["n1_chars","n2_chars"]
        archetype_cols = [c.replace("_chars", "_archetype") for c in node_char_cols]
        archetype_label_cols = [f"Core Node {i+1}" for i in range(len(node_char_cols))]
        sustain_cols = [f"n{i+1}_has_sustain" for i in range(len(node_char_cols)) ]
        df = (
            self.combined_team_stats
            .join(self.combined_total_samples_df, on=ck, how="left")
            .with_columns([
                *[
                pl.col(c).list.join(", ").map_elements(lambda s: f"({s})", return_dtype=pl.String)
                .alias(f"Team Node {i+1}")
                for i, c in enumerate(node_char_cols)
            ],
            *[
                pl.col(c).list.join(" + ")
                .map_elements(lambda s: f"[{s}]" if s != "" else "[Other]", return_dtype=pl.String)
                .alias(archetype_label_cols[i])
                for i, c in enumerate(archetype_cols)
            ],
            *[
                (pl.col(c) == pl.col("Samples"))
                for c in sustain_cols
            ],
            (pl.col("Samples") / pl.col("combined_version_total_samples") * 100).round(2).alias("Appearance Rate (%)"),
            
            pl.col("Total_Full_Clears"),
            (pl.col("Total_Full_Clears")/pl.col("Samples")* 100).round(2).alias("Full_Clear_Rate"),
                *self._score_stats_exprs("Scores"),
            ])
            .sort(ck + ["Samples"], descending=[True, True, True])
            .with_row_index("Rank", offset=1)
        )
        
        team_label_cols = [f"Team Node {i+1}" for i in range(len(node_char_cols))]
        
        return df.select(["Rank", *ck, 
                            *team_label_cols,
                            *archetype_label_cols,
                            *sustain_cols,
                            "Total_Full_Clears", "Full_Clear_Rate", "Appearance Rate (%)", "Samples",
                            f"Min {m}", f"25th Percentile {m}", f"Median {m}",
                            f"75th Percentile {m}", f"Average {m}", f"Std Dev {m}", f"Max {m}"])

    def get_combined_archetype_df(self) -> pl.DataFrame:
        self._assert_combined()
        ck = self._CMB_KEYS
        m  = self.metric
        
        node_char_cols = ["n1_chars","n2_chars"]
        archetype_cols = [c.replace("_chars", "_archetype") for c in node_char_cols]
        archetype_label_cols = [f"Core Node {i+1}" for i in range(len(node_char_cols))]

        sustain_cols = [f"n{i+1}_has_sustain" for i in range(len(node_char_cols)) ]
        sustain_label_cols = [f"n{i+1}_Sustain_Percentage" for i in range(len(node_char_cols))]
        
        df = (
            self.combined_archetypes_stats
            .join(self.combined_total_samples_df, on=ck, how="left")
            .with_columns([
            *[
                pl.col(c).list.join(" + ")
                .map_elements(lambda s: f"[{s}]" if s != "" else "[Other]", return_dtype=pl.String)
                .alias(archetype_label_cols[i])
                for i, c in enumerate(archetype_cols)
            ],
            *[
                (pl.col(c) /pl.col("Samples")* 100).round(2).alias(sustain_label_cols[i])
                for i, c in enumerate(sustain_cols)
            ],
            (pl.col("Samples") / pl.col("combined_version_total_samples") * 100).round(2).alias("Appearance Rate (%)"),
            pl.col("Total_Full_Clears"),
            (pl.col("Total_Full_Clears")/pl.col("Samples")* 100).round(2).alias("Full_Clear_Rate"),
                *self._score_stats_exprs("Scores"),
            ])
            .sort(ck + ["Samples"], descending=[True, True, True])
            .with_row_index("Rank", offset=1)
        )
        return df.select(["Rank", *ck, *archetype_label_cols,
            *sustain_cols,
            *sustain_label_cols, "Appearance Rate (%)", "Samples",
                          f"Min {m}", f"25th Percentile {m}", f"Median {m}",
                          f"75th Percentile {m}", f"Average {m}", f"Std Dev {m}", f"Max {m}"])

    def get_combined_char_df(self) -> pl.DataFrame:
        self._assert_combined()
        ck = self._CMB_KEYS
        m  = self.metric
        df = (
            self.combined_char_stats
            .join(self.combined_total_samples_df, on=ck, how="left")
            .with_columns([
                pl.col("n1_chars").alias("Character Node 1"),
                pl.col("n2_chars").alias("Character Node 2"),
                (pl.col("Samples") / pl.col("combined_version_total_samples") * 100).round(2).alias("Appearance Rate (%)"),
                *self._score_stats_exprs("Scores"),
            ])
            .sort(ck + ["Samples"], descending=[True, True, True])
            .with_row_index("Rank", offset=1)
        )
        return df.select(["Rank", *ck, "Character Node 1", "Character Node 2",
                          "Samples", "Appearance Rate (%)",
                          f"Min {m}", f"25th Percentile {m}", f"Median {m}",
                          f"75th Percentile {m}", f"Average {m}", f"Std Dev {m}", f"Max {m}"])

    def plot_statistics_all(self, output: bool = True):
        return self._plot_score_distribution(
            df=self.lf, score_col=self.score_col, output=output,
            title=f"Score Distribution — [{self.mode.upper()}], floor {self.floor}, node {self.node}",
        )

    def plot_statistics_all_combined(self, output: bool = True):
        return self._plot_score_distribution(
            df=self.combined, score_col="total_score", output=output,
            title=f"Combined Score Distribution — [{self.mode.upper()}], floor {self.floor}",
        )