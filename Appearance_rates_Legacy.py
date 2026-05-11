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
        lf      = self.df.lazy()
        char_lf = self.char_df.lazy()

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
