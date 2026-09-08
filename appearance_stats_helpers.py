"""Shared polars expression builders for the appearance-rate batch dashboards.

Every Appearance_rate_*_batch script (MOC, Anomaly, Apocalyptic Shadow, Pure
Fiction, by-cost) and Appearance_rates_Legacy compute the same per-row summary
stats — min / max / median / percentiles / average / std — plus the rate
columns (Appearance Rate %, Usage %, Samples, sustain & full-clear rates) for
teams, archetypes, characters, duos and gear.  These helpers define those
expressions once so the display functions stop repeating them.
"""

import polars as pl


def list_stats_exprs(col: str, label: str, style: str = "full") -> list[pl.Expr]:
    """Standard distribution-stat expressions for a List column.

    style="full"  -> Min {label}, 25th Percentile {label}, Median {label},
                     75th Percentile {label}, Average {label},
                     Std Dev {label}, Max {label}
    style="short" -> Min {label}, 25th %, Median, 75th %, Avg {label},
                     Max {label}, Std Dev {label}
    style="bare"  -> Min, 25th Percentile, Median, 75th Percentile,
                     Average {label}, Std Dev, Max
    """
    if style == "short":
        return [
            pl.col(col).list.min().alias(f"Min {label}"),
            pl.col(col).list.eval(pl.element().quantile(0.25)).list.first().round(2).alias("25th %"),
            pl.col(col).list.median().round(2).alias("Median"),
            pl.col(col).list.eval(pl.element().quantile(0.75)).list.first().round(2).alias("75th %"),
            pl.col(col).list.mean().round(2).alias(f"Avg {label}"),
            pl.col(col).list.max().alias(f"Max {label}"),
            pl.col(col).list.eval(pl.element().std(ddof=1)).list.first().round(2).alias(f"Std Dev {label}"),
            pl.col(col).list.eval(
                            pl.element()
                            .value_counts(sort=True)
                            .struct.rename_fields([f"Scores", "count"])
                        ).alias(f"Scores Distributions")
        ]
    if style == "bare":
        return [
            pl.col(col).list.min().alias("Min"),
            pl.col(col).list.eval(pl.element().quantile(0.25)).list.first().round(2).alias("25th Percentile"),
            pl.col(col).list.median().round(2).alias("Median"),
            pl.col(col).list.eval(pl.element().quantile(0.75)).list.first().round(2).alias("75th Percentile"),
            pl.col(col).list.mean().round(2).alias(f"Average {label}"),
            pl.col(col).list.eval(pl.element().std(ddof=1)).list.first().round(2).alias("Std Dev"),
            pl.col(col).list.max().alias("Max"),
            pl.col(col).list.eval(
                            pl.element()
                            .value_counts(sort=True)
                            .struct.rename_fields([f"Scores", "count"])
                        ).alias(f"Scores Distributions")
        ]
    return [
        pl.col(col).list.min().alias(f"Min {label}"),
        pl.col(col).list.eval(pl.element().quantile(0.25)).list.first().round(2).alias(f"25th Percentile {label}"),
        pl.col(col).list.median().round(2).alias(f"Median {label}"),
        pl.col(col).list.eval(pl.element().quantile(0.75)).list.first().round(2).alias(f"75th Percentile {label}"),
        pl.col(col).list.mean().round(2).alias(f"Average {label}"),
        pl.col(col).list.eval(pl.element().std(ddof=1)).list.first().round(2).alias(f"Std Dev {label}"),
        pl.col(col).list.max().alias(f"Max {label}"),
        pl.col(col).list.eval(
                pl.element()
                .value_counts(sort=True)
                .struct.rename_fields([f"Scores", "count"])
            ).alias(f"Scores Distributions")
        ]


def gear_stats_exprs(col: str, label: str, style: str = "snake") -> list[pl.Expr]:
    """Gear-item stats: Avg / Median / Min / Max / Std + 25th & 75th percentiles.

    style="snake"       -> Avg_{label}, Median_{label}, Min_{label}, Max_{label},
                           Std_{label}, 25th Percentile {label}, 75th Percentile {label}
                           (median & percentiles unrounded — MOC/APOC/PF/anomaly batch)
    style="legacy"      -> Avg_{label}, 25th Percentile {label} (rounded),
                           Median_{label} (rounded), 75th Percentile {label} (rounded),
                           Min_{label}, Max_{label}, Std_{label} (rounded) — Legacy batch
    style="legacy_bare" -> Avg {label}, Median, Min_{label}, Max_{label}, Std,
                           25th Percentile, 75th Percentile — Legacy class 1
    """
    if style == "legacy":
        return [
            pl.col(col).list.mean().round(2).alias(f"Avg_{label}"),
            pl.col(col).list.eval(pl.element().quantile(0.25)).list.first().round(2).alias(f"25th Percentile {label}"),
            pl.col(col).list.median().round(2).alias(f"Median_{label}"),
            pl.col(col).list.eval(pl.element().quantile(0.75)).list.first().round(2).alias(f"75th Percentile {label}"),
            pl.col(col).list.min().alias(f"Min_{label}"),
            pl.col(col).list.max().alias(f"Max_{label}"),
            pl.col(col).list.std(ddof=1).round(2).alias(f"Std_{label}"),
            pl.col(col).list.eval(
                            pl.element()
                            .value_counts(sort=True)
                            .struct.rename_fields([f"Scores", "count"])
                        ).alias(f"Scores Distributions")
        ]
    if style == "legacy_bare":
        return [
            pl.col(col).list.mean().round(2).alias(f"Avg {label}"),
            pl.col(col).list.median().alias("Median"),
            pl.col(col).list.min().alias(f"Min_{label}"),
            pl.col(col).list.max().alias(f"Max_{label}"),
            pl.col(col).list.std().round(2).alias("Std"),
            pl.col(col).list.eval(pl.element().quantile(0.25)).list.first().alias("25th Percentile"),
            pl.col(col).list.eval(pl.element().quantile(0.75)).list.first().alias("75th Percentile"),
            pl.col(col).list.eval(
                            pl.element()
                            .value_counts(sort=True)
                            .struct.rename_fields([f"Scores", "count"])
                        ).alias(f"Scores Distributions")
        ]
    return [
        pl.col(col).list.mean().round(2).alias(f"Avg_{label}"),
        pl.col(col).list.median().alias(f"Median_{label}"),
        pl.col(col).list.min().alias(f"Min_{label}"),
        pl.col(col).list.max().alias(f"Max_{label}"),
        pl.col(col).list.std().round(2).alias(f"Std_{label}"),
        pl.col(col).list.eval(pl.element().quantile(0.25)).list.first().alias(f"25th Percentile {label}"),
        pl.col(col).list.eval(pl.element().quantile(0.75)).list.first().alias(f"75th Percentile {label}"),
        pl.col(col).list.eval(
                        pl.element()
                        .value_counts(sort=True)
                        .struct.rename_fields([f"Scores", "count"])
                    ).alias(f"Scores Distributions")
    ]


def appearance_rate_expr(samples_col: str, total, ndigits: int = 2,
                         name: str = "Appearance Rate (%)") -> pl.Expr:
    """(samples / total * 100) rounded to `ndigits`, aliased `name`.

    `total` may be a column name (str) or a scalar (e.g. Legacy's
    `self.total_samples`).
    """
    total_expr = pl.col(total) if isinstance(total, str) else total
    return (pl.col(samples_col) / total_expr * 100).round(ndigits).alias(name)


def usage_rate_expr(samples_col: str, total, ndigits: int = 2) -> pl.Expr:
    """(samples / total * 100) aliased 'Usage %'."""
    return appearance_rate_expr(samples_col, total, ndigits=ndigits, name="Usage %")


def sustain_pct_expr(sustain_col: str, samples_col: str,
                     name: str = "Sustain_Percentage") -> pl.Expr:
    """(sustains / samples * 100) aliased `name` (default 'Sustain_Percentage')."""
    return (pl.col(sustain_col) / pl.col(samples_col) * 100).round(2).alias(name)


def full_clear_rate_expr(full_clear_col: str, samples_col: str,
                         name: str = "Full_Clear_Rate") -> pl.Expr:
    """(full_clears / samples * 100) aliased `name` (default 'Full_Clear_Rate')."""
    return (pl.col(full_clear_col) / pl.col(samples_col) * 100).round(2).alias(name)


def sustain_flag_expr(sustain_col: str, samples_col: str, name: str = "Sustain?") -> pl.Expr:
    """Boolean 'is every sample sustained?' flag (default alias 'Sustain?')."""
    return (pl.col(sustain_col) == pl.col(samples_col)).alias(name)


def eidolon_pct_exprs(sample_cols: list[str], total_col: str = "Total_Samples") -> list[pl.Expr]:
    """Per-eidolon share of samples, e.g. 'Eidolon 1.0 %'."""
    return [
        ((pl.col(c) / pl.col(total_col)) * 100).round(2).alias(f"{c.replace('Samples_', '')} %")
        for c in sample_cols
    ]