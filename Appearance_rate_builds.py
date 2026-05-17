import polars as pl
import os
import orjson
import polars.selectors as cs
import duckdb
from dotenv import load_dotenv

load_dotenv()

class HonkaiStatistics_builds:
    def __init__(self, mode="all"):
        self.mode = mode
        self.folder = "raw_data"
        self.skip_versions = ["1.0.3", "1.1.3"]
        self.keys = [
            "MOC_VERSIONS", "PF_VERSIONS", "APOC_VERSIONS", 
            "PF_VERSIONS_LEGACY", "MOC_VERSIONS_LATE_LEGACY", "MOC_VERSIONS_LEGACY"
        ]
        
        # Numeric columns for weighted averages
        self.target_cols = [
            "HP", "ATK", "DEF", "SPD", "CRIT Rate", "CRIT DMG", "Break Effect",
            "SPD sub", "HP sub", "ATK sub", "DEF sub", "CRIT Rate sub", 
            "CRIT DMG sub", "Effect RES sub", "Effect Hit Rate sub", "Break Effect sub"
        ]

        # Categorical columns for usage metrics
        self.relic_slots = ["Body", "Feet", "Sphere", "Rope"]
        
        self.stats = self._build_stats()

    def _get_version_lazyframe(self, version):
        path = os.path.join(self.folder, f"{version}_build.parquet")
        if not os.path.exists(path):
            print(f"Downloading {version}...")
            url = f"https://huggingface.co/datasets/LvlUrArti/MocData/resolve/main/{version}_build.csv"
            os.makedirs(self.folder, exist_ok=True)
            temp_df = pl.read_csv(url, infer_schema_length=10000, ignore_errors=True)
            temp_df.write_parquet(path)
        
        corrupt_bullets = ["â€¢", "Ã¢â‚¬Â¢"]
        clean_bullets = ["•", "•"]

        return (
            pl.scan_parquet(path)
            # Step 1: Secure UID as a string first to separate it from numeric types
            .with_columns(pl.col("uid").cast(pl.String))
            # Step 2: Safe handling of numeric and string cleanups
            .with_columns([
                # Target numeric datatypes safely using selector exclusions
                cs.numeric().exclude("uid").cast(pl.Float64),
                    
                # String cleanups
                cs.string().str.replace_many(corrupt_bullets, clean_bullets)
                .str.replace_all(r"\band\b", "&"),
                    
                # Add version tag
                pl.lit(version).alias("version_name")
            ])
            # Step 3: Conditional Variant Mapping
            .with_columns([
                pl.when((pl.col("character") == "March 7th") & (pl.col("path") == "Preservation"))
                .then(pl.lit("Ice March 7th"))
                    
                .when((pl.col("character") == "March 7th") & (pl.col("path") == "Ice"))
                .then(pl.lit("Ice March 7th"))
                    
                .when((pl.col("character") == "March 7th") & (pl.col("path") == "Imaginary"))
                .then(pl.lit("Imaginary March 7th"))
                    
                .when((pl.col("character") == "Trailblazer") & (pl.col("path").is_in(["Physical", "Destruction"])))
                .then(pl.lit("Physical Trailblazer"))
                    
                .when((pl.col("character") == "Trailblazer") & (pl.col("path").is_in(["Fire", "Preservation"])))
                .then(pl.lit("Fire Trailblazer"))
                    
                .when((pl.col("character") == "Trailblazer") & (pl.col("path").is_in(["Imaginary", "Harmony"])))
                .then(pl.lit("Imaginary Trailblazer"))
                    
                .when((pl.col("character") == "Trailblazer") & (pl.col("path").is_in(["Ice", "Remembrance"])))
                .then(pl.lit("Ice Trailblazer"))
                    
                .when((pl.col("character") == "Trailblazer") & (pl.col("path").is_in(["Lightning", "Erudition"])))
                .then(pl.lit("Lightning Trailblazer"))
                    
                # Fallback: Keep the original value of the "character" column if no rules match
                .otherwise(pl.col("character"))
                .alias("character")
            ])
        )

    def _build_stats(self):
        lazy_frames = []
        if self.mode == "all":
            for key in self.keys:
                raw_val = os.getenv(key)
                if raw_val:
                    for v in [v.strip() for v in raw_val.split(',') if v.strip()]:
                        if v not in self.skip_versions:
                            lazy_frames.append(self._get_version_lazyframe(v))
        else:
            lazy_frames.append(self._get_version_lazyframe(self.mode))

        # 1. Combine raw data diagonally
        full_raw = pl.concat(lazy_frames, how="diagonal")
        
        # Get the actual available columns without crashing
        available_cols = full_raw.collect_schema().names()

        # 2. Usage Metrics Logic
        usage_dfs = []
        for slot in self.relic_slots:
            if slot in available_cols:
                slot_usage = (
                    full_raw.group_by(["character", slot])
                    .agg(pl.len().alias("count"))
                    .with_columns(
                        usage_pct=(pl.col("count") / pl.col("count").sum().over("character") * 100).round(1)
                    ).sort(pl.col('count'), descending=True)
                    .select([
                        "character",
                        pl.struct([
                            pl.col(slot).alias("main_stat"),
                            pl.col("count").alias("samples"),
                            pl.col("usage_pct")
                        ]).alias(f"{slot}_data")
                    ])
                    .group_by("character")
                    .agg(pl.col(f"{slot}_data"))
                )
                usage_dfs.append(slot_usage)
        
        valid_cols = [col for col in self.target_cols if col in available_cols]
        
        # 3. Numeric Stats Logic
        numeric_stats = (
            full_raw.group_by("character")
            .agg([
                pl.len().alias("total_sample_size"),
                pl.col("version_name").n_unique().alias("num_versions"),
                cs.by_name(valid_cols).mean().name.prefix("Avg_")
            ])
        )
        
        # 3.5 Extra Stats Logic (Perfectly Organized Per-Stat Dictionaries)
        extra_stats = (
            full_raw.group_by("character")
            .agg([
                cs.by_name(valid_cols).min().name.prefix("min_"),
                cs.by_name(valid_cols).quantile(0.05).name.prefix("p05_"),
                cs.by_name(valid_cols).quantile(0.25).name.prefix("p25_"),
                cs.by_name(valid_cols).quantile(0.50).name.prefix("p50_"),
                cs.by_name(valid_cols).quantile(0.75).name.prefix("p75_"),
                cs.by_name(valid_cols).quantile(0.95).name.prefix("p95_"),
                cs.by_name(valid_cols).max().name.prefix("max_"),
                cs.by_name(valid_cols).std().name.prefix("std_"),
            ])
            .select([
                "character",
                pl.struct([
                    pl.struct([
                        pl.col(f"min_{col}").alias("min"),
                        pl.col(f"p05_{col}").alias("p05"),
                        pl.col(f"p25_{col}").alias("p25"),
                        pl.col(f"p50_{col}").alias("p50"),
                        pl.col(f"p75_{col}").alias("p75"),
                        pl.col(f"p95_{col}").alias("p95"),
                        pl.col(f"max_{col}").alias("max"),
                        pl.col(f"std_{col}").alias("std"),
                    ]).alias(col)
                    for col in valid_cols
                ]).alias("extra_stats")
            ])
        )

        # 4. Final Join
        final_df = numeric_stats
        
        # Join the relic usage frames
        for usage in usage_dfs:
            final_df = final_df.join(usage, on="character", how="left")
            
        # Join the new extra_stats struct frame
        final_df = final_df.join(extra_stats, on="character", how="left")
        
        return final_df.sort("total_sample_size", descending=True).collect()

    def save_to_db(self, table_name=None):
        """
        Saves the processed self.stats DataFrame directly into DuckDB.
        Automatically defaults table names based on context if none specified.
        """
        db_path = os.getenv("DB_File")
        if not db_path:
            raise ValueError("Environment variable 'DB_File' is not specified in your configuration.")

        if table_name is None:
            if self.mode == "all":
                table_name = "character_builds_all_versions"
            else:
                # Sanitizes specific version formats (e.g., '2.3.3' to 'character_builds_v2_3_3')
                safe_ver = str(self.mode).replace(".", "_")
                table_name = f"character_builds_v{safe_ver}"

        print(f"Connecting to DuckDB instance: {db_path}")
        # Establish connection to your targeted DuckDB storage instance
        conn = duckdb.connect(db_path)
        
        try:
            # Reference the data directly. DuckDB handles Polars List and Struct types 
            # natively without needing to cast them to strings!
            export_df = self.stats
            
            # Atomically replace or create the table with the proper nested types
            print(f"Writing data to table '{table_name}'...")
            conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM export_df")
            
            # Fetch confirmation counts
            row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"Success: Updated '{table_name}' with {row_count} entries.")
            
        except Exception as e:
            print(f"Error executing database ingestion: {e}")
            raise e
        finally:
            conn.close()