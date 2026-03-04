from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert NYC taxi parquet to monitor JSONL schema")
    parser.add_argument(
        "--input-glob",
        default="data/raw/nyc_taxi/yellow_tripdata_2024-*.parquet",
        help="Input parquet glob",
    )
    parser.add_argument("--output", default="data/events_nyc_taxi.jsonl", help="Output JSONL path")
    parser.add_argument("--rows", type=int, default=25000, help="Number of rows to export")
    args = parser.parse_args()

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    lf = (
        pl.scan_parquet(args.input_glob)
        .with_row_index("row_nr")
        .with_columns(
            [
                # Intentionally coarse ID to surface duplicate trends in monitoring.
                pl.concat_str(
                    [
                        pl.col("VendorID").cast(pl.String),
                        pl.lit("_"),
                        pl.col("tpep_pickup_datetime").dt.strftime("%Y%m%d%H%M"),
                        pl.lit("_"),
                        pl.col("PULocationID").cast(pl.String),
                        pl.lit("_"),
                        pl.col("DOLocationID").cast(pl.String),
                    ]
                ).alias("event_id"),
                pl.col("tpep_pickup_datetime").dt.strftime("%Y-%m-%dT%H:%M:%SZ").alias("timestamp"),
                pl.lit("nyc_taxi").alias("source"),
                pl.col("DOLocationID").cast(pl.String).fill_null("unknown").alias("user_id"),
                pl.when(
                    (pl.col("trip_distance") <= 0)
                    | (pl.col("fare_amount") <= 0)
                    | (pl.col("total_amount") <= 0)
                )
                .then(pl.lit("error"))
                .otherwise(pl.lit("ok"))
                .alias("status"),
            ]
        )
        .select(["event_id", "timestamp", "source", "user_id", "status"])
        .limit(args.rows)
    )

    df = lf.collect(streaming=True)
    df.write_ndjson(out_path)
    print(f"wrote_rows={df.height} output={out_path}")


if __name__ == "__main__":
    main()
