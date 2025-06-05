import argparse
import logging
import pathlib

import polars
import polars as pl
from dmpworks.transform.transforms import normalise_identifier, normalise_isni
from polars._typing import SchemaDefinition

SCHEMA: SchemaDefinition = {
    "id": pl.String,
    # "names": pl.List(pl.Struct({"value": pl.String, "types": pl.List(pl.String), "lang": pl.String})),
    # "domains": pl.List(pl.String),
    "external_ids": pl.List(pl.Struct({"type": pl.String, "all": pl.List(pl.String), "preferred": pl.String})),
}


def load_ror(ror_v2_json_file: pathlib.Path):
    return pl.read_json(ror_v2_json_file, schema=SCHEMA)


def create_ror_index(ror_df: pl.DataFrame) -> pl.DataFrame:
    return (
        ror_df.select(ror_id=normalise_identifier(pl.col("id")), external_ids=pl.col("external_ids"))
        .explode("external_ids")
        .unnest("external_ids")
        .select(pl.col("ror_id"), type=pl.col("type"), identifier=pl.col("all"))
        .explode("identifier")
        .filter(pl.col("type").is_not_null() | polars.col("identifier").is_not_null())
        .select(
            pl.col("ror_id"),
            type=pl.col("type"),
            identifier=pl.when(pl.col("type") == "isni")  # Clean ISNIs
            .then(normalise_isni(pl.col("identifier")))
            .when(pl.col("type") == "fundref")  # Add 10.13039 prefix to Fundref IDs
            .then(pl.concat_str([pl.lit("10.13039/"), pl.col("identifier").str.strip_chars().str.to_lowercase()]))
            .otherwise(pl.col("identifier").str.strip_chars().str.to_lowercase()),
        )
    )


def setup_parser(parser: argparse.ArgumentParser) -> None:
    # Positional arguments
    parser.add_argument(
        "ror_v2_json_file",
        type=pathlib.Path,
        help="Path to the ROR V2 (e.g. /path/to/v1.63-2025-04-03-ror-data_schema_v2.json)",
    )
    parser.add_argument(
        "out_dir",
        type=pathlib.Path,
        help="Path to the output directory (e.g. /path/to/ror_transformed).",
    )

    # Callback function
    parser.set_defaults(func=handle_command)


def handle_command(args: argparse.Namespace):
    logging.basicConfig(level=logging.DEBUG)

    df_ror = load_ror(args.ror_v2_json_file)
    df_ror_index = create_ror_index(df_ror)

    out = args.out_dir / "ror.parquet"
    df_ror_index.write_parquet(out, compression="snappy")


def main():
    parser = argparse.ArgumentParser(description="Transform ROR to Parquet for the DMP Tool.")
    setup_parser(parser)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
