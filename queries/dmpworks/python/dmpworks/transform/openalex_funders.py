import argparse
import logging
import os
import pathlib

import polars as pl
from dmpworks.transform.openalex_works import normalise_ids
from dmpworks.transform.pipeline import process_files_parallel
from dmpworks.transform.transforms import normalise_identifier
from dmpworks.transform.utils_cli import add_common_args, copy_dict, handle_errors, validate_common_args
from dmpworks.transform.utils_file import read_jsonls, setup_multiprocessing_logging
from polars._typing import SchemaDefinition

logger = logging.getLogger(__name__)

FUNDERS_SCHEMA: SchemaDefinition = {
    "id": pl.String,  # https://docs.openalex.org/api-entities/funders/funder-object#id
    "display_name": pl.String,  # https://docs.openalex.org/api-entities/funders/funder-object#display_name
    "ids": pl.Struct(  # https://docs.openalex.org/api-entities/funders/funder-object#ids
        {
            "crossref": pl.String,
            "doi": pl.String,
            "openalex": pl.String,
            "ror": pl.String,
            "wikidata": pl.String,
        }
    ),
}


def transform_funders(lz: pl.LazyFrame) -> list[tuple[str, pl.LazyFrame]]:
    funders = lz.select(
        id=normalise_identifier(pl.col("id")),
        display_name=pl.col("display_name"),
        ids=normalise_ids(pl.col("ids"), ["crossref", "doi", "openalex", "ror", "wikidata"]),
        # TODO: remove 'entity/' from start of wikidata
    )

    return [("openalex_funders", funders)]


def setup_parser(parser: argparse.ArgumentParser) -> None:
    # Positional arguments
    parser.add_argument(
        "in_dir",
        type=pathlib.Path,
        help="Path to the OpenAlex funders directory (e.g. /path/to/openalex_snapshot/data/works)",
    )
    parser.add_argument(
        "out_dir",
        type=pathlib.Path,
        help="Path to the output directory (e.g. /path/to/parquets/openalex_funders).",
    )

    # Common keyword arguments
    add_common_args(
        parser=parser,
        batch_size=os.cpu_count(),
        extract_workers=1,
        transform_workers=1,
        cleanup_workers=1,
        extract_queue_size=0,
        transform_queue_size=2,
        cleanup_queue_size=0,
        max_file_processes=os.cpu_count(),
        n_batches=None,
    )

    # Callback function
    parser.set_defaults(func=handle_command)


def handle_command(args: argparse.Namespace):
    setup_multiprocessing_logging(logging.getLevelName(args.log_level))

    # Validate
    errors = []
    if not args.in_dir.is_dir():
        errors.append(f"in_dir '{args.in_dir}' is not a valid directory.")

    if not args.out_dir.is_dir():
        errors.append(f"out_dir '{args.out_dir}' is not a valid directory.")

    validate_common_args(args, errors)
    handle_errors(errors)

    process_files_parallel(
        **copy_dict(vars(args), ["command", "transform_command", "func"]),
        schema=FUNDERS_SCHEMA,
        transform_func=transform_funders,
        file_glob="**/*.gz",
        read_func=read_jsonls,
    )


def main():
    parser = argparse.ArgumentParser(description="Transform OpenAlex Funders to Parquet for the DMP Tool.")
    setup_parser(parser)
    args = parser.parse_args()
    args.func(args)
