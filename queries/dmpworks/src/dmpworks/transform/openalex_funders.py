import argparse
import logging
import os
import pathlib

import polars as pl
from dmpworks.transform.openalex_works import normalise_ids
from dmpworks.transform.pipeline import process_files_parallel
from dmpworks.transform.transforms import normalise_identifier
from dmpworks.transform.utils_cli import add_common_args, handle_errors, validate_common_args
from dmpworks.transform.utils_file import read_jsonls, validate_directory
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
        help="Path to the OpenAlex snapshot root directory (e.g. /path/to/openalex_snapshot)",
    )
    parser.add_argument(
        "out_dir",
        type=pathlib.Path,
        help="Path to the output directory (e.g. /path/to/parquets/openalex_works).",
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
    logging.basicConfig(level=logging.DEBUG)

    # Validate
    errors = []
    if not args.in_dir.is_dir() and not validate_directory(
        args.in_dir,
        ["data", "browse.html", "LICENSE.txt", "README.txt", "RELEASE_NOTES.txt"],
    ):
        errors.append(f"in_dir '{args.in_dir}' is not a valid directory.")

    if not args.out_dir.is_dir():
        errors.append(f"out_dir '{args.out_dir}' is not a valid directory.")

    validate_common_args(args, errors)
    handle_errors(errors)

    table_dir = args.in_dir / "data" / "works"
    args_dict = vars(args)
    del args_dict["in_dir"]
    # del args_dict["table_name"]
    process_files_parallel(
        **args_dict,
        in_dir=table_dir,
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
