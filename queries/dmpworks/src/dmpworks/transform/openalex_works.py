import argparse
import json
import logging
import os
import pathlib
from typing import Optional

import polars as pl
from dmpworks.transform.pipeline import process_files_parallel
from dmpworks.transform.transforms import make_page, normalise_identifier, remove_markup
from dmpworks.transform.utils_cli import add_common_args, handle_errors, validate_common_args
from dmpworks.transform.utils_file import read_jsonls, validate_directory
from polars._typing import SchemaDefinition

logger = logging.getLogger(__name__)

WORKS_SCHEMA: SchemaDefinition = {
    "id": pl.String,  # https://docs.openalex.org/api-entities/works/work-object#id
    "doi": pl.String,  # https://docs.openalex.org/api-entities/works/work-object#doi
    "ids": pl.Struct(  # https://docs.openalex.org/api-entities/works/work-object#ids
        {
            "doi": pl.String,
            "mag": pl.String,
            "openalex": pl.String,
            "pmid": pl.String,
            "pmcid": pl.String,
        }
    ),
    "title": pl.String,  # https://docs.openalex.org/api-entities/works/work-object#title
    "abstract_inverted_index": pl.String,  # https://docs.openalex.org/api-entities/works/work-object#abstract_inverted_index
    "type": pl.String,  # https://docs.openalex.org/api-entities/works/work-object#type
    "publication_date": pl.Date,  # https://docs.openalex.org/api-entities/works/work-object#publication_date
    "updated_date": pl.Datetime,  # https://docs.openalex.org/api-entities/works/work-object#updated_date
    "authorships": pl.List(  # https://docs.openalex.org/api-entities/works/work-object#authorships
        pl.Struct(
            {
                "author": pl.Struct(
                    {
                        "id": pl.String,
                        "display_name": pl.String,
                        "orcid": pl.String,
                    }
                ),
                "institutions": pl.List(
                    pl.Struct(
                        {
                            "id": pl.String,
                            "display_name": pl.String,
                            "type": pl.String,
                            "ror": pl.String,
                        }
                    )
                ),
            }
        )
    ),
    "grants": pl.List(  # https://docs.openalex.org/api-entities/works/work-object#grants
        pl.Struct(
            {
                "funder": pl.String,
                "funder_display_name": pl.String,
                "award_id": pl.String,
            }
        )
    ),
    "primary_location": pl.Struct(  # https://docs.openalex.org/api-entities/works/work-object#primary_location
        {
            "source": pl.Struct(
                {
                    "display_name": pl.String,
                    "publisher": pl.String,
                }
            )
        }
    ),
    "biblio": pl.Struct(  # https://docs.openalex.org/api-entities/works/work-object#biblio
        {
            "volume": pl.String,
            "issue": pl.String,
            "first_page": pl.String,
            "last_page": pl.String,
        }
    ),
}


def normalise_ids(expr: pl.Expr, field_names: list[str]) -> pl.Expr:
    return pl.struct(
        [normalise_identifier(expr.struct.field(field_name)).alias(field_name) for field_name in field_names]
    )


def revert_inverted_index(text: str | None) -> Optional[str]:
    if isinstance(text, str):
        try:
            data = json.loads(text)
        except json.decoder.JSONDecodeError as e:
            logging.error(f"Error invalid JSON: error={e}")
            return None

        # Build abstract
        words = []
        for word, positions in data.items():
            for pos in positions:
                if pos >= len(words):
                    words.extend([None] * (pos + 1 - len(words)))
                words[pos] = word

        # Filter out any Nones left by mistake and join
        abstract = " ".join(word for word in words if word is not None).strip()
        return abstract if abstract else None

    return None


def transform_works(lz: pl.LazyFrame) -> list[tuple[str, pl.LazyFrame]]:
    lz_cached = lz.cache()

    works = lz_cached.select(
        id=normalise_identifier(pl.col("id")),
        doi=normalise_identifier(pl.col("doi")),
        ids=normalise_ids(pl.col("ids"), ["doi", "mag", "openalex", "pmid", "pmcid"]),
        title=remove_markup(pl.col("title")),
        abstract=remove_markup(
            pl.col("abstract_inverted_index").map_elements(revert_inverted_index, return_dtype=pl.String)
        ),
        type=pl.col("type"),
        publication_date=pl.col("publication_date"),  # e.g. 2014-06-04
        updated_date=pl.col("updated_date"),  # e.g. 025-02-27T06:49:42.321119
        container_title=pl.col("primary_location").struct.field("source").struct.field("display_name"),
        volume=pl.col("biblio").struct.field("volume"),
        issue=pl.col("biblio").struct.field("issue"),
        page=make_page(
            pl.col("biblio").struct.field("first_page"),
            pl.col("biblio").struct.field("last_page"),
        ),
        publisher=pl.col("primary_location").struct.field("source").struct.field("publisher"),
        publisher_location=None,
    )

    exploded_authors = (
        lz_cached.select(
            work_id=normalise_identifier(pl.col("id")),
            work_doi=normalise_identifier(pl.col("doi")),
            authorships=pl.col("authorships"),
        )
        .explode("authorships")
        .unnest("authorships")
    )
    works_authors = exploded_authors.select(
        pl.col("work_id"),
        pl.col("work_doi"),
        author_id=normalise_identifier(pl.col("author").struct.field("id")),
        display_name=pl.col("author").struct.field("display_name"),
        orcid=normalise_identifier(pl.col("author").struct.field("orcid")),
    ).unique()
    # TODO: author order?
    # TODO: alternate author names?

    works_affiliations = (
        exploded_authors.select(
            pl.col("work_id"),
            pl.col("work_doi"),
            institutions=pl.col("institutions"),
        )
        .explode("institutions")
        .unnest("institutions")
        .select(
            pl.col("work_id"),
            pl.col("work_doi"),
            institution_id=normalise_identifier(pl.col("id")),
            display_name=pl.col("display_name"),
            type=pl.col("type"),
            ror=normalise_identifier(pl.col("ror")),
        )
        .unique()
    )

    works_funders = (
        lz_cached.select(
            work_id=normalise_identifier(pl.col("id")),
            work_doi=normalise_identifier(pl.col("doi")),
            grants=pl.col("grants"),
        )
        .explode("grants")
        .unnest("grants")
        .select(
            pl.col("work_id"),
            pl.col("work_doi"),
            funder_id=normalise_identifier(pl.col("funder")),
            funder_display_name=pl.col("funder_display_name"),
            award_id=pl.col("award_id"),
        )
        .unique()
    )

    return [
        ("openalex_works", works),
        ("openalex_works_authors", works_authors),
        ("openalex_works_affiliations", works_affiliations),
        ("openalex_works_funders", works_funders),
    ]


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
        schema=WORKS_SCHEMA,
        transform_func=transform_works,
        file_glob="**/*.gz",
        read_func=read_jsonls,
    )


def main():
    parser = argparse.ArgumentParser(description="Transform OpenAlex Works to Parquet for the DMP Tool.")
    setup_parser(parser)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
