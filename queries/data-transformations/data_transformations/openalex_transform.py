import argparse
import json
import logging
import os
import pathlib
from typing import Dict
from typing import Optional

import polars as pl
from polars._typing import SchemaDefinition

from cli import handle_errors, add_common_args, validate_common_args
from pipeline import process_files_parallel
from transformations import normalise_identifier, remove_markup, make_page
from utils import read_jsonls, validate_directory, extract_gzip

logger = logging.getLogger(__name__)

SCHEMAS: Dict[str, SchemaDefinition] = {
    "works": {
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
    },
    "funders": {
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
    },
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
            pl.col("abstract_inverted_index")
            .str.replace_all(r"\n|\r|\t|\b|\f", "")  # Remove some special characters
            .map_elements(revert_inverted_index, return_dtype=pl.String)
        ),
        type=pl.col("type"),
        publication_date=pl.col("publication_date"),
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


def transform_funders(lz: pl.LazyFrame) -> list[tuple[str, pl.LazyFrame]]:
    funders = lz.with_columns(
        [
            normalise_identifier(pl.col("id")).alias("id"),
            pl.col("display_name"),
            normalise_ids(pl.col("ids"), ["crossref", "doi", "openalex", "ror", "wikidata"]).alias("ids"),
        ]
    )

    return [("openalex_funders", funders)]


def parse_args():
    parser = argparse.ArgumentParser(description="Transform OpenAlex to Parquet for the DMP Tool.")

    # Positional arguments
    parser.add_argument(
        "in_dir",
        type=pathlib.Path,
        help="Path to the OpenAlex snapshot root directory (e.g. /path/to/openalex_snapshot)",
    )
    parser.add_argument(
        "out_dir",
        type=pathlib.Path,
        help="Path to the output directory (e.g. /path/to/openalex_transformed/works).",
    )
    parser.add_argument(
        "table_name",
        choices=["works", "funders"],
        help="Table name (must be 'works' or 'funders').",
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
    args = parser.parse_args()

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
    handle_errors(parser, errors)
    return args


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    args = parse_args()

    # Get schema
    table_name = args.table_name
    schema = SCHEMAS.get(table_name, None)
    if schema is None:
        raise ValueError(f"Schema not found for table_name={table_name}")

    # Get transform function
    transform_func = {"works": transform_works, "funders": transform_funders}.get(table_name, None)
    if transform_func is None:
        raise ValueError(f"Transform function not found for table_name={table_name}")

    table_dir = args.in_dir / "data" / table_name
    args_dict = vars(args)
    del args_dict["in_dir"]
    del args_dict["table_name"]
    process_files_parallel(
        **args_dict,
        in_dir=table_dir,
        schema=schema,
        transform_func=transform_func,
        file_glob="**/*.gz",
        read_func=read_jsonls,
    )
