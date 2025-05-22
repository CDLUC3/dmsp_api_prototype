import argparse
import logging
import os
import pathlib

import polars as pl
from polars._typing import SchemaDefinition

from cli import handle_errors, add_common_args, validate_common_args
from pipeline import process_files_parallel
from transformations import remove_markup, normalise_identifier, date_parts_to_date
from utils import read_jsonls, validate_directory, extract_gzip

logger = logging.getLogger(__name__)

# https://www.crossref.org/documentation/schema-library/markup-guide-metadata-segments/relationships/
# Just including types that might be useful for finding dataset relations
RELATION_SCHEMA = pl.Struct({"id": pl.String, "id-type": pl.String, "asserted-by": pl.String})
RELATION_TYPES = [
    # "is-expression-of",
    # "has-expression",
    # "is-format-of",
    # "has-format",
    # "is-identical-to",
    # "is-manifestation-of",
    # "has-manifestation",
    # "is-manuscript-of",
    # "has-manuscript",
    # "is-preprint-of",
    # "has-preprint",
    # "is-replaced-by",
    # "replaces",
    # "is-translation-of",
    # "has-translation",
    # "is-variant-form-of",
    # "is-original-form-of",
    # "is-version-of",
    # "has-version",
    "is-based-on",  # dataset
    "is-basis-for",  # dataset
    # "is-comment-on",
    # "has-comment",
    # "is-continued-by",
    # "continues",
    "is-derived-from",  # dataset
    # "has-derivation",
    "is-documented-by",  # dataset
    "documents",  # dataset
    # "finances",
    # "is-financed-by",
    # "is-part-of",
    # "has-part",
    # "is-review-of",
    # "has-review",
    "references",  # dataset
    "is-referenced-by",  # dataset
    "is-related-material",  # dataset
    "has-related-material",  # dataset
    # "is-reply-to",
    # "has-reply",
    # "requires",
    # "is-required-by",
    # "is-compiled-by",
    "compiles",  # dataset
    "is-supplement-to",  # dataset
    "is-supplemented-by",  # dataset
]

SCHEMA: SchemaDefinition = {
    "DOI": pl.String,
    "type": pl.String,  # TODO: convert types?
    "title": pl.List(pl.String),
    "abstract": pl.String,
    "author": pl.List(
        pl.Struct(
            {
                "given": pl.String,
                "family": pl.String,
                "name": pl.String,
                "ORCID": pl.String,
                "affiliation": pl.List(
                    pl.Struct(
                        {
                            "name": pl.String,
                            "id": pl.List(RELATION_SCHEMA),
                        }
                    )
                ),
            }
        )
    ),
    "funder": pl.List(
        pl.Struct(
            {
                "DOI": pl.String,
                "name": pl.String,
                "award": pl.List(pl.String),
            }
        )
    ),
    "container-title": pl.List(pl.String),
    "volume": pl.String,
    "issue": pl.String,
    "page": pl.String,
    "publisher": pl.String,
    "publisher-location": pl.String,
    "issued": pl.Struct({"date-parts": pl.List(pl.List(pl.Int64))}),
    "deposited": pl.Struct(  # https://github.com/crossref/rest-api-doc/blob/master/api_format.md see deposited
        {"date-time": pl.String}
    ),
    "relation": pl.Struct({relation_type: pl.List(RELATION_SCHEMA) for relation_type in RELATION_TYPES}),
}

def transform(lz: pl.LazyFrame) -> list[tuple[str, pl.LazyFrame]]:
    lz_cached = lz.cache()

    works = (
        lz_cached.select(
            doi=pl.col("DOI"),
            title=remove_markup(pl.col("title").list.join(" ")),
            abstract=remove_markup(pl.col("abstract")),
            type=pl.col("type"),
            publication_date=date_parts_to_date(
                pl.col("issued").struct.field("date-parts").list.get(0, null_on_oob=True)
            ),
            updated_date=pl.col("deposited")
            .struct.field("date-time")
            .str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%SZ"),  # E.g. "2019-04-12T00:53:45Z"
            container_title=pl.col("container-title").list.join(" "),
            volume=pl.col("volume"),
            issue=pl.col("issue"),
            page=pl.col("page"),
            publisher=pl.col("publisher"),
            publisher_location=pl.col("publisher-location"),
        ),
    )

    exploded_authors = (
        lz_cached.select(work_doi=pl.col("DOI"), author=pl.col("author")).explode("author").unnest("author")
    )

    works_authors = exploded_authors.select(
        pl.col("work_doi"),
        given=pl.col("given"),
        family=pl.col("family"),
        name=pl.col("name"),
        orcid=normalise_identifier(pl.col("ORCID")),
    ).unique()

    # TODO: convert these IDs to ROR
    works_affiliations = (
        exploded_authors.select(pl.col("work_doi"), affiliation=pl.col("affiliation"))
        .explode("affiliation")
        .unnest("affiliation")
        .select(pl.col("work_doi"), name=pl.col("name"), id=pl.col("id"))
        .explode("id")
        .unnest("id")
        .select(
            pl.col("work_doi"),
            name=pl.col("name"),
            affiliation_id=normalise_identifier(pl.col("id")),
            id_type=pl.col("id-type"),
            asserted_by=pl.col("asserted-by"),
        )
        .unique()
    )

    works_funders = (
        lz_cached.select(work_doi=pl.col("DOI"), funder=pl.col("funder"))
        .explode("funder")
        .unnest("funder")
        .select(pl.col("work_doi"), name=pl.col("name"), funder_doi=pl.col("DOI"), award=pl.col("award"))
        .explode("award")  # Creates a new row for each element in the award list
        .unique()
    )

    works_relations = (
        lz_cached.select(work_doi=pl.col("DOI"), relation=pl.col("relation"))
        .unnest("relation")
        .unpivot(
            index="work_doi",
            variable_name="type",
            value_name="values",
        )
        .explode("values")
        .unnest("values")
        .select(
            pl.col("work_doi"),
            relation_type=pl.col("type"),
            relation_id=normalise_identifier(pl.col("id")),
            id_type=pl.col("id-type"),
            asserted_by=pl.col("asserted-by"),
        )
        .unique()
    )

    return [
        ("crossref_works", works),
        ("crossref_works_authors", works_authors),
        ("crossref_works_affiliations", works_affiliations),
        ("crossref_works_funders", works_funders),
        ("crossref_works_relations", works_relations),
    ]


def parse_args():
    parser = argparse.ArgumentParser(description="Transform Crossref Metadata to Parquet for the DMP Tool.")

    # Positional arguments
    parser.add_argument(
        "in_dir",
        type=pathlib.Path,
        help="Path to the input Crossref Metadata directory (e.g., /path/to/March 2025 Public Data File from Crossref).",
    )
    parser.add_argument(
        "out_dir",
        type=pathlib.Path,
        help="Path to the output directory for transformed Parquet files (e.g. /path/to/crossref_transformed).",
    )

    # Common keyword arguments
    add_common_args(
        parser=parser,
        batch_size=os.cpu_count(),
        extract_workers=1,
        transform_workers=2,
        cleanup_workers=1,
        extract_queue_size=0,
        transform_queue_size=3,
        cleanup_queue_size=0,
        max_file_processes=os.cpu_count(),
        n_batches=None,
    )
    args = parser.parse_args()

    # Validate
    errors = []
    if not args.in_dir.is_dir() and not validate_directory(args.in_dir, ["0.jsonl.gz", "1.jsonl.gz", "2.jsonl.gz"]):
        errors.append(f"in_dir '{args.in_dir}' is not a valid directory.")

    if not args.out_dir.is_dir():
        errors.append(f"out_dir '{args.out_dir}' is not a valid directory.")

    validate_common_args(args, errors)
    handle_errors(parser, errors)

    return args


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    args = parse_args()
    process_files_parallel(
        **vars(args),
        schema=SCHEMA,
        transform_func=transform,
        file_glob="*.jsonl.gz",
        read_func=read_jsonls,
        extract_func=extract_gzip,
    )
