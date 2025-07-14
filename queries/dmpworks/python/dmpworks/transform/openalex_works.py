import logging
import os
import pathlib

import dmpworks.polars_expr_plugin as pe
import polars as pl
from dmpworks.transform.pipeline import process_files_parallel
from dmpworks.transform.transforms import clean_string, make_page, normalise_identifier
from dmpworks.transform.utils_file import read_jsonls
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


def transform_works(lz: pl.LazyFrame) -> list[tuple[str, pl.LazyFrame]]:
    lz_cached = lz.cache()

    works = lz_cached.select(
        id=normalise_identifier(pl.col("id")),
        doi=normalise_identifier(pl.col("doi")),
        ids=normalise_ids(pl.col("ids"), ["doi", "mag", "openalex", "pmid", "pmcid"]),
        title=clean_string(pl.col("title")),
        abstract=clean_string(pe.revert_inverted_index(pl.col("abstract_inverted_index"))),
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


def transform_openalex_works(
    in_dir: pathlib.Path,
    out_dir: pathlib.Path,
    batch_size: int = os.cpu_count(),
    extract_workers: int = 1,
    transform_workers: int = 1,
    cleanup_workers: int = 1,
    extract_queue_size: int = 0,
    transform_queue_size: int = 2,
    cleanup_queue_size: int = 0,
    max_file_processes: int = os.cpu_count(),
    n_batches: int = None,
    low_memory: bool = False,
):
    process_files_parallel(
        # Non customizable parameters, specific to Crossref Metadata
        schema=WORKS_SCHEMA,
        transform_func=transform_works,
        file_glob="**/*.gz",
        read_func=read_jsonls,
        # Customisable parameters
        in_dir=in_dir,
        out_dir=out_dir,
        batch_size=batch_size,
        extract_workers=extract_workers,
        transform_workers=transform_workers,
        cleanup_workers=cleanup_workers,
        extract_queue_size=extract_queue_size,
        transform_queue_size=transform_queue_size,
        cleanup_queue_size=cleanup_queue_size,
        max_file_processes=max_file_processes,
        n_batches=n_batches,
        low_memory=low_memory,
    )
