import argparse
import json
import logging
import os
import pathlib

import polars as pl
from polars import Date
from polars._typing import SchemaDefinition

from cli import handle_errors, add_common_args, validate_common_args
from pipeline import process_files_parallel
from transformations import make_page, remove_markup, extract_orcid
from utils import read_jsonls, validate_directory, extract_gzip

logger = logging.getLogger(__name__)


AFFILIATION_SCHEMA = pl.List(
    pl.Struct(
        {
            "name": pl.String,
            "affiliationIdentifier": pl.String,
            "affiliationIdentifierScheme": pl.String,
            "schemeUri": pl.String,
        }
    )
)
NAME_IDENTIFIERS_SCHEMA = pl.List(
    pl.Struct(
        {
            "nameIdentifier": pl.String,
            "nameIdentifierScheme": pl.String,
            "nameIdentifierSchemeUri": pl.String,
        }
    )
)
CREATOR_OR_CONTRIBUTOR = pl.Struct(
    {
        "givenName": pl.String,
        "familyName": pl.String,
        "name": pl.String,
        "nameType": pl.String,
        "affiliation": pl.String,  # Should all be lists, however some of these are objects
        # "affiliation": pl.List(
        #     pl.Struct(
        #         {
        #             "name": pl.String,
        #             "affiliationIdentifier": pl.String,
        #             "affiliationIdentifierScheme": pl.String,
        #             "schemeUri": pl.String,
        #         }
        #     )
        # ),
        "nameIdentifiers": pl.String,  # Should all be lists, however some of these are objects
        # "nameIdentifiers": pl.List(
        #     pl.Struct(
        #         {
        #             "nameIdentifier": pl.String,
        #             "nameIdentifierScheme": pl.String,
        #             "nameIdentifierSchemeUri": pl.String,
        #         }
        #     )
        # ),
    }
)

SCHEMA: SchemaDefinition = {
    "id": pl.String,
    "attributes": pl.Struct(
        {
            "created": pl.String,  # ISO 8601 string
            "titles": pl.List(pl.Struct({"title": pl.String})),
            "descriptions": pl.List(pl.Struct({"description": pl.String})),
            "types": pl.Struct({"resourceTypeGeneral": pl.String}),
            "container": pl.Struct(
                {
                    "title": pl.String,
                    "volume": pl.String,
                    "issue": pl.String,
                    "firstPage": pl.String,
                    "lastPage": pl.String,
                }
            ),
            "publisher": pl.Struct({"name": pl.String}),
            "creators": pl.List(CREATOR_OR_CONTRIBUTOR),
            # "contributors": pl.List(CREATOR_OR_CONTRIBUTOR),
            "fundingReferences": pl.List(
                pl.Struct(
                    {
                        "funderIdentifier": pl.String,
                        "funderIdentifierType": pl.String,
                        "funderName": pl.String,
                        "awardNumber": pl.String,
                        "awardUri": pl.String,
                    }
                )
            ),
            "relatedIdentifiers": pl.List(  # https://support.datacite.org/docs/connecting-to-works
                pl.Struct(
                    {"relationType": pl.String, "relatedIdentifier": pl.String, "relatedIdentifierType": pl.String}
                )
            ),
        }
    ),
}


def parse_datacite_list(text: str | None) -> list[dict]:
    if text is None:
        return []

    try:
        data = json.loads(text)
    except json.decoder.JSONDecodeError as e:
        logging.error(f"Error invalid JSON: text={text}, error={e}")
        return []

    if isinstance(data, dict):
        return [data]
    elif isinstance(data, list):
        return data
    else:
        return []


def transform(lz: pl.LazyFrame) -> list[tuple[str, pl.LazyFrame]]:
    lz_cached = lz.cache()

    works = lz_cached.with_columns(
        doi=pl.col("id"),
        title=remove_markup(
            pl.col("attributes").struct.field("titles").list.eval(pl.element().struct.field("title")).list.join(" ")
        ),
        abstract=remove_markup(
            pl.col("attributes")
            .struct.field("descriptions")
            .list.eval(pl.element().struct.field("description"))
            .list.join(" ")
        ),
        type=pl.col("attributes").struct.field("types").struct.field("resourceTypeGeneral"),
        publication_date=pl.col("attributes")
        .struct.field("created")
        .str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%SZ")
        .cast(Date),  # E.g. 2018-05-06T17:23:29Z
        container_title=pl.col("attributes").struct.field("container").struct.field("title"),
        volume=pl.col("attributes").struct.field("container").struct.field("volume"),
        issue=pl.col("attributes").struct.field("container").struct.field("issue"),
        page=make_page(
            pl.col("attributes").struct.field("container").struct.field("firstPage"),
            pl.col("attributes").struct.field("container").struct.field("lastPage"),
        ),
        publisher=pl.col("attributes").struct.field("publisher"),
        publisher_location=None,
    )

    # TODO: author order?
    exploded_authors = (
        lz_cached.select(work_doi=pl.col("id"), author=pl.col("attributes").struct.field("creators"))
        .explode("author")
        .unnest("author")
        .select(
            pl.col("work_doi"),
            given_name=pl.col("givenName"),
            family_name=pl.col("familyName"),
            name=pl.col("name"),
            name_type=pl.col("nameType"),
            affiliation=pl.col("affiliation").map_elements(parse_datacite_list, return_dtype=AFFILIATION_SCHEMA),
            name_identifiers=pl.col("nameIdentifiers").map_elements(
                parse_datacite_list, return_dtype=NAME_IDENTIFIERS_SCHEMA
            ),
        )
    )

    # extract and clean ORCIDs
    works_authors = exploded_authors.select(
        pl.col("work_doi"),
        pl.col("given_name"),
        pl.col("family_name"),
        pl.col("name"),
        pl.col("name_type"),
        orcid=extract_orcid(
            pl.col("name_identifiers")
            .filter(pl.element().struct.field("nameIdentifierScheme").str.contains("(?i)orc"))
            .list.get(0, null_on_oob=True)
        ),
    )

    # TODO: convert to RORs?
    works_affiliations = (
        exploded_authors.select(pl.col("work_doi"), name_identifiers=pl.col("name_identifiers"))
        .explode("name_identifiers")
        .unnest("name_identifiers")
        .select(
            pl.col("work_doi"),
            name_identifier=pl.col("nameIdentifier"),
            name_identifier_scheme=pl.col("nameIdentifierScheme"),
            name_identifier_scheme_uri=pl.col("nameIdentifierSchemeUri"),
        )
    )

    works_funders = (
        lz_cached.select(
            work_doi=pl.col("id"), fundingReferences=pl.col("attributes").struct.field("fundingReferences")
        )
        .explode("fundingReferences")
        .unnest("fundingReferences")
        .select(
            pl.col("work_doi"),
            funder_identifier=pl.col("funderIdentifier"),
            funder_identifier_type=pl.col("funderIdentifierType"),
            funder_name=pl.col("funderName"),
            award_number=pl.col("awardNumber"),
            award_uri=pl.col("awardUri"),
        )
    )

    works_relations = (
        lz_cached.select(
            work_doi=pl.col("id"), relatedIdentifiers=pl.col("attributes").struct.field("relatedIdentifiers")
        )
        .explode("relatedIdentifiers")
        .unnest("relatedIdentifiers")
        .select(
            pl.col("work_doi"),
            relation_type=pl.col("relationType"),
            related_identifier=pl.col("relatedIdentifier"),
            related_identifier_type=pl.col("relatedIdentifierType"),
        )
    )

    return [
        ("datacite_works", works),
        ("datacite_authors", exploded_authors),
        ("datacite_affiliations", works_affiliations),
        ("datacite_funders", works_funders),
        ("datacite_relations", works_relations),
    ]


def parse_args():
    parser = argparse.ArgumentParser(description="Transform DataCite to Parquet for the DMP Tool.")

    # Positional arguments
    parser.add_argument(
        "in_dir",
        type=pathlib.Path,
        help="Path to the input DataCite directory (e.g., /path/to/DataCite_Public_Data_File_2024).",
    )
    parser.add_argument(
        "out_dir",
        type=pathlib.Path,
        help="Path to the output directory for transformed Parquet files (e.g. /path/to/datacite_transformed).",
    )

    # Common keyword arguments
    add_common_args(
        parser=parser,
        batch_size=os.cpu_count(),
        extract_workers=1,
        transform_workers=1,
        cleanup_workers=1,
        extract_queue_size=0,
        transform_queue_size=10,
        cleanup_queue_size=0,
        max_file_processes=os.cpu_count(),
        n_batches=None,
    )
    args = parser.parse_args()

    # Validate
    errors = []
    if not args.in_dir.is_dir() and not validate_directory(args.in_dir, ["dois", "MANIFEST", "README"]):
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
        file_glob="**/*jsonl.gz",
        read_func=read_jsonls,
        extract_func=extract_gzip,
    )
