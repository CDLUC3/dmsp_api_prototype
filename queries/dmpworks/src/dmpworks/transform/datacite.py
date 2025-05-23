import argparse
import json
import logging
import os
import pathlib

import polars as pl
from dmpworks.transform.pipeline import process_files_parallel
from dmpworks.transform.transforms import (
    extract_orcid,
    make_page,
    normalise_identifier,
    remove_markup,
    replace_with_null,
)
from dmpworks.transform.utils_cli import add_common_args, handle_errors, validate_common_args
from dmpworks.transform.utils_file import extract_gzip, read_jsonls, validate_directory
from polars import Date
from polars._typing import SchemaDefinition

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
            "schemeUri": pl.String,
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
        #             "schemeUri": pl.String,
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
            "updated": pl.String,  # ISO 8601 string
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

    works = lz_cached.select(
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
        updated_date=pl.col("attributes")
        .struct.field("updated")
        .str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%SZ"),
        # E.g. 2025-01-01T00:00:01Z
        container_title=pl.col("attributes").struct.field("container").struct.field("title"),
        volume=pl.col("attributes").struct.field("container").struct.field("volume"),
        issue=pl.col("attributes").struct.field("container").struct.field("issue"),
        page=make_page(
            pl.col("attributes").struct.field("container").struct.field("firstPage"),
            pl.col("attributes").struct.field("container").struct.field("lastPage"),
        ),
        publisher=pl.col("attributes").struct.field("publisher").struct.field("name"),
        publisher_location=None,
    ).with_columns(
        title=replace_with_null(pl.col("title"), [""]),
        abstract=replace_with_null(pl.col("abstract"), ["", ":unav", "Cover title."]),
    )

    # TODO: author order?
    exploded_authors = (
        lz_cached.select(work_doi=pl.col("id"), creators=pl.col("attributes").struct.field("creators"))
        .explode("creators")
        .unnest("creators")
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

    # Build authors and clean their ORCID IDs
    works_authors = (
        exploded_authors.filter(pl.col("name_type") == "Personal")
        .select(
            pl.col("work_doi"),
            pl.col("given_name"),
            pl.col("family_name"),
            pl.col("name"),
            orcid=extract_orcid(
                pl.col("name_identifiers")
                .list.eval(
                    pl.element().filter(pl.element().struct.field("nameIdentifierScheme").str.contains("(?i)orc"))
                )
                .list.first()
                .struct.field("nameIdentifier")
            ),
        )
        .filter(~pl.all_horizontal([pl.col(col).is_null() for col in ["given_name", "family_name", "name", "orcid"]]))
    )

    # Build works_affiliations using affiliation identifiers and organisation author name identifiers
    affiliations = (
        exploded_authors.select(pl.col("work_doi"), affiliation=pl.col("affiliation"))
        .explode("affiliation")
        .unnest("affiliation")
        .select(
            pl.col("work_doi"),
            affiliation_identifier=normalise_identifier(pl.col("affiliationIdentifier")),
            affiliation_identifier_scheme=pl.col("affiliationIdentifierScheme"),
            name=pl.col("name"),
            scheme_uri=pl.col("schemeUri"),
            source=pl.lit("PersonalAuthorAffiliation"),
        )
    )
    organisational_authors = (
        exploded_authors.filter(pl.col("name_type") == "Organizational")
        .select(pl.col("work_doi"), pl.col("name"), name_identifiers=pl.col("name_identifiers"))
        .explode("name_identifiers")
        .unnest("name_identifiers")
        .select(
            pl.col("work_doi"),
            affiliation_identifier=normalise_identifier(pl.col("nameIdentifier")),
            affiliation_identifier_scheme=pl.col("nameIdentifierScheme"),
            name=pl.col("name"),
            scheme_uri=pl.col("schemeUri"),
            source=pl.lit("OrganisationalAuthor"),
        )
    )
    works_affiliations = (
        pl.concat([affiliations, organisational_authors])
        .with_columns(name=replace_with_null(pl.col("name"), ["", "none", ":unkn"]))
        .filter(
            ~pl.all_horizontal(
                [
                    pl.col(col).is_null()
                    for col in ["affiliation_identifier", "affiliation_identifier_scheme", "name", "scheme_uri"]
                ]
            )
        )
        .unique()
    )

    # Build funders
    works_funders = (
        lz_cached.select(
            work_doi=pl.col("id"), fundingReferences=pl.col("attributes").struct.field("fundingReferences")
        )
        .explode("fundingReferences")
        .unnest("fundingReferences")
        .select(
            pl.col("work_doi"),
            funder_identifier=normalise_identifier(pl.col("funderIdentifier")),
            funder_identifier_type=pl.col("funderIdentifierType"),
            funder_name=pl.col("funderName"),
            award_number=pl.col("awardNumber"),
            award_uri=pl.col("awardUri"),
        )
        .filter(
            ~pl.all_horizontal(
                [
                    pl.col(col).is_null()
                    for col in [
                        "funder_identifier",
                        "funder_identifier_type",
                        "funder_name",
                        "award_number",
                        "award_uri",
                    ]
                ]
            )
        )
    )

    # Build relations
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
            # TODO: only run normalise_identifier for DOI  related_identifier_type
            related_identifier_type=pl.col("relatedIdentifierType"),
        )
        .filter(
            ~pl.all_horizontal(
                [pl.col(col).is_null() for col in ["relation_type", "related_identifier", "related_identifier_type"]]
            )
        )
    )

    return [
        ("datacite_works", works),
        ("datacite_works_authors", works_authors),
        ("datacite_works_affiliations", works_affiliations),
        ("datacite_works_funders", works_funders),
        ("datacite_works_relations", works_relations),
    ]


def setup_parser(parser: argparse.ArgumentParser) -> None:
    # Positional arguments
    parser.add_argument(
        "in_dir",
        type=pathlib.Path,
        help="Path to the input DataCite directory (e.g., /path/to/DataCite_Public_Data_File_2024).",
    )
    parser.add_argument(
        "out_dir",
        type=pathlib.Path,
        help="Path to the output directory for transformed Parquet files (e.g. /path/to/parquets/datacite).",
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

    # Callback function
    parser.set_defaults(func=handle_command)


def handle_command(args: argparse.Namespace):
    logging.basicConfig(level=logging.DEBUG)

    # Validate
    errors = []
    if not args.in_dir.is_dir() and not validate_directory(args.in_dir, ["dois", "MANIFEST", "README"]):
        errors.append(f"in_dir '{args.in_dir}' is not a valid directory.")

    if not args.out_dir.is_dir():
        errors.append(f"out_dir '{args.out_dir}' is not a valid directory.")

    validate_common_args(args, errors)
    handle_errors(errors)

    process_files_parallel(
        **vars(args),
        schema=SCHEMA,
        transform_func=transform,
        file_glob="**/*jsonl.gz",
        read_func=read_jsonls,
        extract_func=extract_gzip,
    )


def main():
    parser = argparse.ArgumentParser(description="Transform DataCite to Parquet for the DMP Tool.")
    setup_parser(parser)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
