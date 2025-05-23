import argparse

from dmpworks.transform.crossref_metadata import setup_parser as setup_crossref_metadata
from dmpworks.transform.datacite import setup_parser as setup_datacite
from dmpworks.transform.openalex_works import setup_parser as setup_openalex_works
from dmpworks.transform.openalex_funders import setup_parser as setup_openalex_funders
from dmpworks.transform.ror import setup_parser as setup_ror
from dmpworks.opensearch.create_index import setup_parser as setup_create_index
from dmpworks.opensearch.sync_works import setup_parser as setup_sync_works


def main():
    parser = argparse.ArgumentParser(prog="dmpworks")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.required = True

    # Transform Commands
    transform_parser = subparsers.add_parser("transform")
    transform_subparsers = transform_parser.add_subparsers(dest="transform_command")
    transform_subparsers.required = True

    # fmt: off
    crossref_parser = transform_subparsers.add_parser("crossref-metadata", description="Transform Crossref Metadata to Parquet for the DMP Tool")
    setup_crossref_metadata(crossref_parser)

    datacite_parser = transform_subparsers.add_parser("datacite", description="Transform DataCite to Parquet for the DMP Tool.")
    setup_datacite(datacite_parser)

    openalex_works_parser = transform_subparsers.add_parser("openalex-works", description="Transform OpenAlex Works to Parquet for the DMP Tool.")
    setup_openalex_works(openalex_works_parser)

    openalex_funders_parser = transform_subparsers.add_parser("openalex-funders", description="Transform OpenAlex Funders to Parquet for the DMP Tool.")
    setup_openalex_funders(openalex_funders_parser)

    ror_parser = transform_subparsers.add_parser("ror", description="Transform ROR to Parquet for the DMP Tool.")
    setup_ror(ror_parser)
    # fmt: on

    # OpenSearch Commands
    os_parser = subparsers.add_parser("opensearch")
    os_subparsers = os_parser.add_subparsers(dest="opensearch_command")
    os_subparsers.required = True

    # fmt: off
    create_index_parser = os_subparsers.add_parser("create-index", description="Create an OpenSearch index.")
    setup_create_index(create_index_parser)

    sync_works_parser = os_subparsers.add_parser("sync-works", description="Sync the DMP Tool Works Index Table with OpenSearch.")
    setup_sync_works(sync_works_parser)
    # fmt: on

    args = parser.parse_args()
    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
