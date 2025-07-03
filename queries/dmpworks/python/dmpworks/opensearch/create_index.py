import json
import logging
from argparse import ArgumentParser, Namespace
from importlib.resources import files

from opensearchpy import OpenSearch

from dmpworks.opensearch.sync_works import make_opensearch_client

MAPPINGS_PACKAGE = "dmpworks.opensearch.mappings"


def create_index(client: OpenSearch, index_name: str, mapping_filename: str):
    resource = files(MAPPINGS_PACKAGE) / mapping_filename

    # Validate mapping file
    if not resource.is_file():
        raise FileNotFoundError(f"mapping {mapping_filename} not found in {MAPPINGS_PACKAGE} package resources")

    # Load mapping
    with resource.open("r", encoding="utf-8") as f:
        mapping = json.load(f)

    # Create the index with mapping
    response = client.indices.create(index=index_name, body=mapping)
    logging.info(response)


def setup_parser(parser: ArgumentParser) -> None:
    parser.add_argument(
        "index_name",
        type=str,
        help="The name of the OpenSearch index to create (e.g., works).",
    )
    parser.add_argument(
        "mapping_filename",
        type=str,
        help=f"The name of the OpenSearch mapping in the {MAPPINGS_PACKAGE} resource package (e.g., works-mapping.json).",
    )
    parser.add_argument(
        "--mode",
        choices=["local", "aws"],
        default="local",
        help="Select the mode: local or aws",
    )
    parser.add_argument(
        "--host",
        default="localhost",
        help="Host address (default: localhost)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=9200,
        help="Port number (default: 9200)",
    )
    parser.add_argument(
        "--region",
        help="AWS region (e.g., us-west-1)",
    )
    parser.add_argument(
        "--service",
        help="? (e.g., )",
    )

    # Callback function
    parser.set_defaults(func=handle_command)


def handle_command(args: Namespace):
    logging.basicConfig(level=logging.INFO)

    client = make_opensearch_client(args)
    create_index(client, args.index_name, args.mapping_filename)


def main():
    parser = ArgumentParser(description="Create an OpenSearch index.")
    setup_parser(parser)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
