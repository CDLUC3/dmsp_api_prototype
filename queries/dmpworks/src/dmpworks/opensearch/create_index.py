import json
import logging
from argparse import ArgumentParser, Namespace
from importlib.resources import files

from opensearchpy import OpenSearch

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
    # fmt: off
    parser.add_argument("index-name", type=str, help="The name of the OpenSearch index to create (e.g., works).")
    parser.add_argument("mapping-filename", type=str, help=f"The name of the OpenSearch mapping in the {MAPPINGS_PACKAGE} resource package (e.g., works-mapping.json).")
    parser.add_argument("--host", default="localhost", help="Host address (default: localhost)")
    parser.add_argument("--port", type=int, default=9200, help="Port number (default: 9200)")
    # fmt: on

    # Callback function
    parser.set_defaults(func=handle_command)


def handle_command(args: Namespace):
    logging.basicConfig(level=logging.INFO)

    client = OpenSearch(
        hosts=[{"host": args.host, "port": args.host}],
        http_compress=True,
        use_ssl=False,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
    )
    create_index(client, args.index_name, args.mapping_filename)


def main():
    parser = ArgumentParser(description="Create an OpenSearch index.")
    setup_parser(parser)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
