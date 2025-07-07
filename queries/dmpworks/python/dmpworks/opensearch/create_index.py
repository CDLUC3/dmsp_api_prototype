import json
import logging
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
