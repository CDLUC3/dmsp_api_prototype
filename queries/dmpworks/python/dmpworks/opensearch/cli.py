import logging
from typing import Optional

from cyclopts import App

from dmpworks.cli_utils import Directory, LogLevel
from dmpworks.opensearch.enrich_dmps import enrich_dmps
from dmpworks.opensearch.index import create_index, update_mapping
from dmpworks.opensearch.sync_dmps import sync_dmps
from dmpworks.opensearch.sync_works import sync_works
from dmpworks.opensearch.utils import (
    make_opensearch_client,
    OpenSearchClientConfig,
    OpenSearchSyncConfig,
)

app = App(name="opensearch", help="OpenSearch utilities.")


@app.command(name="create-index")
def create_index_cmd(
    index_name: str,
    mapping_filename: str,
    client_config: Optional[OpenSearchClientConfig] = None,
    log_level: LogLevel = "INFO",
):
    """Create an OpenSearch index.

    Args:
        index_name: The name of the OpenSearch index to create (e.g., works).
        mapping_filename: The name of the OpenSearch mapping in the dmpworks.opensearch.mappings resource package (e.g., works-mapping.json).
        client_config: OpenSearch client settings.
        log_level: Python log level.
    """

    if client_config is None:
        client_config = OpenSearchClientConfig()

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    client = make_opensearch_client(client_config)
    create_index(client, index_name, mapping_filename)


@app.command(name="update-mapping")
def update_mapping_cmd(
    index_name: str,
    mapping_filename: str,
    client_config: Optional[OpenSearchClientConfig] = None,
    log_level: LogLevel = "INFO",
):
    """Update an OpenSearch index mapping.

    Args:
        index_name: The name of the OpenSearch index to update (e.g., works).
        mapping_filename: The name of the OpenSearch mapping in the dmpworks.opensearch.mappings resource package (e.g., works-mapping.json).
        client_config: OpenSearch client settings.
        log_level: Python log level.
    """

    if client_config is None:
        client_config = OpenSearchClientConfig()

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    client = make_opensearch_client(client_config)
    update_mapping(client, index_name, mapping_filename)


@app.command(name="sync-works")
def sync_works_cmd(
    index_name: str,
    in_dir: Directory,
    client_config: Optional[OpenSearchClientConfig] = None,
    sync_config: Optional[OpenSearchSyncConfig] = None,
    log_level: LogLevel = "INFO",
):
    """Sync the DMP Tool Works Index Table with OpenSearch.

    Args:
        index_name: The name of the OpenSearch index to sync to (e.g., works).
        in_dir: Path to the DMP Tool Works index table export directory (e.g., /path/to/export).
        client_config: OpenSearch client settings.
        sync_config: OpenSearch sync settings.
        log_level: Python log level.
    """

    if client_config is None:
        client_config = OpenSearchClientConfig()

    if sync_config is None:
        sync_config = OpenSearchSyncConfig()

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    logging.getLogger("opensearch").setLevel(logging.WARNING)

    sync_works(
        index_name,
        in_dir,
        client_config,
        sync_config,
        log_level=level,
    )


@app.command(name="sync-dmps")
def sync_dmps_cmd(
    index_name: str,
    in_dir: Directory,
    client_config: Optional[OpenSearchClientConfig] = None,
    sync_config: Optional[OpenSearchSyncConfig] = None,
    log_level: LogLevel = "INFO",
):
    """Sync the DMP Tool DMP Table with OpenSearch.

    Args:
        index_name: The name of the OpenSearch index to sync to (e.g., dmps).
        in_dir: Path to the DMP Tool DMPs export directory (e.g., /path/to/export).
        client_config: OpenSearch client settings.
        sync_config: OpenSearch sync settings.
        log_level: Python log level.
    """

    if client_config is None:
        client_config = OpenSearchClientConfig()

    if sync_config is None:
        sync_config = OpenSearchSyncConfig()

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    logging.getLogger("opensearch").setLevel(logging.WARNING)

    sync_dmps(
        index_name,
        in_dir,
        client_config,
        sync_config,
        log_level=level,
    )


@app.command(name="enrich-dmps")
def enrich_dmps_cmd(
    index_name: str,
    client_config: Optional[OpenSearchClientConfig] = None,
    log_level: LogLevel = "INFO",
):
    """"""

    if client_config is None:
        client_config = OpenSearchClientConfig()

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    logging.getLogger("opensearch").setLevel(logging.WARNING)

    enrich_dmps(
        index_name,
        client_config,
    )


if __name__ == "__main__":
    app()
