import logging
from typing import Optional

from cyclopts import App

from dmpworks.cli_utils import Directory, LogLevel
from dmpworks.opensearch.create_index import create_index
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


@app.command(name="sync-works")
def sync_works_cmd(
    index_name: str,
    in_dir: Directory,
    client_config: Optional[OpenSearchClientConfig] = None,
    sync_config: Optional[OpenSearchSyncConfig] = None,
    dry_run: bool = False,
    measure_chunk_size: bool = False,
    log_level: LogLevel = "INFO",
):
    """Sync the DMP Tool Works Index Table with OpenSearch.

    Args:
        index_name: The name of the OpenSearch index to sync to (e.g., works).
        in_dir: Path to the DMP Tool works hive partitioned index table export directory (e.g., /path/to/export).
        client_config: OpenSearch client settings.
        sync_config: OpenSearch sync settings.
        dry_run: reads and processes files, but doesn't send data to OpenSearch.
        measure_chunk_size: measure the average chunk size, dry-run must also be on.
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
        dry_run=dry_run,
        measure_chunk_size=measure_chunk_size,
        log_level=level,
    )


if __name__ == "__main__":
    app()
