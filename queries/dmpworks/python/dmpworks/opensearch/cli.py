import logging
from dataclasses import dataclass
from typing import Annotated, Literal, Optional, Sequence

import pendulum
from cyclopts import App, Parameter, Token

from dmpworks.cli_utils import Directory, LogLevel
from dmpworks.opensearch.chunk_size import measure_chunk_size
from dmpworks.opensearch.create_index import create_index
from dmpworks.opensearch.sync_works import (
    BATCH_SIZE,
    CHUNK_SIZE,
    make_opensearch_client,
    MAX_CHUNK_BYTES,
    MAX_PROCESSES,
    QUEUE_SIZE,
    sync_works,
    THREAD_COUNT,
)

app = App(name="opensearch", help="OpenSearch utilities.")


def validate_chunk_size(type_, value):
    if value <= 0:
        raise ValueError("Chunk size must be greater than zero.")


def parse_date(type_, tokens: Sequence[Token]) -> pendulum.Date:
    value = tokens[0].value
    try:
        return pendulum.from_format(value, "YYYY-MM-DD").date()
    except Exception:
        raise ValueError(f"Not a valid date: '{value}'. Expected format: YYYY-MM-DD")


Mode = Literal["local", "aws"]
ChunkSize = Annotated[int, Parameter(validator=validate_chunk_size)]
Date = Annotated[Optional[pendulum.Date], Parameter(converter=parse_date)]


@dataclass
class OpenSearchClientConfig:
    mode: Mode = "local"
    host: str = "localhost"
    port: int = 9200
    region: str = None
    service: str = None


class OpenSearchSyncConfig:
    max_processes: int = MAX_PROCESSES
    batch_size: int = BATCH_SIZE
    thread_count: int = THREAD_COUNT
    chunk_size: int = CHUNK_SIZE
    max_chunk_bytes: int = MAX_CHUNK_BYTES
    queue_size: int = QUEUE_SIZE
    log_level: int = logging.INFO


# max_processes: Maximum number of processes. Each process reads a parquet file and loads it into OpenSearch.
#         batch_size: Batch size.
#         thread_count: Thread count.
#         chunk_size: Chunk size.
#         max_chunk_bytes: Maximum chunk size in bytes.
#         queue_size: Queue size.


@app.command(name="chunk-size")
def chunk_size_cmd(
    in_dir: Directory,
    start_date: Date = None,
    chunk_size: ChunkSize = 500,
    log_level: LogLevel = "INFO",
):
    """Estimate chunk size in bytes when ingesting the OpenSearch works index.

    Args:
        in_dir: Path to the DMP Tool works hive partitioned index table export directory (e.g., /path/to/export).
        start_date: Date in YYYY-MM-DD to sync records from the export. If no date is specified then all records are synced.
        chunk_size: Chunk size.
        log_level: Python log level.
    """

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    measure_chunk_size(in_dir, start_date, chunk_size)


@app.command(name="create-index")
def create_index_cmd(
    index_name: str,
    mapping_filename: str,
    client_config: OpenSearchClientConfig,
    log_level: LogLevel = "INFO",
):
    """Create an OpenSearch index.

    Args:
        index_name: The name of the OpenSearch index to create (e.g., works).
        mapping_filename: The name of the OpenSearch mapping in the dmpworks.opensearch.mappings resource package (e.g., works-mapping.json).
        client_config: OpenSearch client settings.
        log_level: Python log level.
    """

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    client = make_opensearch_client(client_config)
    create_index(client, index_name, mapping_filename)


@app.command(name="sync-works")
def sync_works_cmd(
    index_name: str,
    in_dir: Directory,
    client_config: OpenSearchClientConfig,
    sync_config: OpenSearchSyncConfig,
    log_level: LogLevel = "INFO",
):
    """Sync the DMP Tool Works Index Table with OpenSearch.

    Args:
        index_name: The name of the OpenSearch index to sync to (e.g., works).
        in_dir: Path to the DMP Tool works hive partitioned index table export directory (e.g., /path/to/export).
        client_config: OpenSearch client settings.
        sync_config: OpenSearch sync settings.
        log_level: Python log level.
    """

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    logging.getLogger("opensearch").setLevel(logging.WARNING)

    sync_works(in_dir, index_name, client_config, sync_config)


if __name__ == "__main__":
    app()
