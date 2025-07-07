import logging
import pathlib
from typing import Literal, Optional, Sequence

import boto3
import pendulum
from cyclopts import App, Parameter, Token, validators
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection
from sqlalchemy.sql.annotation import Annotated

from dmpworks.opensearch.chunk_size import measure_chunk_size
from dmpworks.opensearch.create_index import create_index
from dmpworks.opensearch.sync_works import sync_works

app = App(name="opensearch", help="OpenSearch utilities")


def validate_chunk_size(type_, value):
    if value <= 0:
        raise ValueError("Chunk size must be greater than zero.")


def parse_date(type_, tokens: Sequence[Token]) -> pendulum.Date:
    value = tokens[0].value
    try:
        return pendulum.from_format(value, "YYYY-MM-DD").date()
    except Exception:
        raise ValueError(f"Not a valid date: '{value}'. Expected format: YYYY-MM-DD")


Directory = Annotated[
    pathlib.Path,
    Parameter(
        validator=validators.Path(
            dir_okay=True,
            file_okay=False,
            exists=True,
        )
    ),
]
Mode = Literal["local", "aws"]
ChunkSize = Annotated[int, Parameter(validator=validate_chunk_size)]
Date = Annotated[Optional[pendulum.Date], Parameter(converter=parse_date)]
LogLevel = Annotated[
    Literal["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"],
    Parameter(help="Python log level"),
]


def make_opensearch_client(
    mode: str, host: str, port: int, region: Optional[str] = None, service: Optional[str] = None
) -> OpenSearch:
    if mode == "aws":
        credentials = boto3.Session().get_credentials()
        auth = AWSV4SignerAuth(credentials, region, service)
        client = OpenSearch(
            hosts=[{'host': host, 'port': port}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            pool_maxsize=20,
        )
    else:
        client = OpenSearch(
            hosts=[{"host": host, "port": port}],
            http_compress=True,
            use_ssl=False,
            verify_certs=False,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
            pool_maxsize=20,
        )

    return client


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
    mode: Mode = "local",
    host: str = "localhost",
    port: int = 9200,
    region: Optional[str] = None,
    service: Optional[str] = None,
    log_level: LogLevel = "INFO",
):
    """Create an OpenSearch index.

    Args:
        index_name: The name of the OpenSearch index to create (e.g., works).
        mapping_filename: The name of the OpenSearch mapping in the dmpworks.opensearch.mappings resource package (e.g., works-mapping.json).
        mode: Select the mode: local or aws.
        host: Host address.
        port: Port number.
        region: AWS region (e.g., us-west-1).
        service: ? (e.g., ).
        log_level: Python log level.
    """

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    client = make_opensearch_client(mode, host, port, region=region, service=service)
    create_index(client, index_name, mapping_filename)


@app.command(name="sync-works")
def sync_works_cmd(
    index_name: str,
    in_dir: Directory,
    mode: Mode = "local",
    start_date: Date = None,
    host: str = "localhost",
    port: int = 9200,
    region: Optional[str] = None,
    service: Optional[str] = None,
    batch_size: int = 5000,
    thread_count: int = 4,
    chunk_size: ChunkSize = 5000,
    max_chunk_bytes: int = 100 * 1024 * 1024,
    queue_size: int = 4,
    log_level: LogLevel = "INFO",
):
    """Sync the DMP Tool Works Index Table with OpenSearch.

    Args:
        index_name: The name of the OpenSearch index to sync to (e.g., works).
        in_dir: Path to the DMP Tool works hive partitioned index table export directory (e.g., /path/to/export).
        mode: Select the mode: local or aws.
        start_date: Date in YYYY-MM-DD to sync records from the export. If no date is specified then all records are synced.
        host: Host address (e.g., us-west-1).
        port: Port number.
        region: AWS region.
        service: AWS service.
        batch_size: Batch size.
        thread_count: Thread count.
        chunk_size: Chunk size.
        max_chunk_bytes: Maximum chunk size in bytes.
        queue_size: Queue size.
        log_level: Python log level.
    """

    level = logging.getLevelName(log_level)
    logging.basicConfig(level=level)
    logging.getLogger("opensearch").setLevel(level)

    client = make_opensearch_client(mode, host, port, region=region, service=service)
    sync_works(
        client,
        in_dir,
        index_name,
        start_date,
        batch_size=batch_size,
        thread_count=thread_count,
        chunk_size=chunk_size,
        max_chunk_bytes=max_chunk_bytes,
        queue_size=queue_size,
    )


if __name__ == "__main__":
    app()
