from dataclasses import dataclass
from typing import Annotated, Literal, Optional, Sequence

import boto3
import pendulum
from cyclopts import Parameter, Token
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection

MAX_PROCESSES = 4
BATCH_SIZE = 5000
THREAD_COUNT = 2
CHUNK_SIZE = 2500
MAX_CHUNK_BYTES = 100 * 1024 * 1024
QUEUE_SIZE = 2


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
    region: Optional[str] = None
    service: Optional[str] = None


@dataclass
class OpenSearchSyncConfig:
    max_processes: int = MAX_PROCESSES
    batch_size: int = BATCH_SIZE
    thread_count: int = THREAD_COUNT
    chunk_size: int = CHUNK_SIZE
    max_chunk_bytes: int = MAX_CHUNK_BYTES
    queue_size: int = QUEUE_SIZE


# max_processes: Maximum number of processes. Each process reads a parquet file and loads it into OpenSearch.
#         batch_size: Batch size.
#         thread_count: Thread count.
#         chunk_size: Chunk size.
#         max_chunk_bytes: Maximum chunk size in bytes.
#         queue_size: Queue size.


def make_opensearch_client(config: OpenSearchClientConfig) -> OpenSearch:
    if config.mode == "aws":
        credentials = boto3.Session().get_credentials()
        auth = AWSV4SignerAuth(credentials, config.region, config.service)
        client = OpenSearch(
            hosts=[{'host': config.host, 'port': config.port}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            pool_maxsize=20,
        )
    else:
        client = OpenSearch(
            hosts=[{"host": config.host, "port": config.port}],
            http_compress=True,
            use_ssl=False,
            verify_certs=False,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
            pool_maxsize=20,
        )

    return client
