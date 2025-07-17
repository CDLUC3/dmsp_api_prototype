import logging
import pathlib
from concurrent.futures import as_completed, ProcessPoolExecutor
from typing import Iterator, List, Optional

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection
from opensearchpy.helpers import parallel_bulk
from tqdm import tqdm

from dmpworks.opensearch.cli import OpenSearchClientConfig, OpenSearchSyncConfig
from dmpworks.utils import timed

MAX_PROCESSES = 4
BATCH_SIZE = 5000
THREAD_COUNT = 2
CHUNK_SIZE = 500
MAX_CHUNK_BYTES = 100 * 1024 * 1024
QUEUE_SIZE = 2


# Global OpenSearch client, one created for each process
open_search: Optional[OpenSearch] = None


def count_records(source) -> int:
    logging.debug(f"Counting records: {source}")
    dataset = ds.dataset(source, format="parquet")
    return dataset.count_rows()


def stream_parquet_batches(
    source: pathlib.Path,
    columns: Optional[list[str]] = None,
    batch_size: Optional[int] = None,
) -> Iterator[pa.RecordBatch]:
    # Yield batches
    # Type hints are wrong, e.g. parameter isn't int_batch_size, it is batch_size
    table = pq.read_table(source, columns=columns)
    for batch in table.to_batches(batch_size=batch_size):
        yield batch


def batch_to_work_actions(index_name: str, batch: pa.RecordBatch) -> Iterator[dict]:
    # Convert date and datetimes
    batch = batch.set_column(
        batch.schema.get_field_index("publication_date"),
        "publication_date",
        pc.strftime(batch["publication_date"], format="%Y-%m-%d"),
    )
    batch = batch.set_column(
        batch.schema.get_field_index("updated_date"),
        "updated_date",
        pc.strftime(batch["updated_date"], format="%Y-%m-%dT%H:%MZ"),
    )

    # Create actions
    docs = batch.to_pylist()
    for doc in docs:
        yield {"_op_type": "update", "_index": index_name, "_id": doc["doi"], "doc": doc, "doc_as_upsert": True}


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


def init_process(config: OpenSearchClientConfig, level: int):
    global open_search
    logging.basicConfig(level=level, format="[%(asctime)s] [%(levelname)s] [%(processName)s] %(message)s")
    open_search = make_opensearch_client(config)


def index_file(
    *,
    file_path: pathlib.Path,
    index_name: str,
    columns: Optional[List[str]],
    batch_size: int = BATCH_SIZE,
    thread_count: int = THREAD_COUNT,
    chunk_size: int = CHUNK_SIZE,
    max_chunk_bytes: int = MAX_CHUNK_BYTES,
    queue_size: int = QUEUE_SIZE,
):
    batches = stream_parquet_batches(source=file_path, columns=columns, batch_size=batch_size)
    actions = (action for batch in batches for action in batch_to_work_actions(index_name, batch))

    success_count, fail_count = 0, 0
    failed_ids = []

    for success, info in parallel_bulk(
        open_search,
        actions,
        thread_count=thread_count,
        chunk_size=chunk_size,
        max_chunk_bytes=max_chunk_bytes,
        queue_size=queue_size,
    ):
        if success:
            success_count += 1
        else:
            fail_count += 1
            failed_ids.append(info.get("update", {}).get("_id"))

    return success_count, fail_count, failed_ids


@timed
def sync_works(
    in_dir: pathlib.Path,
    index_name: str,
    client_config: OpenSearchClientConfig,
    sync_config: OpenSearchSyncConfig,
    log_level: int = logging.INFO,
):
    parquet_files = list(in_dir.rglob("*.parquet"))
    total_records = count_records(in_dir)
    columns = [
        "doi",
        "title",
        "abstract",
        "type",
        "publication_date",
        "updated_date",
        "affiliation_rors",
        "affiliation_names",
        "author_names",
        "author_orcids",
        "award_ids",
        "funder_ids",
        "funder_names",
        "source",
    ]

    total_count, success_count, failure_count = 0, 0, 0
    all_failed_ids = []

    with tqdm(total=total_records, desc="Sync Works with OpenSearch", unit="doc") as pbar:
        with ProcessPoolExecutor(
            max_workers=sync_config.max_processes,
            initializer=init_process,
            initargs=(
                client_config,
                log_level,
            ),
        ) as executor:
            futures = [
                executor.submit(
                    index_file,
                    file_path=file_path,
                    index_name=index_name,
                    columns=columns,
                    batch_size=sync_config.batch_size,
                    thread_count=sync_config.thread_count,
                    chunk_size=sync_config.chunk_size,
                    max_chunk_bytes=sync_config.max_chunk_bytes,
                    queue_size=sync_config.queue_size,
                )
                for file_path in parquet_files
            ]

            for future in as_completed(futures):
                success, failures, failed_ids = future.result()
                total_count += success + failures
                success_count += success
                failure_count += failures
                all_failed_ids.extend(failed_ids)
                pbar.update(success + failures)
                pbar.set_postfix({"Success": f"{success_count:,}", "Fail": f"{failure_count:,}"})

        logging.info(
            f"Bulk indexing complete. Total: {total_count:,}, Success: {success_count:,}, Failures: {failure_count:,}"
        )

        # Print out failed IDs
        if all_failed_ids:
            logging.error(f"Failed to index {len(all_failed_ids)} documents: {', '.join(all_failed_ids)}")
