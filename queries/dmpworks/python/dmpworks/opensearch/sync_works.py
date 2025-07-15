import logging
import multiprocessing as mp
import pathlib
import time
from concurrent.futures import ProcessPoolExecutor
from typing import Iterator, List, Optional

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from opensearchpy import OpenSearch
from opensearchpy.helpers import streaming_bulk
from tqdm import tqdm

from dmpworks.opensearch.utils import (
    CHUNK_SIZE,
    INITIAL_BACKOFF,
    make_opensearch_client,
    MAX_BACKOFF,
    MAX_CHUNK_BYTES,
    MAX_RETRIES,
    OpenSearchClientConfig,
    OpenSearchSyncConfig,
)
from dmpworks.utils import timed

log = logging.getLogger(__name__)


# Global OpenSearch client, one created for each process
open_search: Optional[OpenSearch] = None
success_counter: Optional[mp.Value] = None
failure_counter: Optional[mp.Value] = None
counter_lock: Optional[mp.Value] = None


def count_records(source) -> int:
    log.debug(f"Counting records: {source}")
    dataset = ds.dataset(source, format="parquet")
    return dataset.count_rows()


def stream_parquet_batches(
    source: pathlib.Path,
    columns: Optional[list[str]] = None,
    batch_size: Optional[int] = None,
) -> Iterator[pa.RecordBatch]:
    # Yield batches from a parquet file
    parquet_file = pq.ParquetFile(source)
    for batch in parquet_file.iter_batches(batch_size=batch_size, columns=columns):
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
    for i in range(batch.num_rows):
        doc = {name: batch[name][i].as_py() for name in batch.schema.names}
        yield {
            "_op_type": "update",
            "_index": index_name,
            "_id": doc["doi"],
            "doc": doc,
            "doc_as_upsert": True,
        }


def init_process(
    config: OpenSearchClientConfig,
    success_value: mp.Value,
    failure_value: mp.Value,
    lock: mp.Lock,
    level: int,
):
    global open_search, success_counter, failure_counter, counter_lock
    logging.basicConfig(level=level, format="[%(asctime)s] [%(levelname)s] [%(processName)s] %(message)s")
    # logging.getLogger("opensearch").setLevel(logging.WARNING)
    open_search = make_opensearch_client(config)
    success_counter = success_value
    failure_counter = failure_value
    counter_lock = lock


def index_file(
    *,
    file_path: pathlib.Path,
    index_name: str,
    columns: Optional[List[str]],
    chunk_size: int = CHUNK_SIZE,
    max_chunk_bytes: int = MAX_CHUNK_BYTES,
    max_retries: int = MAX_RETRIES,
    initial_backoff: int = INITIAL_BACKOFF,
    max_backoff: int = MAX_BACKOFF,
):
    batches = stream_parquet_batches(source=file_path, columns=columns, batch_size=chunk_size)
    actions = (action for batch in batches for action in batch_to_work_actions(index_name, batch))

    for ok, info in streaming_bulk(
        open_search,
        actions,
        chunk_size=chunk_size,
        max_chunk_bytes=max_chunk_bytes,
        max_retries=max_retries,
        initial_backoff=initial_backoff,
        max_backoff=max_backoff,
        raise_on_error=False,
        raise_on_exception=False,
    ):
        if not ok:
            with counter_lock:
                failure_counter.value += 1
        else:
            with counter_lock:
                success_counter.value += 1


@timed
def sync_works(
    index_name: str,
    in_dir: pathlib.Path,
    client_config: OpenSearchClientConfig,
    sync_config: OpenSearchSyncConfig,
    log_level: int = logging.INFO,
):
    parquet_files = list(in_dir.rglob("*.parquet"))
    log.info("Counting records...")
    total_records = count_records(in_dir)
    log.info(f"Total records {total_records}")
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

    total = 0
    ctx = mp.get_context("spawn")
    success = ctx.Value("i", 0)
    failure = ctx.Value("i", 0)
    lock = ctx.Lock()

    with tqdm(total=total_records, desc="Sync Works with OpenSearch", unit="doc") as pbar:
        with ProcessPoolExecutor(
            mp_context=ctx,
            max_workers=sync_config.max_processes,
            initializer=init_process,
            initargs=(
                client_config,
                success,
                failure,
                lock,
                log_level,
            ),
        ) as executor:
            log.info("Queuing futures...")
            futures = [
                executor.submit(
                    index_file,
                    file_path=file_path,
                    index_name=index_name,
                    columns=columns,
                    chunk_size=sync_config.chunk_size,
                    max_chunk_bytes=sync_config.max_chunk_bytes,
                    max_retries=sync_config.max_retries,
                    initial_backoff=sync_config.initial_backoff,
                    max_backoff=sync_config.max_backoff,
                )
                for file_path in parquet_files
            ]
            log.info("Finished queuing futures.")

            while futures:
                # Get counts
                with lock:
                    success_count = success.value
                    failure_count = failure.value

                # Update process bar
                new_total = success_count + failure_count
                delta = new_total - total
                total = new_total
                pbar.update(delta)
                pbar.set_postfix({"Success": f"{success_count:,}", "Fail": f"{failure_count:,}"})

                # Get any futures have finished and are done and save failed_ids
                finished = []
                for fut in futures:
                    if fut.done():
                        try:
                            fut.result()
                        except Exception as e:
                            log.exception(f"Error processing future: {e}")
                            continue

                        finished.append(fut)
                for fut in finished:
                    futures.remove(fut)

                # Sleep
                time.sleep(1)

        log.info(f"Bulk indexing complete. Total: {total:,}, Success: {success_count:,}, Failures: {failure_count:,}")
