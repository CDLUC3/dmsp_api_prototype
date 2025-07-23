import json
import logging
import math
import multiprocessing as mp
import pathlib
import queue
import time
from concurrent.futures import ProcessPoolExecutor
from typing import Iterator, List, Optional

import pendulum
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from opensearchpy import OpenSearch
from opensearchpy.helpers import streaming_bulk
from tqdm import tqdm

from dmpworks.opensearch.utils import (
    CHUNK_SIZE,
    COLUMNS,
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
chunk_sizes_queue: Optional[mp.Queue] = None


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
    with pq.ParquetFile(source) as parquet_file:
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
    chunk_sizes: mp.Queue,
    create_open_search: bool,
    level: int,
):
    global open_search, success_counter, failure_counter, counter_lock, chunk_sizes_queue
    logging.basicConfig(level=level, format="[%(asctime)s] [%(levelname)s] [%(processName)s] %(message)s")
    logging.getLogger("opensearch").setLevel(logging.WARNING)
    if create_open_search:
        open_search = make_opensearch_client(config)
    success_counter = success_value
    failure_counter = failure_value
    counter_lock = lock
    chunk_sizes_queue = chunk_sizes


def measure_chunk_bytes(chunk):
    payload = "\n".join(json.dumps(doc, separators=(",", ":")) for doc in chunk) + "\n"
    return len(payload.encode("utf-8"))


def collect_completed_futures(futures: list):
    finished = []
    for fut in futures:
        if fut.done():
            try:
                fut.result()
            except Exception as e:
                log.exception(f"Error processing future: {e}")
            finished.append(fut)
    for fut in finished:
        futures.remove(fut)


def update_progress_bar(
    pbar: tqdm, success_count: int, failure_count: int, total: int, postfix_extra: Optional[dict] = None
):
    new_total = success_count + failure_count
    delta = new_total - total
    pbar.update(delta)
    postfix = {"Success": f"{success_count:,}", "Fail": f"{failure_count:,}"}
    if postfix_extra:
        postfix.update(postfix_extra)
    pbar.set_postfix(postfix)
    return new_total


def collect_chunk_sizes(
    chunk_sizes: mp.Queue,
    min_chunk_size: float,
    max_chunk_size: float,
    sum_chunk_size: float,
    total_chunks: int,
    drain_queue: bool = False,
    timeout_seconds: int = 1,
):
    start_time = time.monotonic()
    while drain_queue or (time.monotonic() - start_time < timeout_seconds):
        try:
            chunk_size = chunk_sizes.get(block=True, timeout=1)
            min_chunk_size = min(min_chunk_size, chunk_size)
            max_chunk_size = max(max_chunk_size, chunk_size)
            sum_chunk_size += chunk_size
            total_chunks += 1
        except queue.Empty:
            # If drain_queue is True and we get an empty queue
            # then we can break
            if drain_queue:
                break

    return min_chunk_size, max_chunk_size, sum_chunk_size, total_chunks


def index_actions(
    actions: Iterator[dict],
    chunk_size: int,
    max_chunk_bytes: int,
    max_retries: int,
    initial_backoff: int,
    max_backoff: int,
):
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
        with counter_lock:
            if ok:
                success_counter.value += 1
            else:
                failure_counter.value += 1


def measure_chunks(
    actions: Iterator[dict],
    chunk_size: int,
):
    chunk = []
    for action in actions:
        chunk.append(action)
        if len(chunk) >= chunk_size:
            size_bytes = measure_chunk_bytes(chunk)
            chunk_sizes_queue.put(size_bytes)
            chunk = []

        with counter_lock:
            success_counter.value += 1

    # Process final chunk if it's non-empty
    if chunk:
        size_bytes = measure_chunk_bytes(chunk)
        chunk_sizes_queue.put(size_bytes)


def dry_run_actions(
    actions: Iterator[dict],
):
    for _ in actions:
        with counter_lock:
            success_counter.value += 1


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
    dry_run: bool = False,
    measure_chunk_size: bool = False,
):
    batches = stream_parquet_batches(source=file_path, columns=columns, batch_size=chunk_size)
    actions = (action for batch in batches for action in batch_to_work_actions(index_name, batch))

    if dry_run:
        log.debug(f"Dry run on file: {file_path}")
        dry_run_actions(
            actions,
        )
    elif measure_chunk_size:
        log.debug(f"Measuring chunks from file: {file_path}")
        measure_chunks(
            actions,
            chunk_size,
        )
    else:
        log.debug(f"Indexing file: {file_path}")
        index_actions(
            actions,
            chunk_size,
            max_chunk_bytes,
            max_retries,
            initial_backoff,
            max_backoff,
        )


def bytes_to_mb(n):
    return n / 1024 / 1024


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
    log.info(f"Total records {total_records:,}")

    total = 0
    ctx = mp.get_context("spawn")
    success = ctx.Value("i", 0)
    failure = ctx.Value("i", 0)
    chunk_sizes: mp.Queue = ctx.Queue()
    lock = ctx.Lock()

    # Chunk size stats
    min_chunk_size = math.inf
    max_chunk_size = -math.inf
    sum_chunk_size = 0
    total_chunks = 0

    start = pendulum.now()
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
                chunk_sizes,
                not (sync_config.dry_run or sync_config.measure_chunk_size),
                log_level,
            ),
        ) as executor:
            log.debug("Queuing futures...")
            futures = [
                executor.submit(
                    index_file,
                    file_path=file_path,
                    index_name=index_name,
                    columns=COLUMNS,
                    chunk_size=sync_config.chunk_size,
                    max_chunk_bytes=sync_config.max_chunk_bytes,
                    max_retries=sync_config.max_retries,
                    initial_backoff=sync_config.initial_backoff,
                    max_backoff=sync_config.max_backoff,
                    dry_run=sync_config.dry_run,
                    measure_chunk_size=sync_config.measure_chunk_size,
                )
                for file_path in parquet_files
            ]
            log.debug("Finished queuing futures.")

            while futures:
                # Get counts
                log.debug("Get counts")
                with lock:
                    success_count = success.value
                    failure_count = failure.value

                # Collect any futures have finished and are done and save failed_ids
                log.debug("Collect futures")
                collect_completed_futures(futures)

                # Collect chunk sizes
                if sync_config.measure_chunk_size:
                    log.debug("Collect chunk sizes")
                    min_chunk_size, max_chunk_size, sum_chunk_size, total_chunks = collect_chunk_sizes(
                        chunk_sizes,
                        min_chunk_size,
                        max_chunk_size,
                        sum_chunk_size,
                        total_chunks,
                    )

                # Update progress bar
                log.debug("Update progress bar")
                postfix_extra = None
                if sync_config.measure_chunk_size and math.isfinite(sum_chunk_size) and total_chunks > 0:
                    log.debug(f"Calculating chunk size")
                    mean_chunk_size = bytes_to_mb(sum_chunk_size / total_chunks)
                    postfix_extra = {"Avg Chunk Size": f"{mean_chunk_size:.2f} MB"}
                total = update_progress_bar(pbar, success_count, failure_count, total, postfix_extra)

                # Sleep
                if not sync_config.measure_chunk_size:
                    log.debug("Sleep")
                    time.sleep(1)

            if sync_config.measure_chunk_size:
                log.debug("Collect any remaining chunk sizes")
                min_chunk_size, max_chunk_size, sum_chunk_size, total_chunks = collect_chunk_sizes(
                    chunk_sizes,
                    min_chunk_size,
                    max_chunk_size,
                    sum_chunk_size,
                    total_chunks,
                    drain_queue=True,
                )

            log.debug("Exiting ProcessPoolExecutor...")
    log.debug("Exited ProcessPoolExecutor")

    end = pendulum.now()
    duration_seconds = float((end - start).seconds)  # Prevent divide by zero
    docs_per_sec = total / max(duration_seconds, 1e-6)

    log.info(f"Bulk indexing complete.")
    log.info(f"Total docs: {total:,}")
    log.info(f"Num success: {success_count:,}")
    log.info(f"Num failures: {failure_count:,}")
    log.info(f"Docs/s: {round(docs_per_sec):,}")

    if sync_config.measure_chunk_size:
        avg_bytes = sum_chunk_size / total_chunks
        log.info(f"Analyzed {total_chunks} chunks from ({total:,} docs total)")
        log.info(f"Min chunk size: {bytes_to_mb(min_chunk_size):.2f} MB")
        log.info(f"Max chunk size: {bytes_to_mb(max_chunk_size):.2f} MB")
        log.info(f"Avg chunk size: {bytes_to_mb(avg_bytes):.2f} MB")
