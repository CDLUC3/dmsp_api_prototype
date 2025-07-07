import logging
import pathlib
from typing import Iterator, Optional

import pendulum
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from opensearchpy import OpenSearch
from opensearchpy.helpers import parallel_bulk
from tqdm import tqdm

from dmpworks.utils import timed


def load_dataset(
    source,
    start_date: Optional[pendulum.Date] = None,
) -> ds.Dataset:
    dataset = ds.dataset(source, format="parquet", partitioning="hive")

    # Optionally filter by start date
    if start_date is not None:
        expr = (ds.field("year") > start_date.year) | (
            (ds.field("year") == start_date.year)
            & (
                (ds.field("month") > start_date.month)
                | ((ds.field("month") == start_date.month) & (ds.field("day") >= start_date.day))
            )
        )
        dataset = dataset.filter(expr)

    return dataset


def count_records(source, start_date: Optional[pendulum.Date] = None) -> int:
    logging.info(f"Counting records: {source}")
    dataset = load_dataset(source, start_date=start_date)
    return dataset.count_rows()


def stream_parquet_batches(
    source,
    start_date: Optional[pendulum.Date] = None,
    columns: Optional[list[str]] = None,
    batch_size: Optional[int] = None,
) -> Iterator[pa.RecordBatch]:
    dataset = load_dataset(source, start_date=start_date)

    # Yield batches
    # Type hints are wrong, e.g. parameter isn't int_batch_size, it is batch_size
    for batch in dataset.to_batches(columns=columns, batch_size=batch_size):
        yield batch


def stream_work_actions(
    *, source, index_name: str, start_date: Optional[pendulum.date] = None, batch_size: Optional[int] = None
) -> Iterator[dict]:
    for batch in stream_parquet_batches(
        source,
        start_date=start_date,
        batch_size=batch_size,
        columns=[
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
        ],
    ):
        actions = batch_to_work_actions(index_name=index_name, batch=batch)
        for action in actions:
            yield action


def batch_to_work_actions(*, index_name: str, batch: pa.RecordBatch) -> Iterator[dict]:
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


def parallel_index_actions(
    client: OpenSearch,
    actions: Iterator[dict],
    total_records: int,
    thread_count: int = 4,
    chunk_size: int = 500,
    max_chunk_bytes: int = 100 * 1024 * 1024,
    queue_size: int = 4,
):
    total = 0
    success_count = 0
    fail_count = 0
    failed_ids = []

    with tqdm(total=total_records, desc="Sync Works with OpenSearch", unit="record") as pbar:
        for success, info in parallel_bulk(
            client,
            actions,
            thread_count=thread_count,
            chunk_size=chunk_size,
            max_chunk_bytes=max_chunk_bytes,
            queue_size=queue_size,
        ):
            total += 1
            pbar.update(1)

            if success:
                success_count += 1
            else:
                fail_count += 1
                failed_ids.append(info.get("update", {}).get("_id"))

            if total % chunk_size == 0:
                pbar.set_postfix({"Success": f"{success_count:,}", "Fail": f"{fail_count:,}"})

    logging.info(f"Bulk indexing complete. Total: {total:,}, Success: {success_count:,}, Failures: {fail_count:,}")

    # Print out failed IDs
    if failed_ids:
        logging.error(f"Failed to index {len(failed_ids)} documents: {', '.join(failed_ids)}")


@timed
def sync_works(
    client: OpenSearch,
    in_dir: pathlib.Path,
    index_name: str,
    start_date: pendulum.Date,
    batch_size: int = 1000,
    thread_count: int = 4,
    chunk_size: int = 500,
    max_chunk_bytes: int = 100 * 1024 * 1024,
    queue_size: int = 4,
):
    actions = stream_work_actions(source=in_dir, index_name=index_name, start_date=start_date, batch_size=batch_size)
    total_records = count_records(in_dir, start_date=start_date)
    parallel_index_actions(
        client,
        actions,
        total_records,
        thread_count=thread_count,
        chunk_size=chunk_size,
        max_chunk_bytes=max_chunk_bytes,
        queue_size=queue_size,
    )
