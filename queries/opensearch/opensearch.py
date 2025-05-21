import logging
from itertools import chain
from typing import Optional, List, Dict, Iterator, Generator

import pendulum
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from opensearchpy import OpenSearch
from opensearchpy.helpers import parallel_bulk


def stream_parquet_batches(
    source,
    start_date: Optional[pendulum.date] = None,
    columns: Optional[List[str]] = None,
    batch_size: Optional[int] = None,
) -> Iterator[pa.RecordBatch]:
    dataset = ds.dataset(source, format="parquet", partitioning="hive")

    # Optionally filter by start date
    if start_date is not None:
        expr = (
            (ds.field("year") >= start_date.year)
            | ((ds.field("year") == start_date.year) & (ds.field("month") >= start_date.month))
            | (
                (ds.field("year") == start_date.year)
                & (ds.field("month") == start_date.month)
                & (ds.field("day") >= start_date.day)
            )
        )
        dataset = dataset.filter(expr)

    # Yield batches
    # Type hints are wrong, e.g. parameter isn't int_batch_size, it is batch_size
    for batch in dataset.to_batches(columns=columns, batch_size=batch_size):
        yield batch


def stream_work_actions(
    source, start_date: Optional[pendulum.date] = None, batch_size: Optional[int] = None
) -> Generator[Iterator[dict], None, None]:
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
            "author_orcids",
            "award_ids",
            "funder_ids",
            "funder_names",
            "source",
        ],
    ):
        yield batch_to_work_actions(batch)


def batch_to_work_actions(batch: pa.RecordBatch) -> Iterator[Dict]:
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
        yield {"_op_type": "update", "_index": "works", "_id": doc["doi"], "doc": doc, "doc_as_upsert": True}


def os_index_upsert(
    client: OpenSearch,
    actions: Iterator[List[Dict]],
    thread_count: int = 4,
    chunk_size: int = 500,
    max_chunk_bytes: int = 100 * 1024 * 1024,
    queue_size: int = 4,
):
    total = 0
    success_count = 0
    fail_count = 0

    for success, info in parallel_bulk(
        client,
        actions,
        thread_count=thread_count,
        chunk_size=chunk_size,
        max_chunk_bytes=max_chunk_bytes,
        queue_size=queue_size,
    ):
        total += 1

        if success:
            success_count += 1
            logging.debug(f"Indexed document: {success}")
        else:
            fail_count += 1
            logging.error(f"Failed to index: {info}")

    logging.info(f"os_index_upsert: complete. Total: {total}, Success: {success_count}, Failures: {fail_count}")


def os_create_index_pattern():
    pass


def os_create_index():
    pass


def main():
    logging.basicConfig(level=logging.DEBUG)
    path = "/path/to/export"
    host = "localhost"
    port = 9200
    batch_size = 1000
    client = OpenSearch(
        hosts=[{"host": host, "port": port}],
        http_compress=True,
        use_ssl=False,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
    )
    actions = chain.from_iterable(stream_work_actions(path, batch_size=batch_size))
    os_index_upsert(client, actions)


if __name__ == "__main__":
    main()
