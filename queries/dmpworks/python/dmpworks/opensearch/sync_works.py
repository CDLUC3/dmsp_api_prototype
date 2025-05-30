import argparse
import logging
import pathlib
from argparse import ArgumentParser, Namespace
from typing import Iterator, Optional

import pendulum
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from opensearchpy import OpenSearch
from opensearchpy.helpers import parallel_bulk
from tqdm import tqdm

from dmpworks.transform.utils_cli import handle_errors


def load_dataset(
    source,
    start_date: Optional[pendulum.Date] = None,
) -> ds.Dataset:
    dataset = ds.dataset(source, format="parquet", partitioning="hive")

    # Optionally filter by start date
    if start_date is not None:
        expr = (
            (ds.field("year") > start_date.year)
            | ((ds.field("year") == start_date.year) & (ds.field("month") > start_date.month))
            | (
                (ds.field("year") == start_date.year)
                & (ds.field("month") == start_date.month)
                & (ds.field("day") >= start_date.day)
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
                pbar.set_postfix({
                    "Success": f"{success_count:,}",
                    "Fail": f"{fail_count:,}"
                })

    logging.info(f"Bulk indexing complete. Total: {total:,}, Success: {success_count:,}, Failures: {fail_count:,}")

    # Print out failed IDs
    if failed_ids:
        logging.error(f"Failed to index {len(failed_ids)} documents: {', '.join(failed_ids)}")

def parse_date(s: str) -> pendulum.Date:
    try:
        return pendulum.from_format(s, "YYYY-MM-DD").date()
    except Exception:
        raise argparse.ArgumentTypeError(f"Not a valid date: '{s}'. Expected format: YYYY-MM-DD")


def setup_parser(parser: ArgumentParser) -> None:
    parser.add_argument(
        "index_name",
        type=str,
        help="The name of the OpenSearch index to sync to (e.g., works).",
    )
    parser.add_argument(
        "in_dir",
        type=pathlib.Path,
        help="Path to the DMP Tool works hive partitioned index table export directory (e.g., /path/to/export).",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        type=parse_date,
        help="Date in YYYY-MM-DD to sync records from in the export. If no date is specified then all records synced (default: None)",
    )
    parser.add_argument(
        "--host",
        default="localhost",
        help="Host address (default: localhost)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=9200,
        help="Port number (default: 9200)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size (default: 1000)",
    )
    parser.add_argument(
        "--thread-count",
        type=int,
        default=4,
        help="Thread count (default: 4)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=500,
        help="Chunk size (default: 500)",
    )
    parser.add_argument(
        "--max-chunk-bytes",
        type=int,
        default=100 * 1024 * 1024,
        help="Maximum chunk size in bytes (default: 100MB)",
    )
    parser.add_argument(
        "--queue-size",
        type=int,
        default=4,
        help="Queue size (default: 4)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )

    # Callback function
    parser.set_defaults(func=handle_command)


def handle_command(args: Namespace):
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO)
    logging.getLogger("opensearch").setLevel(logging.WARNING)

    start = pendulum.now()

    # Validate
    errors = []
    if not args.in_dir.is_dir():
        errors.append(f"in_dir '{args.in_dir}' is not a valid directory.")
    handle_errors(errors)

    client = OpenSearch(
        hosts=[{"host": args.host, "port": args.port}],
        http_compress=True,
        use_ssl=False,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
    )
    actions = stream_work_actions(
        source=args.in_dir, index_name=args.index_name, start_date=args.start_date, batch_size=args.batch_size
    )
    total_records = count_records(args.in_dir, start_date=args.start_date)
    parallel_index_actions(
        client,
        actions,
        total_records,
        thread_count=args.thread_count,
        chunk_size=args.chunk_size,
        max_chunk_bytes=args.max_chunk_bytes,
        queue_size=args.queue_size,
    )

    end = pendulum.now()
    diff = end - start
    logging.info(f"Execution time: {diff.in_words()}")


def main():
    parser = ArgumentParser(description="Sync the DMP Tool Works Index Table with OpenSearch.")
    setup_parser(parser)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
