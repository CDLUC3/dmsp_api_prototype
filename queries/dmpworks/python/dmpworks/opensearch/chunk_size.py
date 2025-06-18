import json
import logging
import pathlib
from argparse import ArgumentParser, Namespace
from typing import Iterator

from tqdm import tqdm

from dmpworks.opensearch.sync_works import count_records, parse_date, stream_work_actions
from dmpworks.transform.utils_cli import handle_errors


def analyze_bulk_chunks(actions: Iterator[dict], chunk_size: int, total_records: int):
    chunk = []
    chunk_sizes = []
    total_docs = 0

    with tqdm(total=total_records, desc="Analyze chunk size", unit="record") as pbar:
        for action in actions:
            chunk.append(action)
            if len(chunk) >= chunk_size:
                size_bytes = measure_chunk_bytes(chunk)
                chunk_sizes.append(size_bytes)
                total_docs += len(chunk)
                chunk = []
            pbar.update(1)

        if chunk:
            size_bytes = measure_chunk_bytes(chunk)
            chunk_sizes.append(size_bytes)
            total_docs += len(chunk)

    # Final stats
    if chunk_sizes:
        min_bytes = min(chunk_sizes)
        max_bytes = max(chunk_sizes)
        avg_bytes = sum(chunk_sizes) / len(chunk_sizes)
        logging.info(f"Analyzed {len(chunk_sizes)} chunks ({total_docs:,} docs total)")
        logging.info(f"Min chunk size: {min_bytes / 1024 / 1024:.2f} MB")
        logging.info(f"Max chunk size: {max_bytes / 1024 / 1024:.2f} MB")
        logging.info(f"Avg chunk size: {avg_bytes / 1024 / 1024:.2f} MB")
    else:
        logging.warning("No chunks found")


def measure_chunk_bytes(chunk):
    payload = "\n".join(json.dumps(doc, separators=(",", ":")) for doc in chunk) + "\n"
    return len(payload.encode("utf-8"))


def setup_parser(parser: ArgumentParser) -> None:
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
        "--chunk-size",
        type=int,
        default=500,
        help="Chunk size (default: 500)",
    )

    # Callback function
    parser.set_defaults(func=handle_command)


def handle_command(args: Namespace):
    logging.basicConfig(level=logging.INFO)

    # Validate
    errors = []
    if not args.in_dir.is_dir():
        errors.append(f"in_dir '{args.in_dir}' is not a valid directory.")
    handle_errors(errors)

    actions = stream_work_actions(
        source=args.in_dir, index_name="test-index", start_date=args.start_date, batch_size=args.chunk_size
    )
    total_records = count_records(args.in_dir, start_date=args.start_date)
    analyze_bulk_chunks(actions, args.chunk_size, total_records)


def main():
    parser = ArgumentParser(description="Estimate chunk size in bytes when ingesting the OpenSearch works index.")
    setup_parser(parser)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
