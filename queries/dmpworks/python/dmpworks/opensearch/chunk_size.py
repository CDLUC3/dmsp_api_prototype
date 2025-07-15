import json
import logging
import pathlib
from typing import Iterator

from tqdm import tqdm

from dmpworks.opensearch.sync_works import count_records


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


def measure_chunk_size(in_dir: pathlib.Path, chunk_size: int):
    actions = stream_work_actions(source=in_dir, index_name="test-index", batch_size=chunk_size)
    total_records = count_records(in_dir)
    analyze_bulk_chunks(actions, chunk_size, total_records)
