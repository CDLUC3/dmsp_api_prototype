import gzip
import logging
import shutil
from pathlib import Path
from typing import Callable, Generator

import polars as pl
from polars._typing import SchemaDefinition

BatchGenerator = Generator[list[Path], None, None]
PrefetchLoaderExtractFunction = Callable[[list[Path], int], list[Path]]


def validate_directory(path: Path, expected_items: list[str]) -> bool:
    actual_items = {p.name for p in path.iterdir()}
    missing = [item for item in expected_items if item not in actual_items]

    if missing:
        logging.info("Missing items:", missing)
        return False

    return True


def batch_files(files: list[Path], batch_size: int) -> BatchGenerator:
    for i in range(0, len(files), batch_size):
        yield files[i : i + batch_size]


def extract_gzip(in_file: Path, out_file: Path) -> None:
    with gzip.open(in_file, "rb") as f_in, open(out_file, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)


def read_jsonls(files: list[Path], schema: SchemaDefinition, low_memory: bool) -> pl.LazyFrame:
    return pl.scan_ndjson(files, schema=schema, low_memory=low_memory)


def write_parquet(lz: pl.LazyFrame, out_file: Path) -> None:
    lz.sink_parquet(out_file, compression="snappy")


def log_stage(stage: str, status: str, batch: int):
    logging.debug(f"[{stage:<10}] {status:<5} batch={batch}")
