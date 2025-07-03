import argparse
import gzip
import logging
import os
import pathlib
import re
from concurrent.futures import as_completed, ProcessPoolExecutor
from multiprocessing import current_process
from typing import Literal, Optional

import orjson
import pendulum
from tqdm import tqdm

from dmpworks.transform.utils_file import setup_multiprocessing_logging

Dataset = Literal["crossref-metadata", "datacite", "openalex_works"]


def normalise_affiliations(affiliations) -> Optional[list[dict]]:
    if isinstance(affiliations, dict):
        return [affiliations]
    elif isinstance(affiliations, list):
        return affiliations
    else:
        return []


def normalise_identifier(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    return re.sub(r"(?i)^https?://[^/]+/", "", value.strip()).lower()


def keep_record(dataset: Dataset, ror_id: str, institution_name: Optional[str], record: dict) -> bool:
    if dataset == "openalex-works":
        for authorship in record.get("authorships", []):
            for inst in authorship.get("institutions", []):
                identifier = inst.get("ror")
                name = inst.get("display_name")
                if normalise_identifier(identifier) == ror_id or (
                    institution_name is not None and name == institution_name
                ):
                    return True
        return False

    elif dataset == "datacite":
        for creator in record.get("attributes", {}).get("creators", []):
            for affiliation in normalise_affiliations(creator.get("affiliation", [])):
                identifier = affiliation.get("affiliationIdentifier")
                name = affiliation.get("name")
                if normalise_identifier(identifier) == ror_id or (
                    institution_name is not None and name == institution_name
                ):
                    return True
        return False

    elif dataset == "crossref-metadata":
        for author in record.get("author", []):
            for affiliation in author.get("affiliation", []):
                name = affiliation.get("name")
                if institution_name is not None and name == institution_name:
                    return True

                for id_struct in affiliation.get("id", []):
                    identifier = id_struct.get("id")
                    if normalise_identifier(identifier) == ror_id:
                        return True
        return False

    else:
        raise ValueError(f"keep_record: unknown dataset type {dataset}")


def get_file_glob(dataset: Dataset) -> str:
    if dataset == "openalex-works":
        return "**/*.gz"
    elif dataset == "datacite":
        return "**/*jsonl.gz"
    elif dataset == "crossref-metadata":
        return "*.jsonl.gz"
    else:
        raise ValueError(f"get_file_glob: unknown dataset type {dataset}")


def init_process_logs(level: int):
    logging.basicConfig(level=level, format="[%(asctime)s] [%(levelname)s] [%(processName)s] %(message)s")


def filter_dataset(
    dataset: Dataset, ror_id: str, institution_name: Optional[str], file_in: pathlib.Path, out_dir: pathlib
):
    logging.debug(f"start filtering {file_in}")

    worker_id = current_process()._identity[0]
    file_out = out_dir / f"part_{worker_id:03d}.jsonl.gz"

    total_filtered = 0
    with gzip.open(file_out, mode="ab") as f_out:
        with gzip.open(file_in, "rt", encoding="utf-8") as f_in:
            for line in f_in:
                if line.strip():
                    record = orjson.loads(line)
                    if keep_record(dataset, ror_id, institution_name, record):
                        f_out.write(line.encode("utf-8"))  # line already ends with newline
                        total_filtered += 1

    logging.debug(f"end filtering {file_in}")

    return total_filtered


def create_demo_dataset(
    dataset: Dataset,
    ror_id: str,
    institution_name: Optional[str],
    in_dir: pathlib.Path,
    out_dir: pathlib.Path,
    log_level: int,
):
    logging.basicConfig(level=log_level)
    start = pendulum.now()

    is_empty = next(out_dir.iterdir(), None) is None
    if not is_empty:
        raise Exception(f"Output directory is not empty: {out_dir}")

    file_glob = get_file_glob(dataset)
    files = list(pathlib.Path(in_dir).glob(file_glob))
    futures = []

    try:
        with ProcessPoolExecutor(
            max_workers=os.cpu_count(), initializer=init_process_logs, initargs=(log_level,)
        ) as executor:
            for file_in in files:
                future = executor.submit(filter_dataset, dataset, ror_id, institution_name, file_in, out_dir)
                futures.append(future)

            total_files = len(files)
            total_filtered = 0
            total_errors = 0
            with tqdm(total=total_files, desc=f"Filter {dataset}", unit="file") as pbar:
                for i, future in enumerate(as_completed(futures)):
                    try:
                        total_filtered += future.result()
                    except Exception as exc:
                        logging.error(exc)
                        total_errors += 1
                    pbar.update(1)
                    pbar.set_postfix({"Filtered": f"{total_filtered:,}", "Errors": f"{total_errors:,}"})
    except KeyboardInterrupt:
        logging.info(f"Shutting down...")
        executor.shutdown(wait=True, cancel_futures=True)

    end = pendulum.now()
    diff = end - start
    logging.info(f"Execution time: {diff.in_words()}")


def setup_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "dataset",
        choices=["crossref-metadata", "datacite", "openalex-works"],
        help="The dataset to filter.",
    )
    parser.add_argument(
        "ror_id",
        type=str,
        help="A ROR ID without a prefix used to filter records.",
    )
    parser.add_argument(
        "in_dir",
        type=pathlib.Path,
        help="Path to the dataset directory (e.g. /path/to/openalex_works)",
    )
    parser.add_argument(
        "out_dir",
        type=pathlib.Path,
        help="Path to the output directory (e.g. /path/to/demo_dataset/openalex).",
    )
    parser.add_argument(
        "--institution-name",
        type=str,
        help="The name of the institution to filter",
    )
    log_levels = ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"]
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help=f"Logging verbosity. Choices: {', '.join(log_levels)} (default: %(default)s)",
    )

    # Callback function
    parser.set_defaults(func=handle_command)


def handle_command(args: argparse.Namespace):
    setup_multiprocessing_logging(logging.getLevelName(args.log_level))

    # Validate
    errors = []
    if not args.in_dir.is_dir():
        errors.append(f"in_dir '{args.in_dir}' is not a valid directory.")

    if not args.out_dir.is_dir():
        errors.append(f"out_dir '{args.out_dir}' is not a valid directory.")

    create_demo_dataset(
        args.dataset,
        args.ror_id,
        args.institution_name,
        args.in_dir,
        args.out_dir,
        args.log_level,
    )


def main():
    parser = argparse.ArgumentParser(description="Produce a demo versions of each dataset.")
    setup_parser(parser)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
