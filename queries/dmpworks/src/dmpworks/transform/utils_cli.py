import argparse
import os
import sys
from typing import Optional


def add_common_args(
    *,
    parser: argparse.ArgumentParser,
    batch_size: int = os.cpu_count(),
    extract_workers: int = 1,
    transform_workers: int = 1,
    cleanup_workers: int = 1,
    extract_queue_size: int = 0,
    transform_queue_size: int = 0,
    cleanup_queue_size: int = 0,
    max_file_processes: int = os.cpu_count(),
    n_batches: Optional[int] = None,
):
    parser.add_argument(
        "--batch-size",
        type=int,
        default=batch_size,
        help="Number of files to process per batch (must be >= 1) (default: nbr of available CPUs).",
    )
    parser.add_argument(
        "--extract-workers",
        type=int,
        default=extract_workers,
        help=f"Number of parallel workers for file extraction (must be >= 1). (default: {extract_workers})",
    )
    parser.add_argument(
        "--transform-workers",
        type=int,
        default=transform_workers,
        help=f"Number of parallel workers for file transformation (must be >= 1). (default: {transform_workers})",
    )
    parser.add_argument(
        "--cleanup-workers",
        type=int,
        default=cleanup_workers,
        help=f"Number of parallel workers for file cleanup (must be >= 1). (default: {cleanup_workers})",
    )
    parser.add_argument(
        "--extract-queue-size",
        type=int,
        default=extract_queue_size,
        help=f"File extraction queue size (must be >= 0, zero is unlimited). (default: {extract_queue_size})",
    )
    parser.add_argument(
        "--transform-queue-size",
        type=int,
        default=transform_queue_size,
        help=f"File transform queue size (must be >= 0, zero is unlimited). (default: {transform_queue_size})",
    )
    parser.add_argument(
        "--cleanup-queue-size",
        type=int,
        default=cleanup_queue_size,
        help=f"File cleanup queue size (must be >= 0, zero is unlimited). (default: {cleanup_queue_size})",
    )
    parser.add_argument(
        "--max-file-processes",
        type=int,
        default=max_file_processes,
        help=f"Number of processes to use when extracting files in parallel (must be >= 1). (default: nbr of available CPUs)",
    )
    parser.add_argument(
        "--n-batches",
        type=int,
        default=n_batches,
        help=f"Set an explicit number of batches to process (e.g. for testing purposes). (default: {n_batches})",
    )
    parser.add_argument(
        "--low-memory",
        action="store_true",
        help="Enable low memory mode for Polars when streaming records from files.",
    )


def validate_common_args(args: argparse.Namespace, errors: list[str]):
    if args.batch_size < 1:
        errors.append(f"--batch-size must be ≥ 1, got {args.batch_size}.")

    if args.extract_workers < 1:
        errors.append(f"--extract-workers must be ≥ 1, got {args.extract_workers}.")

    if args.transform_workers < 1:
        errors.append(f"--transform-workers must be ≥ 1, got {args.transform_workers}.")

    if args.cleanup_workers < 1:
        errors.append(f"--cleanup-workers must be ≥ 1, got {args.cleanup_workers}.")

    if args.extract_queue_size < 0:
        errors.append(f"--extract-queue-size must be ≥ 0, got {args.extract_queue_size}.")

    if args.transform_queue_size < 0:
        errors.append(f"--transform-queue-size must be ≥ 0, got {args.transform_queue_size}.")

    if args.cleanup_queue_size < 0:
        errors.append(f"--cleanup-queue-size must be ≥ 0, got {args.cleanup_queue_size}.")

    if args.max_file_processes < 1:
        errors.append(f"--max-file-processes must be ≥ 1, got {args.max_file_processes}.")

    if args.n_batches is not None and args.n_batches < 1:
        errors.append(f"--n-batches must be None or ≥ 1, got {args.n_batches}.")


def handle_errors(errors: list[str]):
    # If there are errors, print them and exit
    if errors:
        for error in errors:
            print(f"error: {error}", file=sys.stderr)
        sys.exit(2)


def copy_dict(original_dict: dict, keys_to_remove: list) -> dict:
    return {k: v for k, v in original_dict.items() if k not in keys_to_remove}
