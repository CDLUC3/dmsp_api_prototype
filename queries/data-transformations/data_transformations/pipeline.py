import logging
import os
import pathlib
import queue
import shutil
import threading
from abc import ABC, abstractmethod
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Callable, Optional

import pendulum
import polars as pl
from polars._typing import SchemaDefinition
from tqdm import tqdm

from utils import log_stage, write_parquet, extract_gzip, read_jsonls, batch_files


TransformFunc = Callable[[pl.LazyFrame], list[tuple[str, pl.LazyFrame]]]


class FileExtractor:
    def __init__(self, extract_func: Callable[[Path, Path], None], in_dir: Path, out_dir: Path):
        self.extract_func = extract_func
        self.in_dir = in_dir
        self.out_dir = out_dir

    def __call__(self, in_file: Path) -> Path:
        out_file = self.out_dir / "extract" / in_file.relative_to(self.in_dir).with_suffix("")
        out_file.parent.mkdir(parents=True, exist_ok=True)
        self.extract_func(in_file, out_file)
        return out_file


class BatchTransformer:
    def __init__(
        self,
        read_func: Callable[[list[Path], SchemaDefinition, bool], pl.LazyFrame],
        transform_func: TransformFunc,
        schema: SchemaDefinition,
        low_memory: bool,
        out_dir: Path,
    ):
        self.read_func = read_func
        self.transform_func = transform_func
        self.schema = schema
        self.low_memory = low_memory
        self.out_dir = out_dir

    def __call__(self, idx: int, batch: list[Path]):
        # batch_non_empty = [file for file in batch if file.stat().st_size > 0]
        lz = self.read_func(batch, self.schema, self.low_memory)
        results = self.transform_func(lz)
        for table_name, lz_frame in results:
            parquet_file = self.out_dir / "parquets" / f"{table_name}_{idx}.parquet"
            parquet_file.parent.mkdir(parents=True, exist_ok=True)
            write_parquet(lz_frame, parquet_file)


class BaseWorker(threading.Thread, ABC):
    def __init__(
        self,
        *,
        input_queue: queue.Queue,
        output_queue: queue.Queue,
        shutdown_event: threading.Event,
        name: Optional[str] = None,
    ):
        super().__init__(daemon=True, name=name)
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.shutdown_event = shutdown_event

    def run(self):
        try:
            while not self.shutdown_event.is_set():
                try:
                    task = self.input_queue.get(timeout=1)
                except queue.Empty:
                    continue

                if task is None:
                    break

                idx, batch = task

                try:
                    self.process_task(idx, batch)
                except Exception as e:
                    logging.exception(f"Error processing batch={idx}, files={batch} in {self.name}", e)
                    self.shutdown_event.set()
                # finally:
                #     if not self.input_queue.all_tasks_done:
                #         self.input_queue.task_done()
        finally:
            self.on_shutdown()

    @abstractmethod
    def process_task(self, idx: int, batch: list[Path]):
        """Process the given task. Must be implemented by subclasses."""
        pass

    @abstractmethod
    def on_shutdown(self):
        """Override in subclasses to perform cleanup when the thread exits."""
        pass


class ExtractWorker(BaseWorker):
    def __init__(
        self,
        *,
        input_queue: queue.Queue,
        output_queue: queue.Queue,
        shutdown_event: threading.Event,
        file_extractor: Optional[FileExtractor] = None,
        max_processes: int = os.cpu_count(),
        name: str = None,
    ):
        super().__init__(input_queue=input_queue, output_queue=output_queue, shutdown_event=shutdown_event, name=name)
        self.file_extractor = file_extractor
        self.max_processes = max_processes
        self.daemon = True
        self.executor = ProcessPoolExecutor(max_workers=self.max_processes)

    def process_task(self, idx: int, batch: list[Path]):
        # Extract files with ProcessPoolExecutor
        log_stage("EXTRACT", "start", idx)
        futures = []
        if self.file_extractor is not None:
            for file in batch:
                futures.append(self.executor.submit(self.file_extractor, file))

        # Wait for batch to finish
        extracted_files = []
        for future in futures:
            extracted_files.append(future.result())

        # Queue output
        self.output_queue.put((idx, extracted_files))
        self.input_queue.task_done()
        log_stage("EXTRACT", "end", idx)

    def on_shutdown(self):
        if self.executor is not None:
            self.executor.shutdown()


class TransformWorker(BaseWorker):
    def __init__(
        self,
        *,
        input_queue: queue.Queue,
        output_queue: queue.Queue,
        shutdown_event: threading.Event,
        batch_transformer: BatchTransformer,
        name: str = None,
    ):
        super().__init__(input_queue=input_queue, output_queue=output_queue, shutdown_event=shutdown_event, name=name)
        self.batch_transformer = batch_transformer
        self.daemon = True

    def process_task(self, idx: int, batch: list[Path]):
        log_stage("TRANSFORM", "start", idx)
        self.batch_transformer(idx, batch)

        # Queue output
        self.output_queue.put((idx, batch))
        self.input_queue.task_done()
        log_stage("TRANSFORM", "end", idx)

    def on_shutdown(self):
        pass


class CleanupWorker(BaseWorker):
    def __init__(
        self,
        *,
        input_queue: queue.Queue,
        output_queue: queue.Queue,
        shutdown_event: threading.Event,
        name: str = None,
    ):
        super().__init__(input_queue=input_queue, output_queue=output_queue, shutdown_event=shutdown_event, name=name)
        self.daemon = True

    def process_task(self, idx: int, batch: list[Path]):
        log_stage("CLEANUP", "start", idx)
        [file.unlink(missing_ok=True) for file in batch]
        self.output_queue.put(idx)
        self.input_queue.task_done()
        log_stage("CLEANUP", "end", idx)

    def on_shutdown(self):
        pass


class Pipeline:
    def __init__(
        self,
        *,
        file_extractor: Optional[FileExtractor],
        batch_transformer: BatchTransformer,
        extract_workers: int = 1,
        transform_workers: int = 1,
        cleanup_workers: int = 1,
        extract_queue_size: int = 0,
        transform_queue_size: int = 0,
        cleanup_queue_size: int = 0,
        max_file_processes: int = os.cpu_count(),
    ):
        logging.basicConfig(level=logging.DEBUG, format="[%(asctime)s] [%(levelname)s] [%(threadName)s] %(message)s")
        self.shutdown_event = threading.Event()
        self.extract_queue = queue.Queue(maxsize=extract_queue_size)
        self.transform_queue = queue.Queue(maxsize=transform_queue_size)
        self.cleanup_queue = queue.Queue(maxsize=cleanup_queue_size)
        self.completed_queue = queue.Queue()
        self.extract_workers = [
            ExtractWorker(
                input_queue=self.extract_queue,
                output_queue=self.transform_queue,
                shutdown_event=self.shutdown_event,
                file_extractor=file_extractor,
                max_processes=max_file_processes,
                name=f"Extract-{i}",
            )
            for i in range(extract_workers)
        ]
        self.transform_workers = [
            TransformWorker(
                input_queue=self.transform_queue,
                output_queue=self.cleanup_queue,
                shutdown_event=self.shutdown_event,
                batch_transformer=batch_transformer,
                name=f"Transform-{i}",
            )
            for i in range(transform_workers)
        ]
        self.cleanup_workers = [
            CleanupWorker(
                input_queue=self.cleanup_queue,
                output_queue=self.completed_queue,
                shutdown_event=self.shutdown_event,
                name=f"Cleanup-{i}",
            )
            for i in range(cleanup_workers)
        ]

    def start(self, batches: list[list[Path]]):
        num_batches = len(batches)
        workers = self.extract_workers + self.transform_workers + self.cleanup_workers

        try:
            # Start workers
            for worker in workers:
                worker.start()

            with tqdm(total=num_batches, desc="Transformation Pipeline", unit="batch") as pbar:
                # Fill extract queue
                for idx, batch in enumerate(batches):
                    logging.debug(f"Queuing batch: {idx}")
                    self.extract_queue.put((idx, batch))

                # Wait for tasks to complete
                num_completed = 0
                while num_completed < len(batches) and not self.shutdown_event.is_set():
                    try:
                        idx = self.completed_queue.get(timeout=1)
                        logging.info(f"Task completed: {idx}")
                        if idx is None:
                            break

                        num_completed += 1
                        pbar.update(1)
                        self.completed_queue.task_done()
                    except queue.Empty:
                        logging.debug(f"Completed queue empty")
                        continue

        except KeyboardInterrupt:
            logging.info("Interrupted by user")
            self.shutdown_event.set()

        finally:
            # Signal shutdown
            for q in [self.extract_queue, self.transform_queue, self.cleanup_queue]:
                q.put(None)

            # # Join threads
            # for worker in workers:
            #     worker.join()


def process_files_parallel(
    *,
    in_dir: pathlib.Path,
    out_dir: pathlib.Path,
    schema: SchemaDefinition,
    transform_func: TransformFunc,
    file_glob: str = "**/*.gz",
    extract_func: Callable[[Path, Path], None] = extract_gzip,
    read_func: Callable[[list[Path], SchemaDefinition, bool], pl.LazyFrame] = read_jsonls,
    batch_size: int = os.cpu_count(),
    extract_workers: int = 1,
    transform_workers: int = 1,
    cleanup_workers: int = 1,
    extract_queue_size: int = 0,
    transform_queue_size: int = 0,
    cleanup_queue_size: int = 0,
    max_file_processes: int = os.cpu_count(),
    n_batches: Optional[int] = None,
    low_memory: bool = False,
):
    start = pendulum.now()

    logging.info(f"in_dir: {in_dir}")
    logging.info(f"out_dir: {out_dir}")
    logging.info(f"schema: {schema}")
    logging.info(f"transform_func: {transform_func.__name__}")
    logging.info(f"batch_size: {batch_size}")
    logging.info(f"extract_workers: {extract_workers}")
    logging.info(f"transform_workers: {transform_workers}")
    logging.info(f"cleanup_workers: {cleanup_workers}")
    logging.info(f"extract_queue_size: {extract_queue_size}")
    logging.info(f"transform_queue_size: {transform_queue_size}")
    logging.info(f"cleanup_queue_size: {cleanup_queue_size}")
    logging.info(f"max_file_processes: {max_file_processes}")
    logging.info(f"n_batches: {n_batches}")
    logging.info(f"low_memory: {low_memory}")

    # Cleanup existing output directory
    shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Build file extract and read functions
    file_extractor = None if extract_func is None else FileExtractor(extract_func, in_dir, out_dir)
    batch_transformer = BatchTransformer(read_func, transform_func, schema, low_memory, out_dir)

    # Process batches in parallel
    files = list(Path(in_dir).glob(file_glob))
    batches = list(batch_files(files, batch_size))
    if n_batches is not None:
        batches = batches[:n_batches]
    pipeline = Pipeline(
        file_extractor=file_extractor,
        batch_transformer=batch_transformer,
        extract_workers=extract_workers,
        transform_workers=transform_workers,
        cleanup_workers=cleanup_workers,
        extract_queue_size=extract_queue_size,
        transform_queue_size=transform_queue_size,
        cleanup_queue_size=cleanup_queue_size,
        max_file_processes=max_file_processes,
    )
    pipeline.start(batches)

    end = pendulum.now()
    diff = end - start
    logging.info(f"Execution time: {diff.in_words()}")
