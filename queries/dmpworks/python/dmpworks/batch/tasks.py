import logging
import pathlib
from contextlib import contextmanager
from dataclasses import dataclass

from dmpworks.batch.utils import clean_s3_prefix, download_from_s3, local_path, s3_uri, upload_to_s3

log = logging.getLogger(__name__)


@dataclass
class DownloadTaskContext:
    local_dir: pathlib.Path
    target_uri: str


@contextmanager
def download_source_task(bucket_name: str, dataset: str, task_id: str):
    target_uri = s3_uri(bucket_name, dataset, task_id, "download")
    local_dir = local_path(dataset, task_id, "download")

    clean_s3_prefix(target_uri)
    upload_to_s3(local_dir, target_uri)

    log.info(f"Downloading {dataset}")
    ctx = DownloadTaskContext(
        local_dir=local_dir,
        target_uri=target_uri,
    )
    yield ctx

    upload_to_s3(local_dir, target_uri)


@dataclass
class TransformTaskContext:
    in_dir: pathlib.Path
    out_dir: pathlib.Path
    target_uri: str


@contextmanager
def transform_parquets_task(bucket_name: str, dataset: str, task_id: str):
    in_dir = local_path(dataset, task_id, "transform")
    out_dir = local_path(dataset, task_id, "transform")
    target_uri = s3_uri(bucket_name, dataset, task_id, "transform")

    clean_s3_prefix(target_uri)

    download_from_s3(f"{s3_uri(bucket_name, dataset, task_id, "download")}*", in_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"Transforming {dataset}")
    ctx = TransformTaskContext(
        in_dir=in_dir,
        out_dir=out_dir,
        target_uri=target_uri,
    )
    yield ctx

    upload_to_s3(out_dir / "parquets", f"{target_uri}parquets/", "*.parquet")
