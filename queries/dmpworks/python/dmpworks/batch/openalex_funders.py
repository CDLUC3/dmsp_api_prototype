import logging
from typing import Optional

from cyclopts import App

from dmpworks.batch.tasks import download_source_task, transform_parquets_task
from dmpworks.transform.cli import OpenAlexFundersConfig
from dmpworks.transform.openalex_funders import transform_openalex_funders
from dmpworks.transform.utils_file import setup_multiprocessing_logging
from dmpworks.utils import copy_dict, run_process

log = logging.getLogger(__name__)

DATASET = "openalex_funders"
app = App(name="openalex-funders", help="OpenAlex Funders AWS Batch pipeline.")


@app.command(name="download")
def download_cmd(bucket_name: str, task_id: str):
    """Download OpenAlex Funders from the OpenAlex S3 bucket and upload it to
    the DMP Tool S3 bucket.

    Args:
        bucket_name: S3 bucket name.
        task_id: a unique task ID.
    """

    setup_multiprocessing_logging(logging.INFO)

    with download_source_task(bucket_name, DATASET, task_id) as ctx:
        run_process(
            [
                "s5cmd",
                "--no-sign-request",
                "cp",
                "s3://openalex/data/funders/*",
                f"{ctx.local_dir}/",
            ],
        )


@app.command(name="transform")
def transform_cmd(
    bucket_name: str,
    task_id: str,
    *,
    config: Optional[OpenAlexFundersConfig] = None,
):
    """Download OpenAlex Funders from the DMP Tool S3 bucket, transform it to
    Parquet format, and upload the results to same bucket.

    Args:
        bucket_name: S3 bucket name.
        task_id: a unique task ID.
        config: optional configuration parameters.
    """

    config = OpenAlexFundersConfig() if config is None else config
    setup_multiprocessing_logging(logging.INFO)

    with transform_parquets_task(bucket_name, DATASET, task_id) as ctx:
        transform_openalex_funders(
            in_dir=ctx.in_dir,
            out_dir=ctx.out_dir,
            **copy_dict(vars(config), ["log_level"]),
        )
