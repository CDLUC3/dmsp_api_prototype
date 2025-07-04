import logging

from cyclopts import App

from dmpworks.batch.utils import clean_s3_prefix, download_from_s3, local_path, s3_uri, upload_to_s3
from dmpworks.transform.openalex_funders import FUNDERS_SCHEMA, transform_funders
from dmpworks.transform.pipeline import process_files_parallel
from dmpworks.transform.utils_file import read_jsonls, setup_multiprocessing_logging
from dmpworks.utils import run_process

log = logging.getLogger(__name__)

DATASET = "openalex_funders"
app = App(name=DATASET, help="Manage OpenAlex funders dataset AWS batch pipeline")


@app.command
def download(bucket_name: str, task_id: str):
    setup_multiprocessing_logging(logging.INFO)

    target_uri = s3_uri(bucket_name, DATASET, task_id, "download")
    local_dir = local_path(DATASET, task_id, "download")

    clean_s3_prefix(target_uri)

    log.info("Downloading OpenAlex funders data")
    run_process(["s5cmd", "--no-sign-request", "cp", "s3://openalex/data/funders/*", f"{local_dir}/"])
    # TODO: s5cmd --no-sign-request cp "s3://datafile-beta/dois/*" "${download_path}/"
    # TODO: s5cmd --no-sign-request cp "s3://openalex/data/works/*" "${download_path}/"

    upload_to_s3(local_dir, target_uri)


@app.command
def transform(bucket_name: str, task_id: str):
    setup_multiprocessing_logging(logging.INFO)

    in_dir = local_path(DATASET, task_id, "download")
    out_dir = local_path(DATASET, task_id, "transform")
    target_uri = s3_uri(bucket_name, DATASET, task_id, "transform")

    clean_s3_prefix(target_uri)

    download_from_s3(f"{s3_uri(bucket_name, DATASET, task_id, "download")}*", in_dir)

    log.info("Transforming data")
    out_dir.mkdir(parents=True, exist_ok=True)
    process_files_parallel(
        in_dir=in_dir,
        out_dir=out_dir,
        schema=FUNDERS_SCHEMA,
        transform_func=transform_funders,
        file_glob="**/*.gz",
        read_func=read_jsonls,
    )

    upload_to_s3(out_dir / "parquets", f"{target_uri}parquets/", "*.parquet")


if __name__ == "__main__":
    app()
