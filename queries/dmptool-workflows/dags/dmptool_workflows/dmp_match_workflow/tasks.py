import logging
import os
import urllib.request

import observatory_platform.google.bigquery as bq
import observatory_platform.google.gcs as gcs
import pendulum
from airflow import AirflowException
from google.cloud import bigquery, storage
from google.cloud.bigquery import SourceFormat
from observatory_platform.dataset_api import DatasetAPI, DatasetRelease
from observatory_platform.files import list_files

from dmptool_workflows.config import project_path
from dmptool_workflows.dmp_match_workflow.academic_observatory_dataset import AcademicObservatoryDataset
from dmptool_workflows.dmp_match_workflow.dmptool_api import DMPToolAPI
from dmptool_workflows.dmp_match_workflow.dmptool_dataset import DMPDataset, DMPToolDataset
from dmptool_workflows.dmp_match_workflow.queries import (
    create_dmps_content_table,
    create_embedding_model,
    match_intermediate,
    match_vector_search,
    normalise_crossref,
    normalise_datacite,
    normalise_dmps,
    normalise_openalex,
    run_sql_template,
    update_content_table,
    update_embeddings,
)
from dmptool_workflows.dmp_match_workflow.release import DMPToolMatchRelease

DATASET_API_ENTITY_ID = "dmp_match"
DATE_FORMAT = "%Y-%m-%d"


def create_bq_dataset(
    *,
    project_id: str,
    dataset_id: str,
    location: str,
    table_expiration_days: int,
    bq_client: bigquery.Client = None,
):
    if bq_client is None:
        bq_client = bigquery.Client()

    # Create dataset
    bq.bq_create_dataset(
        project_id=project_id,
        dataset_id=dataset_id,
        location=location,
        description="The BigQuery dataset for matching academic works to DMPs",
        client=bq_client,
    )

    # Set expiration time in milliseconds
    table_expiration_ms = table_expiration_days * 24 * 60 * 60 * 1000
    dataset = bq_client.get_dataset(f"{project_id}.{dataset_id}")
    if dataset.default_table_expiration_ms != table_expiration_ms:
        dataset.default_table_expiration_ms = table_expiration_ms
        bq_client.update_dataset(dataset, ["default_table_expiration_ms"])
        logging.info(f"Updated dataset {dataset_id} with default table expiration of {table_expiration_days} days.")
    else:
        logging.info(f"Dataset {dataset_id} already has expiration of {table_expiration_days} days.")


def download_dmps(
    *,
    dmptool_api: DMPToolAPI,
    dataset_api: DatasetAPI,
    dag_id: str,
    run_id: str,
    entity_id: str = DATASET_API_ENTITY_ID,
):
    # Get release date and list of latest DMP files to download
    latest_files, release_date = dmptool_api.fetch_dmps()

    logging.info(f"Release date: {release_date}")
    logging.info(f"Discovered DMPs: {[file_name for file_name, _ in latest_files]}")

    # Get previous release and on first run check that previous releases removed
    prev_release = dataset_api.get_latest_dataset_release(dag_id=dag_id, entity_id=entity_id, date_key="snapshot_date")
    if prev_release is not None and release_date <= prev_release.snapshot_date:
        raise ValueError(f"fetch_dmps: already processed release {release_date}")

    # Download files
    release = DMPToolMatchRelease(
        dag_id=dag_id,
        run_id=run_id,
        snapshot_date=release_date,
    )
    file_paths = []
    for file_name, file_url in latest_files:
        file_path = os.path.join(release.dmps_folder, file_name)
        file_paths.append(file_path)
        urllib.request.urlretrieve(file_url, file_path)

    return file_paths, release


def fetch_dmps(
    *,
    dmptool_api: DMPToolAPI,
    dataset_api: DatasetAPI,
    dag_id: str,
    run_id: str,
    bucket_name: str,
    project_id: str,
    bq_dataset_id: str,
    bq_client: bigquery.Client = None,
    **context,
) -> DMPToolMatchRelease:
    # Download DMPs
    file_paths, release = download_dmps(dmptool_api=dmptool_api, dataset_api=dataset_api, dag_id=dag_id, run_id=run_id)

    # Upload files to cloud storage
    success = gcs.gcs_upload_files(bucket_name=bucket_name, file_paths=file_paths)
    if not success:
        raise AirflowException(f"Error uploading files {file_paths} to bucket {bucket_name}")

    # Load BigQuery table
    dmp_dataset = DMPDataset(project_id, bq_dataset_id, release.snapshot_date)
    table_id = dmp_dataset.dmps_raw_table_id
    uri = gcs.gcs_blob_uri(bucket_name, gcs.gcs_blob_name_from_path(os.path.join(release.dmps_folder, "*.jsonl.gz")))
    success = bq.bq_load_table(
        uri=uri,
        table_id=table_id,
        source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
        schema_file_path=project_path("dmp_match_workflow", "schema", "dmps.json"),
        client=bq_client,
    )
    if not success:
        raise AirflowException(f"fetch_dmps: error loading {table_id}")

    return release


def create_dmp_matches(
    *,
    ao_project_id: str,
    dmps_project_id: str,
    dataset_id: str,
    release_date: pendulum.Date,
    vertex_ai_model_id: str,
    weighted_count_threshold: int,
    max_matches: int,
    dry_run: bool = False,
    bq_client: bigquery.Client = None,
):
    ao_dataset = AcademicObservatoryDataset(ao_project_id)
    dt_dataset = DMPToolDataset(dmps_project_id, dataset_id, release_date)

    # Create shared functions and embedding model
    embedding_model_id = f"{dataset_id}.embedding_model"
    run_sql_template("shared_functions", dataset_id, dry_run=dry_run)
    create_embedding_model(
        dataset_id=dataset_id,
        embedding_model_id=embedding_model_id,
        vertex_ai_model_id=vertex_ai_model_id,
        dry_run=dry_run,
        bq_client=bq_client,
    )

    # Normalise datasets
    normalise_dmps(
        dataset_id=dataset_id,
        ror_table_id=ao_dataset.ror_dataset.ror_table_id,
        dmps_raw_table_id=dt_dataset.dmp_dataset.dmps_raw_table_id,
        dmps_norm_table_id=dt_dataset.dmp_dataset.normalised_table_id,
        dry_run=dry_run,
        bq_client=bq_client,
    )
    normalise_openalex(
        dataset_id=dataset_id,
        openalex_works_table_id=ao_dataset.openalex_dataset.works_table_id,
        openalex_funders_table_id=ao_dataset.openalex_dataset.funders_table_id,
        crossref_metadata_table_id=ao_dataset.crossref_metadata_dataset.crossref_metadata_table_id,
        datacite_table_id=ao_dataset.datacite_dataset.datacite_table_id,
        dmps_norm_table_id=dt_dataset.dmp_dataset.normalised_table_id,
        openalex_norm_table_id=dt_dataset.openalex_match_dataset.normalised_table_id,
        dry_run=dry_run,
        bq_client=bq_client,
    )
    normalise_crossref(
        dataset_id=dataset_id,
        crossref_metadata_table_id=ao_dataset.crossref_metadata_dataset.crossref_metadata_table_id,
        ror_table_id=ao_dataset.ror_dataset.ror_table_id,
        openalex_norm_table_id=dt_dataset.openalex_match_dataset.normalised_table_id,
        dmps_norm_table_id=dt_dataset.dmp_dataset.normalised_table_id,
        crossref_norm_table_id=dt_dataset.crossref_match_dataset.normalised_table_id,
        dry_run=dry_run,
        bq_client=bq_client,
    )
    normalise_datacite(
        dataset_id=dataset_id,
        datacite_table_id=ao_dataset.datacite_dataset.datacite_table_id,
        ror_table_id=ao_dataset.ror_dataset.ror_table_id,
        dmps_norm_table_id=dt_dataset.dmp_dataset.normalised_table_id,
        openalex_norm_table_id=dt_dataset.openalex_match_dataset.normalised_table_id,
        datacite_norm_table_id=dt_dataset.datacite_match_dataset.normalised_table_id,
        dry_run=dry_run,
        bq_client=bq_client,
    )

    # Generate intermediate matches with the DMP table
    for match in dt_dataset.match_datasets:
        match_intermediate(
            dataset_id=dataset_id,
            dmps_norm_table_id=dt_dataset.dmp_dataset.normalised_table_id,
            match_norm_table_id=match.normalised_table_id,
            match_intermediate_table_id=match.match_intermediate_table_id,
            weighted_count_threshold=weighted_count_threshold,
            max_matches=max_matches,
            dry_run=dry_run,
            dry_run_id=match.name,
            bq_client=bq_client,
        )

    # Generate content tables
    create_dmps_content_table(
        dataset_id=dataset_id,
        dmps_norm_table_id=dt_dataset.dmp_dataset.normalised_table_id,
        dmps_content_table_id=dt_dataset.dmp_dataset.content_table_id,
        dry_run=dry_run,
        bq_client=bq_client,
    )
    for dataset in dt_dataset.all_datasets:
        match_intermediate_table_id = None if dataset.name == "dmps" else dataset.match_intermediate_table_id
        update_content_table(
            dataset_id=dataset_id,
            dataset_name=dataset.name,
            content_table_id=dataset.content_table_id,
            embeddings_table_id=dataset.embeddings_table_id,
            norm_table_id=dataset.normalised_table_id,
            match_intermediate_table_id=match_intermediate_table_id,
            dry_run=dry_run,
            dry_run_id=dataset.name,
            bq_client=bq_client,
        )

    # Generate embeddings for intermediate matches
    for match in dt_dataset.all_datasets:
        update_embeddings(
            dataset_id=dataset_id,
            content_table_id=match.content_table_id,
            embedding_model_id=embedding_model_id,
            embeddings_table_id=match.embeddings_table_id,
            dry_run=dry_run,
            dry_run_id=match.name,
            bq_client=bq_client,
        )

    # Add vector search to matches
    for match in dt_dataset.match_datasets:
        match_vector_search(
            dataset_id=dataset_id,
            match_intermediate_table_id=match.match_intermediate_table_id,
            match_norm_table_id=match.normalised_table_id,
            dmps_norm_table_id=dt_dataset.dmp_dataset.normalised_table_id,
            match_embeddings_table_id=match.embeddings_table_id,
            dmps_embeddings_table_id=dt_dataset.dmp_dataset.embeddings_table_id,
            match_table_id=match.match_table_id,
            dry_run=dry_run,
            dry_run_id=match.name,
            bq_client=bq_client,
        )


def make_export_file_name(release_date: pendulum.Date, source: str) -> str:
    return f"coki-{source}_{release_date.strftime(DATE_FORMAT)}_*.jsonl.gz"


def export_matches(
    *,
    dag_id: str,
    project_id: str,
    dataset_id: str,
    release_date: pendulum.Date,
    bucket_name: str,
    export_folder_blob_name: str,
    bq_client: bigquery.Client = None,
):
    dt_dataset = DMPToolDataset(project_id, dataset_id, release_date)
    for match in dt_dataset.match_datasets:
        table_id = match.match_table_id
        destination_uri = gcs.gcs_blob_uri(
            bucket_name, f"{export_folder_blob_name}/{make_export_file_name(release_date, match.name)}"
        )
        state = bq.bq_export_table(
            table_id=match.match_table_id, file_type="jsonl.gz", destination_uri=destination_uri, client=bq_client
        )
        if not state:
            raise AirflowException(f"export_matches: error exporting {table_id} to {destination_uri}")


def submit_matches(
    *,
    dmptool_api: DMPToolAPI,
    bucket_name: str,
    export_folder_blob_name: str,
    export_folder: str,
    gcs_client: storage.Client = None,
):
    # Download files from bucket
    success = gcs.gcs_download_blobs(
        bucket_name=bucket_name, prefix=export_folder_blob_name, destination_path=export_folder, client=gcs_client
    )
    if not success:
        raise AirflowException(
            f"submit_matches: failed to download files from bucket {gcs.gcs_blob_uri(bucket_name, export_folder_blob_name)}"
        )

    # List files
    file_paths = list_files(export_folder, r"^.*\.jsonl\.gz$")
    if len(file_paths) == 0:
        raise AirflowException(f"submit_matches: no files downloaded")

    # Upload files
    for file_path in file_paths:
        dmptool_api.upload_match(file_path)


def add_dataset_release(
    *,
    dag_id: str,
    run_id: str,
    snapshot_date: pendulum.DateTime,
    bq_project_id: str,
    api_bq_dataset_id: str,
    entity_id: str = DATASET_API_ENTITY_ID,
):
    api = DatasetAPI(bq_project_id=bq_project_id, bq_dataset_id=api_bq_dataset_id)
    now = pendulum.now()
    dataset_release = DatasetRelease(
        dag_id=dag_id,
        entity_id=entity_id,
        dag_run_id=run_id,
        created=now,
        modified=now,
        snapshot_date=snapshot_date,
    )
    api.add_dataset_release(dataset_release)
