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
    create_content_table,
    create_dmps_content_table,
    create_embedding_model,
    generate_embeddings,
    match_intermediate,
    match_vector_search,
    normalise_crossref,
    normalise_datacite,
    normalise_dmps,
    normalise_openalex,
    run_sql_template,
)
from dmptool_workflows.dmp_match_workflow.release import DMPToolMatchRelease

DATASET_API_ENTITY_ID = "dmp_match"


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


def fetch_dmps(
    *,
    dmptool_api: DMPToolAPI,
    dataset_api: DatasetAPI,
    dag_id: str,
    run_id: str,
    bucket_name: str,
    project_id: str,
    bq_dataset_id: str,
    entity_id: str = DATASET_API_ENTITY_ID,
    bq_client: bigquery.Client = None,
    **context,
) -> DMPToolMatchRelease:
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

    # Upload files to cloud storage
    success = gcs.gcs_upload_files(bucket_name=bucket_name, file_paths=file_paths)
    if not success:
        raise AirflowException(f"Error uploading files {file_paths} to bucket {bucket_name}")

    # Load BigQuery table
    dmp_dataset = DMPDataset(project_id, bq_dataset_id, release_date)
    table_id = dmp_dataset.dmps_raw
    uri = gcs.gcs_blob_uri(bucket_name, "*.jsonl.gz")
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
        ror_table_id=ao_dataset.ror_dataset.ror,
        dmps_raw_table_id=dt_dataset.dmp_dataset.dmps_raw,
        dmps_norm_table_id=dt_dataset.dmp_dataset.normalised,
        dry_run=dry_run,
        bq_client=bq_client,
    )
    normalise_openalex(
        dataset_id=dataset_id,
        openalex_works_table_id=ao_dataset.openalex_dataset.works,
        openalex_funders_table_id=ao_dataset.openalex_dataset.funders,
        crossref_metadata_table_id=ao_dataset.crossref_metadata_dataset.crossref_metadata,
        datacite_table_id=ao_dataset.datacite_dataset.datacite,
        dmps_norm_table_id=dt_dataset.dmp_dataset.normalised,
        openalex_norm_table_id=dt_dataset.openalex_match_dataset.normalised,
        dry_run=dry_run,
        bq_client=bq_client,
    )
    normalise_crossref(
        dataset_id=dataset_id,
        crossref_metadata_table_id=ao_dataset.crossref_metadata_dataset.crossref_metadata,
        ror_table_id=ao_dataset.ror_dataset.ror,
        openalex_norm_table_id=dt_dataset.openalex_match_dataset.normalised,
        dmps_norm_table_id=dt_dataset.dmp_dataset.normalised,
        crossref_norm_table_id=dt_dataset.crossref_match_dataset.normalised,
        dry_run=dry_run,
        bq_client=bq_client,
    )
    normalise_datacite(
        dataset_id=dataset_id,
        datacite_table_id=ao_dataset.datacite_dataset.datacite,
        ror_table_id=ao_dataset.ror_dataset.ror,
        dmps_norm_table_id=dt_dataset.dmp_dataset.normalised,
        openalex_norm_table_id=dt_dataset.openalex_match_dataset.normalised,
        datacite_norm_table_id=dt_dataset.datacite_match_dataset.normalised,
        dry_run=dry_run,
        bq_client=bq_client,
    )

    # Generate intermediate matches with the DMP table
    for match in dt_dataset.match_datasets:
        match_intermediate(
            dataset_id=dataset_id,
            dmps_norm_table_id=dt_dataset.dmp_dataset.normalised,
            match_norm_table_id=match.normalised,
            match_intermediate_table_id=match.match_intermediate,
            weighted_count_threshold=weighted_count_threshold,
            max_matches=max_matches,
            dry_run=dry_run,
            dry_run_id=match.name,
            bq_client=bq_client,
        )

    # Generate content tables
    create_dmps_content_table(
        dataset_id=dataset_id,
        dmps_norm_table_id=dt_dataset.dmp_dataset.normalised,
        dmps_content_table_id=dt_dataset.dmp_dataset.content,
        dry_run=dry_run,
        bq_client=bq_client,
    )
    for match in dt_dataset.match_datasets:
        create_content_table(
            dataset_id=dataset_id,
            match_norm_table_id=match.normalised,
            match_intermediate_table_id=match.match_intermediate,
            match_content_table_id=match.content,
            dry_run=dry_run,
            dry_run_id=match.name,
            bq_client=bq_client,
        )

    # Generate embeddings for intermediate matches
    for match in dt_dataset.all_datasets:
        generate_embeddings(
            dataset_id=dataset_id,
            content_table_id=match.content,
            embedding_model_id=embedding_model_id,
            embeddings_table_id=match.content_embeddings,
            dry_run=dry_run,
            dry_run_id=match.name,
            bq_client=bq_client,
        )

    # Add vector search to matches
    for match in dt_dataset.match_datasets:
        match_vector_search(
            dataset_id=dataset_id,
            match_intermediate_table_id=match.match_intermediate,
            match_norm_table_id=match.normalised,
            dmps_norm_table_id=dt_dataset.dmp_dataset.normalised,
            match_embeddings_table_id=match.content_embeddings,
            dmps_embeddings_table_id=dt_dataset.dmp_dataset.content_embeddings,
            match_table_id=match.match,
            dry_run=dry_run,
            dry_run_id=match.name,
            bq_client=bq_client,
        )


def export_matches(
    *,
    dag_id: str,
    project_id: str,
    dataset_id: str,
    release_date: pendulum.Date,
    bucket_name: str,
    bq_client: bigquery.Client = None,
):
    dt_dataset = DMPToolDataset(project_id, dataset_id, release_date)
    for match in dt_dataset.match_datasets:
        table_id = match.match
        destination_uri = match.destination_uri(bucket_name, dag_id)
        state = bq.bq_export_table(
            table_id=match.match, file_type="jsonl.gz", destination_uri=destination_uri, client=bq_client
        )
        if not state:
            raise AirflowException(f"export_matches: error exporting {table_id} to {destination_uri}")


def submit_matches(
    *,
    dmptool_api: DMPToolAPI,
    bucket_name: str,
    bucket_prefix: str,
    export_folder: str,
    gcs_client: storage.Client = None,
):
    # Download files from bucket
    success = gcs.gcs_download_blobs(
        bucket_name=bucket_name, prefix=bucket_prefix, destination_path=export_folder, client=gcs_client
    )
    if not success:
        raise AirflowException(f"submit_matches: failed to download files from bucket {bucket_name}/{bucket_prefix}")

    # List files
    file_paths = list_files(export_folder, r"^*.\.jsonl\.gz$")
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
    api.seed_db()
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
