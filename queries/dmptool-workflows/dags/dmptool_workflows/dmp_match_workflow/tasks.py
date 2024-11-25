import logging

import google.cloud.bigquery
import google.cloud.storage
import observatory_platform.google.bigquery as bq
import observatory_platform.google.gcs as gcs
import pendulum
from airflow import AirflowException
from observatory_platform.files import load_jsonl

from dmptool_workflows.config import project_path
from dmptool_workflows.dmp_match_workflow.academic_observatory_dataset import AcademicObservatoryDataset
from dmptool_workflows.dmp_match_workflow.dmptool_dataset import DMPDataset, DMPToolDataset, make_prefix
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


def create_bq_dataset(
    *,
    project_id: str,
    dataset_id: str,
    location: str,
    table_expiration_days: int,
    client: google.cloud.bigquery.Client = None,
):
    # Create dataset
    dataset = bq.bq_create_dataset(
        project_id=project_id,
        dataset_id=dataset_id,
        location=location,
        description="The BigQuery dataset for matching academic works to DMPs",
        client=client,
    )

    # Set expiration time in milliseconds
    dataset.default_table_expiration_ms = table_expiration_days * 24 * 60 * 60 * 1000
    dataset = client.update_dataset(dataset, ["default_table_expiration_ms"])
    logging.info(f"Updated dataset {dataset_id} with default table expiration of {table_expiration_days} days.")


def fetch_dmps(
    *, project_id: str, bq_dataset_id: str, client: google.cloud.bigquery.Client = None, **context
) -> pendulum.DateTime:
    # Fetch mock data
    # TODO: fetch with Brian's API
    path = project_path("dmp_match_workflow", "data", "dmps20241007.jsonl")
    data = load_jsonl(path)
    release_date = pendulum.datetime(2024, 10, 7)

    # Load BigQuery table
    dmp_dataset = DMPDataset(project_id, bq_dataset_id, release_date)
    table_id = dmp_dataset.dmps_raw
    success = bq.bq_load_from_memory(
        table_id, data, schema_file_path=project_path("dmp_match_workflow", "schema", "dmps.json"), client=client
    )
    if not success:
        raise AirflowException(f"fetch_dmps: error loading {table_id}")

    return release_date


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
    client: google.cloud.bigquery.Client = None,
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
        client=client,
    )

    # Normalise datasets
    normalise_dmps(
        dataset_id=dataset_id,
        ror_table_id=ao_dataset.ror_dataset.ror,
        dmps_raw_table_id=dt_dataset.dmp_dataset.dmps_raw,
        dmps_norm_table_id=dt_dataset.dmp_dataset.normalised,
        dry_run=dry_run,
        client=client,
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
        client=client,
    )
    normalise_crossref(
        dataset_id=dataset_id,
        crossref_metadata_table_id=ao_dataset.crossref_metadata_dataset.crossref_metadata,
        ror_table_id=ao_dataset.ror_dataset.ror,
        openalex_norm_table_id=dt_dataset.openalex_match_dataset.normalised,
        dmps_norm_table_id=dt_dataset.dmp_dataset.normalised,
        crossref_norm_table_id=dt_dataset.crossref_match_dataset.normalised,
        dry_run=dry_run,
        client=client,
    )
    normalise_datacite(
        dataset_id=dataset_id,
        datacite_table_id=ao_dataset.datacite_dataset.datacite,
        ror_table_id=ao_dataset.ror_dataset.ror,
        dmps_norm_table_id=dt_dataset.dmp_dataset.normalised,
        openalex_norm_table_id=dt_dataset.openalex_match_dataset.normalised,
        datacite_norm_table_id=dt_dataset.datacite_match_dataset.normalised,
        dry_run=dry_run,
        client=client,
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
            client=client,
        )

    # Generate content tables
    create_dmps_content_table(
        dataset_id=dataset_id,
        dmps_norm_table_id=dt_dataset.dmp_dataset.normalised,
        dmps_content_table_id=dt_dataset.dmp_dataset.content,
        dry_run=dry_run,
        client=client,
    )
    for match in dt_dataset.match_datasets:
        create_content_table(
            dataset_id=dataset_id,
            match_norm_table_id=match.normalised,
            match_intermediate_table_id=match.match_intermediate,
            match_content_table_id=match.content,
            dry_run=dry_run,
            dry_run_id=match.name,
            client=client,
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
            client=client,
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
            client=client,
        )


def export_matches(
    *,
    dag_id: str,
    project_id: str,
    dataset_id: str,
    release_date: pendulum.Date,
    bucket_name: str,
    client: google.cloud.bigquery.Client = None,
):
    dt_dataset = DMPToolDataset(project_id, dataset_id, release_date)
    for match in dt_dataset.match_datasets:
        table_id = match.match
        destination_uri = match.destination_uri(bucket_name, dag_id)
        state = bq.bq_export_table(
            table_id=match.match, file_type="jsonl.gz", destination_uri=destination_uri, client=client
        )
        if not state:
            raise AirflowException(f"export_matches: error exporting {table_id} to {destination_uri}")


def submit_matches(
    *,
    dag_id: str,
    project_id: str,
    dataset_id: str,
    release_date: pendulum.Date,
    bucket_name: str,
    download_folder: str,
    client: google.cloud.storage.Client = None,
):
    # Download files from bucket
    prefix = make_prefix(dag_id, release_date)
    success = gcs.gcs_download_blobs(
        bucket_name=bucket_name, prefix=prefix, destination_path=download_folder, client=client
    )
    if not success:
        raise AirflowException(f"submit_matches: failed to download files from bucket {bucket_name}/{prefix}")

    # Submit files to DMP tool
    # TODO: upload with Brian's API
    # dt_dataset = DMPToolDataset(project_id, dataset_id, release_date)
    # for match in dt_dataset.match_datasets:
    #     path = match.local_file_path(download_folder)
    #     print(f"File {path} exists: {os.path.exists(path)}")
    #     print(f"Uploading to DMPTool: {path}")
