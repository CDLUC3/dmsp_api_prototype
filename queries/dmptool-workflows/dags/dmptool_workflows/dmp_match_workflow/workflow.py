from __future__ import annotations

from typing import Union

import pendulum
from airflow import DAG
from airflow.decorators import dag, task, task_group

import dmptool_workflows.dmp_match_workflow.queries as queries
import dmptool_workflows.dmp_match_workflow.tasks as tasks
from dmptool_workflows.dmp_match_workflow.academic_observatory_dataset import AcademicObservatoryDataset
from dmptool_workflows.dmp_match_workflow.dmptool_api import DMPToolAPI, get_dmptool_api_creds
from dmptool_workflows.dmp_match_workflow.dmptool_dataset import DMPToolDataset
from dmptool_workflows.dmp_match_workflow.release import DMPToolMatchRelease
from observatory_platform.airflow.airflow import on_failure_callback
from observatory_platform.airflow.tasks import check_dependencies
from observatory_platform.airflow.workflow import CloudWorkspace
from observatory_platform.dataset_api import DatasetAPI
from observatory_platform.google.gcs import gcs_blob_name_from_path


class DagParams:
    def __init__(
        self,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        bq_dataset_id: str = "dmptool",
        bq_ror_dataset_id: str = "ror",
        bq_openalex_dataset_id: str = "openalex",
        bq_crossref_metadata_dataset_id: str = "crossref_metadata",
        bq_datacite_dataset_id: str = "datacite",
        api_bq_dataset_id: str = "dataset_api",
        bq_dataset_expiration_days: int = 31,
        dmptool_api_env: str = "prd",
        dmptool_api_conn_id: str = "dmptool_api_credentials",
        vertex_ai_model_id: str = "text-multilingual-embedding-002",
        weighted_count_threshold: int = 3,
        max_matches: int = 100,
        start_date: pendulum.DateTime = pendulum.datetime(2024, 10, 1),
        schedule: str = "15 0 * * Sun",
        max_active_runs: int = 1,
        retries: int = 2,
        retry_delay: Union[int, float] = 5,
        dry_run: bool = False,
        **kwargs,
    ):
        """
        Parameters for the DMP Match Workflow DAG.

        Args:
            dag_id: the Apache Airflow DAG ID.
            cloud_workspace: the Google Cloud project settings.
            bq_dataset_id: the BigQuery dataset ID where this workflow will save data.
            bq_dataset_expiration_days: expiration days for the tables in the BigQuery dataset.
            dmptool_api_conn_id: the DMPTool API credentials Apache Airflow connection ID.
            vertex_ai_model_id: the ID of Vertex AI model that will be used to generate embeddings. Use
                text-multilingual-embedding-002 (default) for multi-language support or text-embedding-004 for English
                (as of Q4 2024). See here for available models: https://cloud.google.com/vertex-ai/generative-ai/docs/embeddings/get-text-embeddings.
            weighted_count_threshold: the threshold to pre-filter intermediate matches before running embeddings and vector search.
            max_matches: the maximum number of matches to return for each DMP.
            start_date: the Apache Airflow DAG start date. Set in default UTC timezone.
            schedule: the Apache Airflow DAG schedule. Defaults to 7am UTC every Sunday, which is approximately 12am
                Sunday in the America/Los_Angeles timezone. Using UTC (set by start_date) because Astro development
                hibernate mode is set in UTC.
            max_active_runs: the Apache Airflow DAG max active runs.
            retries: the Apache Airflow DAG retries.
            retry_delay: the Apache Airflow DAG retry delay in minutes.
            **kwargs:
        """

        self.dag_id = dag_id
        self.cloud_workspace = cloud_workspace
        self.bq_dataset_id = bq_dataset_id
        self.bq_dataset_expiration_days = bq_dataset_expiration_days
        self.bq_ror_dataset_id = bq_ror_dataset_id
        self.bq_openalex_dataset_id = bq_openalex_dataset_id
        self.bq_crossref_metadata_dataset_id = bq_crossref_metadata_dataset_id
        self.bq_datacite_dataset_id = bq_datacite_dataset_id
        self.api_bq_dataset_id = api_bq_dataset_id
        self.dmptool_api_env = dmptool_api_env
        self.dmptool_api_conn_id = dmptool_api_conn_id
        self.vertex_ai_model_id = vertex_ai_model_id
        self.weighted_count_threshold = weighted_count_threshold
        self.max_matches = max_matches
        self.start_date = start_date
        self.schedule = schedule
        self.max_active_runs = max_active_runs
        self.retries = retries
        self.retry_delay = retry_delay
        self.dry_run = dry_run


def create_dag(dag_params: DagParams) -> DAG:
    @dag(
        dag_id=dag_params.dag_id,
        schedule=dag_params.schedule,
        start_date=dag_params.start_date,
        catchup=False,
        tags=["dmps"],
        max_active_runs=dag_params.max_active_runs,
        default_args=dict(
            retries=dag_params.retries,
            retry_delay=pendulum.duration(minutes=dag_params.retry_delay),
            on_failure_callback=on_failure_callback,
        ),
    )
    def dmp_match_workflow():
        ao_project_id = dag_params.cloud_workspace.input_project_id
        dmps_project_id = dag_params.cloud_workspace.output_project_id

        @task
        def create_bq_dataset(**context) -> None:
            """Create BigQuery dataset if it doesn't exist"""

            tasks.create_bq_dataset(
                project_id=dag_params.cloud_workspace.output_project_id,
                dataset_id=dag_params.bq_dataset_id,
                location=dag_params.cloud_workspace.data_location,
                table_expiration_days=dag_params.bq_dataset_expiration_days,
            )

        @task()
        def fetch_dmps(**context) -> dict:
            """Fetch DMPs table, load as a BigQuery table and construct a release object"""

            client_id, client_secret = get_dmptool_api_creds(dag_params.dmptool_api_conn_id)
            dmptool_api = DMPToolAPI(env=dag_params.dmptool_api_env, client_id=client_id, client_secret=client_secret)
            dataset_api = DatasetAPI(
                bq_project_id=dag_params.cloud_workspace.output_project_id, bq_dataset_id=dag_params.api_bq_dataset_id
            )
            release = tasks.fetch_dmps(
                dmptool_api=dmptool_api,
                dataset_api=dataset_api,
                dag_id=context.get("dag").dag_id,
                run_id=context.get("run_id"),
                bucket_name=dag_params.cloud_workspace.download_bucket,
                project_id=dag_params.cloud_workspace.output_project_id,
                bq_dataset_id=dag_params.bq_dataset_id,
            )
            return release.to_dict()

        @task_group()
        def create_dmp_matches(release: dict, **context) -> None:
            """Create work to DMP matches. 0 retries because if something fails here it needs investigating."""

            embedding_model_id = f"{dag_params.bq_dataset_id}.embedding_model"
            vertex_ai_model_id = dag_params.vertex_ai_model_id
            weighted_count_threshold = dag_params.weighted_count_threshold
            max_matches = dag_params.max_matches
            dry_run = dag_params.dry_run

            @task(retries=0)
            def create_shared_functions(release: dict, **context):
                release = DMPToolMatchRelease.from_dict(release)
                bq_query_labels = tasks.get_bq_query_labels(release, context)

                queries.run_sql_template(
                    "shared_functions", dag_params.bq_dataset_id, dry_run, bq_query_labels=bq_query_labels
                )

            @task(retries=0)
            def create_embedding_model(release: dict, **context):
                release = DMPToolMatchRelease.from_dict(release)
                bq_query_labels = tasks.get_bq_query_labels(release, context)

                queries.create_embedding_model(
                    dataset_id=dag_params.bq_dataset_id,
                    embedding_model_id=embedding_model_id,
                    vertex_ai_model_id=vertex_ai_model_id,
                    dry_run=dry_run,
                    bq_query_labels=bq_query_labels,
                )

            @task()
            def enrich_funder_data(release: dict, **context):
                release = DMPToolMatchRelease.from_dict(release)
                bq_query_labels = tasks.get_bq_query_labels(release, context)
                dt_dataset = DMPToolDataset(dmps_project_id, dag_params.bq_dataset_id, release.snapshot_date)
                tasks.enrich_funder_data(
                    dmps_raw_table_id=dt_dataset.dmp_dataset.dmps_raw_table_id,
                    dmps_awards_table_id=dt_dataset.dmp_dataset.dmps_awards_table_id,
                )

            @task(retries=0)
            def normalise_dmps(release: dict, **context):
                # TODO: update to use new funder info

                release = DMPToolMatchRelease.from_dict(release)
                bq_query_labels = tasks.get_bq_query_labels(release, context)

                ao_dataset = AcademicObservatoryDataset(
                    ao_project_id,
                    ror_dataset_id=dag_params.bq_ror_dataset_id,
                    openalex_dataset_id=dag_params.bq_openalex_dataset_id,
                    crossref_metadata_dataset_id=dag_params.bq_crossref_metadata_dataset_id,
                    datacite_dataset_id=dag_params.bq_datacite_dataset_id,
                )
                dt_dataset = DMPToolDataset(dmps_project_id, dag_params.bq_dataset_id, release.snapshot_date)

                queries.normalise_dmps(
                    dataset_id=dag_params.bq_dataset_id,
                    ror_table_id=ao_dataset.ror_dataset.ror_table_id,
                    dmps_raw_table_id=dt_dataset.dmp_dataset.dmps_raw_table_id,
                    dmps_awards_table_id=dt_dataset.dmp_dataset.dmps_awards_table_id,
                    dmps_norm_table_id=dt_dataset.dmp_dataset.normalised_table_id,
                    dry_run=dry_run,
                    bq_query_labels=bq_query_labels,
                )

            @task(retries=0)
            def normalise_openalex(release: dict, **context):
                release = DMPToolMatchRelease.from_dict(release)
                bq_query_labels = tasks.get_bq_query_labels(release, context)

                ao_dataset = AcademicObservatoryDataset(
                    ao_project_id,
                    ror_dataset_id=dag_params.bq_ror_dataset_id,
                    openalex_dataset_id=dag_params.bq_openalex_dataset_id,
                    crossref_metadata_dataset_id=dag_params.bq_crossref_metadata_dataset_id,
                    datacite_dataset_id=dag_params.bq_datacite_dataset_id,
                )
                dt_dataset = DMPToolDataset(dmps_project_id, dag_params.bq_dataset_id, release.snapshot_date)

                queries.normalise_openalex(
                    dataset_id=dag_params.bq_dataset_id,
                    openalex_works_table_id=ao_dataset.openalex_dataset.works_table_id,
                    openalex_funders_table_id=ao_dataset.openalex_dataset.funders_table_id,
                    crossref_metadata_table_id=ao_dataset.crossref_metadata_dataset.crossref_metadata_table_id,
                    datacite_table_id=ao_dataset.datacite_dataset.datacite_table_id,
                    dmps_norm_table_id=dt_dataset.dmp_dataset.normalised_table_id,
                    openalex_norm_table_id=dt_dataset.openalex_match_dataset.normalised_table_id,
                    dry_run=dry_run,
                    bq_query_labels=bq_query_labels,
                )

            @task(retries=0)
            def normalise_crossref(release: dict, **context):
                release = DMPToolMatchRelease.from_dict(release)
                bq_query_labels = tasks.get_bq_query_labels(release, context)

                ao_dataset = AcademicObservatoryDataset(
                    ao_project_id,
                    ror_dataset_id=dag_params.bq_ror_dataset_id,
                    openalex_dataset_id=dag_params.bq_openalex_dataset_id,
                    crossref_metadata_dataset_id=dag_params.bq_crossref_metadata_dataset_id,
                    datacite_dataset_id=dag_params.bq_datacite_dataset_id,
                )
                dt_dataset = DMPToolDataset(dmps_project_id, dag_params.bq_dataset_id, release.snapshot_date)

                queries.normalise_crossref(
                    dataset_id=dag_params.bq_dataset_id,
                    crossref_metadata_table_id=ao_dataset.crossref_metadata_dataset.crossref_metadata_table_id,
                    ror_table_id=ao_dataset.ror_dataset.ror_table_id,
                    openalex_norm_table_id=dt_dataset.openalex_match_dataset.normalised_table_id,
                    dmps_norm_table_id=dt_dataset.dmp_dataset.normalised_table_id,
                    crossref_norm_table_id=dt_dataset.crossref_match_dataset.normalised_table_id,
                    dry_run=dry_run,
                    bq_query_labels=bq_query_labels,
                )

            @task(retries=0)
            def normalise_datacite(release: dict, **context):
                release = DMPToolMatchRelease.from_dict(release)
                bq_query_labels = tasks.get_bq_query_labels(release, context)

                ao_dataset = AcademicObservatoryDataset(
                    ao_project_id,
                    ror_dataset_id=dag_params.bq_ror_dataset_id,
                    openalex_dataset_id=dag_params.bq_openalex_dataset_id,
                    crossref_metadata_dataset_id=dag_params.bq_crossref_metadata_dataset_id,
                    datacite_dataset_id=dag_params.bq_datacite_dataset_id,
                )
                dt_dataset = DMPToolDataset(dmps_project_id, dag_params.bq_dataset_id, release.snapshot_date)
                queries.normalise_datacite(
                    dataset_id=dag_params.bq_dataset_id,
                    datacite_table_id=ao_dataset.datacite_dataset.datacite_table_id,
                    ror_table_id=ao_dataset.ror_dataset.ror_table_id,
                    dmps_norm_table_id=dt_dataset.dmp_dataset.normalised_table_id,
                    openalex_norm_table_id=dt_dataset.openalex_match_dataset.normalised_table_id,
                    datacite_norm_table_id=dt_dataset.datacite_match_dataset.normalised_table_id,
                    dry_run=dry_run,
                    bq_query_labels=bq_query_labels,
                )

            @task(retries=0)
            def match_intermediate(release: dict, **context):
                release = DMPToolMatchRelease.from_dict(release)
                bq_query_labels = tasks.get_bq_query_labels(release, context)

                dt_dataset = DMPToolDataset(dmps_project_id, dag_params.bq_dataset_id, release.snapshot_date)
                for match in dt_dataset.match_datasets:
                    queries.match_intermediate(
                        dataset_id=dag_params.bq_dataset_id,
                        dataset_name=match.name,
                        dmps_norm_table_id=dt_dataset.dmp_dataset.normalised_table_id,
                        match_norm_table_id=match.normalised_table_id,
                        match_intermediate_table_id=match.match_intermediate_table_id,
                        weighted_count_threshold=weighted_count_threshold,
                        max_matches=max_matches,
                        dry_run=dry_run,
                        dry_run_id=match.name,
                        bq_query_labels=bq_query_labels,
                    )

            @task(retries=0)
            def update_content_table(release: dict, **context):
                # Insert or update new content (title + abstract) in the content table
                # Deletes updated records from the embeddings table, so that new embeddings are generated for these records
                release = DMPToolMatchRelease.from_dict(release)
                bq_query_labels = tasks.get_bq_query_labels(release, context)

                dt_dataset = DMPToolDataset(dmps_project_id, dag_params.bq_dataset_id, release.snapshot_date)
                for dataset in dt_dataset.all_datasets:
                    match_intermediate_table_id = (
                        None if dataset.name == "dmps" else dataset.match_intermediate_table_id
                    )
                    queries.update_content_table(
                        dataset_id=dag_params.bq_dataset_id,
                        dataset_name=dataset.name,
                        content_table_id=dataset.content_table_id,
                        embeddings_table_id=dataset.embeddings_table_id,
                        norm_table_id=dataset.normalised_table_id,
                        match_intermediate_table_id=match_intermediate_table_id,
                        dry_run=dry_run,
                        dry_run_id=dataset.name,
                        bq_query_labels=bq_query_labels,
                    )

            @task(retries=0)
            def update_embeddings(release: dict, **context):
                # Generate embeddings for new or updated records in the content table
                release = DMPToolMatchRelease.from_dict(release)
                bq_query_labels = tasks.get_bq_query_labels(release, context)

                dt_dataset = DMPToolDataset(dmps_project_id, dag_params.bq_dataset_id, release.snapshot_date)
                for match in dt_dataset.all_datasets:
                    queries.update_embeddings(
                        dataset_id=dag_params.bq_dataset_id,
                        dataset_name=match.name,
                        content_table_id=match.content_table_id,
                        embedding_model_id=embedding_model_id,
                        embeddings_table_id=match.embeddings_table_id,
                        dry_run=dry_run,
                        dry_run_id=match.name,
                        bq_query_labels=bq_query_labels,
                    )

            @task(retries=0)
            def match_vector_search(release: dict, **context):
                release = DMPToolMatchRelease.from_dict(release)
                bq_query_labels = tasks.get_bq_query_labels(release, context)

                dt_dataset = DMPToolDataset(dmps_project_id, dag_params.bq_dataset_id, release.snapshot_date)
                for match in dt_dataset.match_datasets:
                    queries.match_vector_search(
                        dataset_id=dag_params.bq_dataset_id,
                        dataset_name=match.name,
                        match_intermediate_table_id=match.match_intermediate_table_id,
                        match_norm_table_id=match.normalised_table_id,
                        dmps_norm_table_id=dt_dataset.dmp_dataset.normalised_table_id,
                        match_embeddings_table_id=match.embeddings_table_id,
                        dmps_embeddings_table_id=dt_dataset.dmp_dataset.embeddings_table_id,
                        match_table_id=match.match_table_id,
                        dry_run=dry_run,
                        dry_run_id=match.name,
                        bq_query_labels=bq_query_labels,
                    )

            task_create_shared_functions = create_shared_functions(release)
            task_create_embedding_model = create_embedding_model(release)
            task_enrich_funder_data = enrich_funder_data(release)
            task_normalise_dmps = normalise_dmps(release)
            task_normalise_openalex = normalise_openalex(release)
            task_normalise_crossref = normalise_crossref(release)
            task_normalise_datacite = normalise_datacite(release)
            task_match_intermediate = match_intermediate(release)
            task_update_content_table = update_content_table(release)
            task_update_embeddings = update_embeddings(release)
            task_match_vector_search = match_vector_search(release)

            (
                task_create_shared_functions
                >> task_create_embedding_model
                >> task_enrich_funder_data
                >> task_normalise_dmps
                >> task_normalise_openalex
                >> task_normalise_crossref
                >> task_normalise_datacite
                >> task_match_intermediate
                >> task_update_content_table
                >> task_update_embeddings
                >> task_match_vector_search
            )

        @task
        def export_matches(release: dict, **context) -> None:
            """Export matches to Google Cloud Storage bucket"""

            release = DMPToolMatchRelease.from_dict(release)

            tasks.export_matches(
                dag_id=context.get("dag").dag_id,
                project_id=dag_params.cloud_workspace.output_project_id,
                dataset_id=dag_params.bq_dataset_id,
                release_date=release.snapshot_date,
                bucket_name=dag_params.cloud_workspace.download_bucket,
                export_folder_blob_name=gcs_blob_name_from_path(release.export_folder),
            )

        @task
        def submit_matches(release: dict, **context) -> None:
            """Send match files to DMPTool"""

            release = DMPToolMatchRelease.from_dict(release)
            client_id, client_secret = get_dmptool_api_creds(dag_params.dmptool_api_conn_id)
            dmptool_api = DMPToolAPI(env=dag_params.dmptool_api_env, client_id=client_id, client_secret=client_secret)
            tasks.submit_matches(
                dmptool_api=dmptool_api,
                bucket_name=dag_params.cloud_workspace.download_bucket,
                export_folder_blob_name=gcs_blob_name_from_path(release.export_folder),
                export_folder=release.export_folder,
            )

        @task
        def add_dataset_release(release: dict, **context):
            """Add dataset release"""

            release = DMPToolMatchRelease.from_dict(release)
            tasks.add_dataset_release(
                dag_id=context.get("dag").dag_id,
                run_id=context.get("run_id"),
                snapshot_date=release.snapshot_date,
                bq_project_id=dag_params.cloud_workspace.output_project_id,
                api_bq_dataset_id=dag_params.api_bq_dataset_id,
            )

        check_task = check_dependencies(airflow_vars=[], airflow_conns=[dag_params.dmptool_api_conn_id])
        create_bq_dataset_task = create_bq_dataset()
        xcom_release = fetch_dmps()
        create_dmp_matches_task = create_dmp_matches(xcom_release)
        export_matches_task = export_matches(xcom_release)
        submit_matches_task = submit_matches(xcom_release)
        add_dataset_release_task = add_dataset_release(xcom_release)

        (
            check_task
            >> create_bq_dataset_task
            >> xcom_release
            >> create_dmp_matches_task
            >> export_matches_task
            >> submit_matches_task
            >> add_dataset_release_task
        )

    return dmp_match_workflow()
