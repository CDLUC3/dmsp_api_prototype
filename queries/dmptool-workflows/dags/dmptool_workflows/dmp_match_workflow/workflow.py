from __future__ import annotations

from typing import Union

import pendulum
from airflow import DAG
from airflow.decorators import dag, task
from observatory_platform.airflow.airflow import on_failure_callback
from observatory_platform.airflow.tasks import check_dependencies
from observatory_platform.airflow.workflow import CloudWorkspace

import dmptool_workflows.dmp_match_workflow.tasks as tasks
from dmptool_workflows.dmp_match_workflow.release import DMPToolMatchRelease


class DagParams:
    def __init__(
        self,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        bq_dataset_id: str = "dmptool",
        aws_cognito_conn_id: str = "dmptool_aws_cognito",
        vertex_ai_model_id: str = "text-multilingual-embedding-002",
        weighted_count_threshold: int = 3,
        max_matches: int = 100,
        start_date: pendulum.DateTime = pendulum.datetime(2024, 10, 1),
        schedule: str = "0 7 * * Sun",
        max_active_runs: int = 1,
        retries: int = 2,
        retry_delay: Union[int, float] = 5,
        **kwargs,
    ):
        """
        Parameters for the DMP Match Workflow DAG.

        Args:
            dag_id: the Apache Airflow DAG ID.
            cloud_workspace: the Google Cloud project settings.
            bq_dataset_id: the BigQuery dataset ID where this workflow will save data.
            aws_cognito_conn_id: the AWS Cognito Client Apache Airflow connection ID.
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
        self.aws_cognito_conn_id = aws_cognito_conn_id
        self.vertex_ai_model_id = vertex_ai_model_id
        self.weighted_count_threshold = weighted_count_threshold
        self.max_matches = max_matches
        self.start_date = start_date
        self.schedule = schedule
        self.max_active_runs = max_active_runs
        self.retries = retries
        self.retry_delay = retry_delay


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
        @task
        def create_bq_dataset(**context) -> None:
            """Create BigQuery dataset if it doesn't exist"""

            tasks.create_bq_dataset(
                project_id=dag_params.cloud_workspace.output_project_id,
                dataset_id=dag_params.bq_dataset_id,
                location=dag_params.cloud_workspace.data_location,
            )

        @task()
        def fetch_dmps(**context) -> dict:
            """Fetch DMPs table, load as a BigQuery table and construct a release object"""

            release_date = tasks.fetch_dmps(
                project_id=dag_params.cloud_workspace.output_project_id, bq_dataset_id=dag_params.bq_dataset_id
            )
            return DMPToolMatchRelease(
                dag_id=dag_params.dag_id,
                cloud_workspace=dag_params.cloud_workspace,
                run_id=context["run_id"],
                snapshot_date=release_date,
            ).to_dict()

        @task(retries=0)
        def create_dmp_matches(release: dict, **context) -> None:
            """Create work to DMP matches. 0 retries because if something fails here it needs investigating."""

            # TODO: perhaps break down into different tasks?
            print(dag_params.cloud_workspace.input_project_id)
            print(dag_params.cloud_workspace.output_project_id)
            release = DMPToolMatchRelease.from_dict(release)
            tasks.create_dmp_matches(
                ao_project_id=dag_params.cloud_workspace.input_project_id,
                dmps_project_id=dag_params.cloud_workspace.output_project_id,
                dataset_id=dag_params.bq_dataset_id,
                release_date=release.snapshot_date,
                vertex_ai_model_id=dag_params.vertex_ai_model_id,
                weighted_count_threshold=dag_params.weighted_count_threshold,
                max_matches=dag_params.max_matches,
            )

        @task
        def export_matches(release: dict, **context) -> None:
            """Export matches to Google Cloud Storage bucket"""

            release = DMPToolMatchRelease.from_dict(release)
            tasks.export_matches(
                dag_id=dag_params.dag_id,
                project_id=dag_params.cloud_workspace.output_project_id,
                dataset_id=dag_params.bq_dataset_id,
                release_date=release.snapshot_date,
                bucket_name=dag_params.cloud_workspace.transform_bucket,
            )

        @task
        def submit_matches(release: dict, **context) -> None:
            """Send match files to DMPTool"""

            release = DMPToolMatchRelease.from_dict(release)
            tasks.submit_matches(
                dag_id=dag_params.dag_id,
                project_id=dag_params.cloud_workspace.output_project_id,
                dataset_id=dag_params.bq_dataset_id,
                release_date=release.snapshot_date,
                bucket_name=dag_params.cloud_workspace.transform_bucket,
                download_folder=release.transform_folder,
            )

        check_task = check_dependencies(airflow_conns=[])  # dag_params.aws_cognito_conn_id
        create_bq_dataset_task = create_bq_dataset()
        xcom_release = fetch_dmps()
        create_dmp_matches_task = create_dmp_matches(xcom_release)
        export_matches_task = export_matches(xcom_release)
        submit_matches_task = submit_matches(xcom_release)

        (check_task >> create_bq_dataset_task >> xcom_release >> create_dmp_matches_task >> export_matches_task >> submit_matches_task)

    return dmp_match_workflow()
