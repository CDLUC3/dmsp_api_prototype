import json
import os
from unittest.mock import patch

import pendulum
from airflow.models import Connection
from observatory_platform.airflow.release import DATE_TIME_FORMAT
from observatory_platform.airflow.workflow import Workflow
from observatory_platform.dataset_api import DatasetAPI
from observatory_platform.files import save_jsonl_gz
from observatory_platform.google.bigquery import bq_find_schema
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment
from observatory_platform.sandbox.test_utils import bq_load_tables, SandboxTestCase, Table

from dmptool_workflows.config import project_path, TestConfig
from dmptool_workflows.dmp_match_workflow.dmptool_api import DMPToolAPI
from dmptool_workflows.dmp_match_workflow.release import DMPToolMatchRelease
from dmptool_workflows.dmp_match_workflow.tasks import DATASET_API_ENTITY_ID
from dmptool_workflows.dmp_match_workflow.workflow import create_dag, DagParams


class TestDMPToolWorkflow(SandboxTestCase):
    def setUp(self) -> None:
        self.dag_id = "dmp_match_workflow"
        self.gcp_project_id: str = TestConfig.gcp_project_id
        self.gcp_data_location: str = TestConfig.gcp_data_location

    def test_dag_structure(self):
        """Test that the DAG has the correct structure."""

        env = SandboxEnvironment()
        with env.create():
            dag_params = DagParams(
                dag_id=self.dag_id,
                cloud_workspace=env.cloud_workspace,
            )
            dag = create_dag(dag_params)
            self.assert_dag_structure(
                {
                    "check_dependencies": ["create_bq_dataset"],
                    "create_bq_dataset": ["fetch_dmps"],
                    "fetch_dmps": [
                        "create_dmp_matches.create_shared_functions",
                        "create_dmp_matches.create_embedding_model",
                        "create_dmp_matches.normalise_dmps",
                        "create_dmp_matches.normalise_openalex",
                        "create_dmp_matches.normalise_crossref",
                        "create_dmp_matches.normalise_datacite",
                        "create_dmp_matches.match_intermediate",
                        "create_dmp_matches.create_dmps_content_table",
                        "create_dmp_matches.create_content_table",
                        "create_dmp_matches.generate_embeddings",
                        "create_dmp_matches.match_vector_search",
                        "export_matches",
                        "submit_matches",
                        "add_dataset_release",
                    ],
                    "create_dmp_matches.create_shared_functions": ["create_dmp_matches.create_embedding_model"],
                    "create_dmp_matches.create_embedding_model": ["create_dmp_matches.normalise_dmps"],
                    "create_dmp_matches.normalise_dmps": ["create_dmp_matches.normalise_openalex"],
                    "create_dmp_matches.normalise_openalex": ["create_dmp_matches.normalise_crossref"],
                    "create_dmp_matches.normalise_crossref": ["create_dmp_matches.normalise_datacite"],
                    "create_dmp_matches.normalise_datacite": ["create_dmp_matches.match_intermediate"],
                    "create_dmp_matches.match_intermediate": ["create_dmp_matches.create_dmps_content_table"],
                    "create_dmp_matches.create_dmps_content_table": ["create_dmp_matches.create_content_table"],
                    "create_dmp_matches.create_content_table": ["create_dmp_matches.generate_embeddings"],
                    "create_dmp_matches.generate_embeddings": ["create_dmp_matches.match_vector_search"],
                    "create_dmp_matches.match_vector_search": ["export_matches"],
                    "export_matches": ["submit_matches"],
                    "submit_matches": ["add_dataset_release"],
                    "add_dataset_release": [],
                },
                dag,
            )

    def test_dag_load(self):
        """Test that the DAG can be loaded from a DAG bag."""

        # Test successful
        env = SandboxEnvironment(
            workflows=[
                Workflow(
                    dag_id=self.dag_id,
                    name="DMP Match Workflow",
                    class_name="dmptool_workflows.dmp_match_workflow.workflow",
                    cloud_workspace=self.fake_cloud_workspace,
                )
            ]
        )

        with env.create():
            dag_file = os.path.join(project_path(), "..", "..", "dags", "load_dags.py")
            self.assert_dag_load(self.dag_id, dag_file)

    def test_workflow(self):
        env = SandboxEnvironment(project_id=self.gcp_project_id, data_location=self.gcp_data_location)
        api_bq_dataset_id = env.add_dataset("dataset_api")
        bq_dataset_id = env.add_dataset("dmptool")
        bucket_name = env.add_bucket("test-data")
        dmp_snapshot_date = pendulum.datetime(year=2024, month=11, day=30)
        logical_date = pendulum.datetime(year=2024, month=12, day=1)

        with env.create() as t:
            # Load test data
            load_test_data(self.gcp_project_id, bq_dataset_id, bucket_name, logical_date)

            # Crate DAG
            dag_params = DagParams(
                dag_id=self.dag_id,
                cloud_workspace=env.cloud_workspace,
                bq_dataset_id=bq_dataset_id,
                bq_ror_dataset_id=bq_dataset_id,
                bq_openalex_dataset_id=bq_dataset_id,
                bq_crossref_metadata_dataset_id=bq_dataset_id,
                bq_datacite_dataset_id=bq_dataset_id,
                retries=0,
                api_bq_dataset_id=api_bq_dataset_id,
            )
            env.add_connection(Connection(conn_id=dag_params.dmptool_api_conn_id, uri=f"http://user:pass@"))
            dag = create_dag(dag_params)

            # Mock DMP downloading
            def download_dmps(
                *,
                dmptool_api: DMPToolAPI,
                dataset_api: DatasetAPI,
                dag_id: str,
                run_id: str,
                entity_id: str = DATASET_API_ENTITY_ID,
            ):
                release = DMPToolMatchRelease(
                    dag_id=dag_id,
                    run_id=run_id,
                    snapshot_date=dmp_snapshot_date,
                )
                input_file = os.path.join(
                    project_path("dmp_match_workflow", "tests", "fixtures", "data"), "dmps_raw.json"
                )
                data = load_json(input_file)
                file_path = os.path.join(
                    release.dmps_folder, f"coki-dmps_{dmp_snapshot_date.format('YYYY-MM-DD')}_1.jsonl.gz"
                )
                save_jsonl_gz(file_path, data)
                return [file_path], release

            # Run DAG
            with patch("dmptool_workflows.dmp_match_workflow.workflow.DMPToolAPI") as MockDMPToolAPI, patch(
                "dmptool_workflows.dmp_match_workflow.tasks.download_dmps", side_effect=download_dmps
            ) as mock_download_dmps:
                dagrun = dag.test(execution_date=logical_date)

                # Check that DAG ran successfully
                self.assertEqual("success", dagrun.state)

                # Assert that dmptool_api.upload_match called three times, once for OpenAlex, Crossref and DataCite
                mock_dmptool_api = MockDMPToolAPI.return_value
                self.assertEqual(3, mock_dmptool_api.upload_match.call_count)
                datasets = ["openalex", "crossref", "datacite"]
                for name in datasets:
                    mock_dmptool_api.upload_match.assert_any_call(
                        os.path.join(
                            t,
                            "data",
                            f"{self.dag_id}",
                            dagrun.run_id,
                            f"snapshot_{dmp_snapshot_date.format(DATE_TIME_FORMAT)}",
                            "export",
                            f"coki-{name}_{dmp_snapshot_date.format('YYYY-MM-DD')}_000000000000.jsonl.gz",
                        )
                    )


def load_json(file_path: str):
    with open(file_path, mode="r") as f:
        return json.load(f)


def load_test_data(project_id: str, dataset_id: str, bucket_name: str, snapshot_date: pendulum.DateTime):
    schema_path = project_path("dmp_match_workflow", "tests", "fixtures", "schema")
    data_path = project_path("dmp_match_workflow", "tests", "fixtures", "data")
    tables = [
        Table(
            "crossref_metadata",
            True,
            dataset_id,
            load_json(os.path.join(data_path, "crossref_metadata.json")),
            bq_find_schema(path=schema_path, table_name="crossref_metadata"),
        ),
        Table(
            "datacite",
            True,
            dataset_id,
            load_json(os.path.join(data_path, "datacite.json")),
            bq_find_schema(path=schema_path, table_name="datacite"),
        ),
        # It may be better to put these two OpenAlex tables into their own dataset
        Table(
            "funders",
            False,
            dataset_id,
            load_json(os.path.join(data_path, "openalex_funders.json")),
            bq_find_schema(path=schema_path, table_name="openalex_funders"),
        ),
        Table(
            "works",
            False,
            dataset_id,
            load_json(os.path.join(data_path, "openalex_works.json")),
            bq_find_schema(path=schema_path, table_name="openalex_works"),
        ),
        Table(
            "ror",
            True,
            dataset_id,
            load_json(os.path.join(data_path, "ror.json")),
            bq_find_schema(path=schema_path, table_name="ror"),
        ),
    ]

    bq_load_tables(
        project_id=project_id,
        tables=tables,
        bucket_name=bucket_name,
        snapshot_date=snapshot_date,
    )
