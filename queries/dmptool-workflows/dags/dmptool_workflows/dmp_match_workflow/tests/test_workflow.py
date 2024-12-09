import os

from observatory_platform.airflow.workflow import Workflow
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment
from observatory_platform.sandbox.test_utils import SandboxTestCase

from dmptool_workflows.config import project_path
from dmptool_workflows.dmp_match_workflow.workflow import create_dag, DagParams


class TestDMPToolWorkflow(SandboxTestCase):
    def setUp(self) -> None:
        self.dag_id = "dmp_match_workflow"

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
                    "submit_matches": [],
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
        pass
