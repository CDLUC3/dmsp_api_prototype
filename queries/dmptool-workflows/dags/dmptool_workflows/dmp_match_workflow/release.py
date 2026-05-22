import pendulum
from observatory_platform.airflow.release import SnapshotRelease
from observatory_platform.airflow.workflow import CloudWorkspace


class DMPToolMatchRelease(SnapshotRelease):
    def __init__(
        self,
        *,
        dag_id: str,
        cloud_workspace: CloudWorkspace,
        run_id: str,
        snapshot_date: pendulum.DateTime,
    ):
        super().__init__(
            dag_id=dag_id,
            run_id=run_id,
            snapshot_date=snapshot_date,
        )
        self.cloud_workspace = cloud_workspace

    @staticmethod
    def from_dict(dict_: dict):
        dag_id = dict_["dag_id"]
        cloud_workspace = CloudWorkspace.from_dict(dict_["cloud_workspace"])
        run_id = dict_["run_id"]
        snapshot_date = pendulum.parse(dict_["snapshot_date"])
        return DMPToolMatchRelease(
            dag_id=dag_id,
            cloud_workspace=cloud_workspace,
            run_id=run_id,
            snapshot_date=snapshot_date,
        )

    def to_dict(self) -> dict:
        return dict(
            dag_id=self.dag_id,
            cloud_workspace=self.cloud_workspace.to_dict(),
            run_id=self.run_id,
            snapshot_date=self.snapshot_date.to_datetime_string(),
        )
