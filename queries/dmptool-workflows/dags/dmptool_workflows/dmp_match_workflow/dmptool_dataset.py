import os

import observatory_platform.google.bigquery as bq
import pendulum

DATE_FORMAT = "%Y-%m-%d"


class DMPToolDataset:
    def __init__(self, project_id: str, dataset_id: str, release_date: pendulum.Date):
        self.dmp_dataset = DMPDataset(project_id, dataset_id, release_date)
        self.openalex_match_dataset = MatchDataset(project_id, dataset_id, "openalex", release_date)
        self.crossref_match_dataset = MatchDataset(project_id, dataset_id, "crossref", release_date)
        self.datacite_match_dataset = MatchDataset(project_id, dataset_id, "datacite", release_date)
        self.all_datasets = [
            self.dmp_dataset,
            self.openalex_match_dataset,
            self.crossref_match_dataset,
            self.datacite_match_dataset,
        ]
        self.match_datasets = [self.openalex_match_dataset, self.crossref_match_dataset, self.datacite_match_dataset]


class DMPDataset:
    def __init__(self, project_id: str, dataset_id: str, release_date: pendulum.Date):
        self.name = "dmps"
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.release_date = release_date

    @property
    def dmps_raw(self):
        return bq.bq_sharded_table_id(self.project_id, self.dataset_id, "dmps_raw", self.release_date)

    @property
    def normalised(self):
        return bq.bq_sharded_table_id(self.project_id, self.dataset_id, "dmps", self.release_date)

    @property
    def content(self):
        return bq.bq_sharded_table_id(self.project_id, self.dataset_id, "dmps_content", self.release_date)

    @property
    def content_embeddings(self):
        return bq.bq_sharded_table_id(self.project_id, self.dataset_id, "dmps_content_embeddings", self.release_date)


class MatchDataset:
    def __init__(self, project_id: str, dataset_id: str, name: str, release_date: pendulum.Date):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.name = name
        self.release_date = release_date

    @property
    def normalised(self):
        return bq.bq_sharded_table_id(self.project_id, self.dataset_id, self.name, self.release_date)

    @property
    def match_intermediate(self):
        return bq.bq_sharded_table_id(
            self.project_id, self.dataset_id, f"{self.name}_match_intermediate", self.release_date
        )

    @property
    def content(self):
        return bq.bq_sharded_table_id(self.project_id, self.dataset_id, f"{self.name}_content", self.release_date)

    @property
    def content_embeddings(self):
        return bq.bq_sharded_table_id(
            self.project_id, self.dataset_id, f"{self.name}_content_embeddings", self.release_date
        )

    @property
    def match(self):
        return bq.bq_sharded_table_id(self.project_id, self.dataset_id, f"{self.name}_match", self.release_date)

    def destination_uri(self, bucket_name: str, dag_id: str) -> str:
        return make_destination_uri(bucket_name, dag_id, self.release_date, self.name)

    def local_file_path(self, download_folder: str) -> str:
        return os.path.join(download_folder, f"{self.name}.jsonl.gz")


def make_destination_uri(bucket_name: str, dag_id: str, release_date: pendulum.Date, source: str) -> str:
    prefix = make_prefix(dag_id, release_date)
    return f"gs://{bucket_name}/{prefix}/coki-{source}_{release_date.strftime(DATE_FORMAT)}_*.jsonl.gz"


def make_prefix(dag_id: str, release_date: pendulum.Date) -> str:
    date_str = release_date.strftime(DATE_FORMAT)
    return f"{dag_id}_{date_str}"
