import os

import observatory_platform.google.bigquery as bq
import pendulum


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
    def dmps_raw_table_id(self):
        return bq.bq_sharded_table_id(self.project_id, self.dataset_id, "dmps_raw", self.release_date)

    @property
    def normalised_table_id(self):
        return bq.bq_sharded_table_id(self.project_id, self.dataset_id, "dmps", self.release_date)

    @property
    def content_table_id(self):
        """Returns the full content table ID. For the DMPs, a new content table is created each run.

        :return: the full table id.
        """

        return bq.bq_table_id(self.project_id, self.dataset_id, "dmps_content")

    @property
    def embeddings_table_id(self):
        """Returns the full content table ID.

        :return: the full table id.
        """

        return bq.bq_table_id(self.project_id, self.dataset_id, "dmps_embeddings")


class MatchDataset:
    def __init__(self, project_id: str, dataset_id: str, name: str, release_date: pendulum.Date):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.name = name
        self.release_date = release_date

    @property
    def normalised_table_id(self):
        return bq.bq_sharded_table_id(self.project_id, self.dataset_id, self.name, self.release_date)

    @property
    def match_intermediate_table_id(self):
        return bq.bq_sharded_table_id(
            self.project_id, self.dataset_id, f"{self.name}_match_intermediate", self.release_date
        )

    @property
    def content_table_id(self):
        """Returns the full content table ID.

        :return: the full table id.
        """
        return bq.bq_table_id(self.project_id, self.dataset_id, f"{self.name}_content")

    @property
    def embeddings_table_id(self):
        """Returns the full content embeddings table ID.

        :return: the full table id.
        """
        return bq.bq_table_id(self.project_id, self.dataset_id, f"{self.name}_embeddings")

    @property
    def match_table_id(self):
        return bq.bq_sharded_table_id(self.project_id, self.dataset_id, f"{self.name}_match", self.release_date)

    def local_file_path(self, download_folder: str) -> str:
        return os.path.join(download_folder, f"{self.name}.jsonl.gz")
