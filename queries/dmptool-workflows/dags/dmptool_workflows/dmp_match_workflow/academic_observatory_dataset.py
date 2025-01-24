from functools import cached_property

import observatory_platform.google.bigquery as bq
from google.cloud import bigquery


class AcademicObservatoryDataset:
    def __init__(
        self,
        project_id: str,
        ror_dataset_id: str = "ror",
        openalex_dataset_id: str = "openalex",
        crossref_metadata_dataset_id: str = "crossref_metadata",
        datacite_dataset_id: str = "datacite",
        bq_client: bigquery.Client = None,
    ):
        self.project_id = project_id
        self.ror_dataset = RORDataset(project_id, ror_dataset_id, bq_client=bq_client)
        self.openalex_dataset = OpenAlexDataset(project_id, openalex_dataset_id, bq_client=bq_client)
        self.crossref_metadata_dataset = CrossrefMetadataDataset(
            project_id, crossref_metadata_dataset_id, bq_client=bq_client
        )
        self.datacite_dataset = DataCiteDataset(project_id, datacite_dataset_id, bq_client=bq_client)


class RORDataset:
    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        bq_client: bigquery.Client = None,
    ):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bq_client = bq_client

    @cached_property
    def ror(self):
        return bq.bq_select_latest_table(bq.bq_table_id(self.project_id, self.dataset_id, "ror"), client=self.bq_client)


class OpenAlexDataset:
    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        bq_client: bigquery.Client = None,
    ):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bq_client = bq_client

    @property
    def works(self):
        return bq.bq_table_id(self.project_id, self.dataset_id, "works")

    @property
    def funders(self):
        return bq.bq_table_id(self.project_id, self.dataset_id, "funders")


class CrossrefMetadataDataset:
    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        bq_client: bigquery.Client = None,
    ):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bq_client = bq_client

    @cached_property
    def crossref_metadata(self):
        return bq.bq_select_latest_table(
            bq.bq_table_id(self.project_id, self.dataset_id, "crossref_metadata"), client=self.bq_client
        )


class DataCiteDataset:
    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        bq_client: bigquery.Client = None,
    ):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bq_client = bq_client

    @cached_property
    def datacite(self):
        return bq.bq_select_latest_table(
            bq.bq_table_id(self.project_id, self.dataset_id, "datacite"), client=self.bq_client
        )
