from functools import cached_property

import observatory_platform.google.bigquery as bq
from google.cloud import bigquery


class AcademicObservatoryDataset:
    def __init__(
        self,
        project_id: str,
        bq_client: bigquery.Client = None,
    ):
        self.project_id = project_id
        self.ror_dataset = RORDataset(project_id, bq_client=bq_client)
        self.openalex_dataset = OpenAlexDataset(project_id, bq_client=bq_client)
        self.crossref_metadata_dataset = CrossrefMetadataDataset(project_id, bq_client=bq_client)
        self.datacite_dataset = DataCiteDataset(project_id, bq_client=bq_client)


class RORDataset:
    def __init__(
        self,
        project_id: str,
        bq_client: bigquery.Client = None,
    ):
        self.project_id = project_id
        self.bq_client = bq_client

    @cached_property
    def ror(self):
        return bq.bq_select_latest_table(bq.bq_table_id(self.project_id, "ror", "ror"), client=self.bq_client)


class OpenAlexDataset:
    def __init__(
        self,
        project_id: str,
        bq_client: bigquery.Client = None,
    ):
        self.project_id = project_id
        self.bq_client = bq_client

    @property
    def works(self):
        return bq.bq_table_id(self.project_id, "openalex", "works")

    @property
    def funders(self):
        return bq.bq_table_id(self.project_id, "openalex", "funders")


class CrossrefMetadataDataset:
    def __init__(
        self,
        project_id: str,
        bq_client: bigquery.Client = None,
    ):
        self.project_id = project_id
        self.bq_client = bq_client

    @cached_property
    def crossref_metadata(self):
        return bq.bq_select_latest_table(
            bq.bq_table_id(self.project_id, "crossref_metadata", "crossref_metadata"), client=self.bq_client
        )


class DataCiteDataset:
    def __init__(
        self,
        project_id: str,
        bq_client: bigquery.Client = None,
    ):
        self.project_id = project_id
        self.bq_client = bq_client

    @cached_property
    def datacite(self):
        return bq.bq_select_latest_table(bq.bq_table_id(self.project_id, "datacite", "datacite"), client=self.bq_client)
