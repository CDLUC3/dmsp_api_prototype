from functools import cached_property

import observatory_platform.google.bigquery as bq
from google.cloud.bigquery import Client


class AcademicObservatoryDataset:
    def __init__(
        self,
        project_id: str,
        client: Client = None,
    ):
        self.project_id = project_id
        self.ror_dataset = RORDataset(project_id, client=client)
        self.openalex_dataset = OpenAlexDataset(project_id, client=client)
        self.crossref_metadata_dataset = CrossrefMetadataDataset(project_id, client=client)
        self.datacite_dataset = DataCiteDataset(project_id, client=client)


class RORDataset:
    def __init__(
        self,
        project_id: str,
        client: Client = None,
    ):
        self.project_id = project_id
        self.client = client

    @cached_property
    def ror(self):
        return bq.bq_select_latest_table(bq.bq_table_id(self.project_id, "ror", "ror"), client=self.client)


class OpenAlexDataset:
    def __init__(
        self,
        project_id: str,
        client: Client = None,
    ):
        self.project_id = project_id
        self.client = client

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
        client: Client = None,
    ):
        self.project_id = project_id
        self.client = client

    @cached_property
    def crossref_metadata(self):
        return bq.bq_select_latest_table(
            bq.bq_table_id(self.project_id, "crossref_metadata", "crossref_metadata"), client=self.client
        )


class DataCiteDataset:
    def __init__(
        self,
        project_id: str,
        client: Client = None,
    ):
        self.project_id = project_id
        self.client = client

    @cached_property
    def datacite(self):
        return bq.bq_select_latest_table(bq.bq_table_id(self.project_id, "datacite", "datacite"), client=self.client)
