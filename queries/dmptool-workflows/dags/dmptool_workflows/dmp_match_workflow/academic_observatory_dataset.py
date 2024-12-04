from functools import cached_property

import observatory_platform.google.bigquery as bq


class AcademicObservatoryDataset:
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.ror_dataset = RORDataset(project_id)
        self.openalex_dataset = OpenAlexDataset(project_id)
        self.crossref_metadata_dataset = CrossrefMetadataDataset(project_id)
        self.datacite_dataset = DataCiteDataset(project_id)


class RORDataset:
    def __init__(self, project_id: str):
        self.project_id = project_id

    @cached_property
    def ror(self):
        return bq.bq_select_latest_table(bq.bq_table_id(self.project_id, "ror", "ror"))


class OpenAlexDataset:
    def __init__(self, project_id: str):
        self.project_id = project_id

    @property
    def works(self):
        return bq.bq_table_id(self.project_id, "openalex", "works")

    @property
    def funders(self):
        return bq.bq_table_id(self.project_id, "openalex", "funders")


class CrossrefMetadataDataset:
    def __init__(self, project_id: str):
        self.project_id = project_id

    @cached_property
    def crossref_metadata(self):
        return bq.bq_select_latest_table(bq.bq_table_id(self.project_id, "crossref_metadata", "crossref_metadata"))


class DataCiteDataset:
    def __init__(self, project_id: str):
        self.project_id = project_id

    @cached_property
    def datacite(self):
        return bq.bq_select_latest_table(bq.bq_table_id(self.project_id, "datacite", "datacite"))
