MODEL (
  name datacite.works,
  dialect duckdb,
  kind VIEW,
  audits (
    number_of_rows(threshold := CAST(@VAR('audit_datacite_works_threshold') AS INT64)),
    unique_values(columns := (doi), blocking := false),
    not_empty_string(column := doi, blocking := false),
    not_empty_string(column := title, blocking := false),
    not_empty_string(column := abstract, blocking := false),
    accepted_values(column := type, is_in :=('Audiovisual', 'Award', 'Book', 'BookChapter', 'Collection',
                                             'ComputationalNotebook', 'ConferencePaper', 'ConferenceProceeding', 'DataPaper',
                                             'Dataset', 'Dissertation', 'Event', 'Film', 'Image', 'Instrument', 'InteractiveResource',
                                             'Journal', 'JournalArticle', 'List of nomenclatural and taxonomic changes for the New Zealand flora.',
                                             'Model', 'Other', 'OutputManagementPlan', 'PeerReview', 'PhysicalObject',
                                             'Preprint', 'Project', 'Report', 'Service', 'Software', 'Sound', 'Standard',
                                             'StudyRegistration', 'Text', 'Workflow'), blocking := false),
    not_empty_string(column := container_title, blocking := false),
    not_empty_string(column := volume, blocking := false),
    not_empty_string(column := issue, blocking := false),
    not_empty_string(column := page, blocking := false),
    not_empty_string(column := publisher, blocking := false),
    not_empty_string(column := publisher_location, blocking := false)
  )
);

SELECT *
FROM read_parquet(@VAR('datacite_path') || '/datacite_works_[0-9]*.parquet');