MODEL (
  name openalex.works,
  dialect duckdb,
  kind VIEW,
  audits (
    number_of_rows(threshold := CAST(@VAR('audit_openalex_works_threshold') AS INT64)),
    unique_values(columns := (id), blocking := false),
    not_empty_string(column := id, blocking := false),
    not_empty_string(column := doi, blocking := false),
    not_empty_string(column := title, blocking := false),
    not_empty_string(column := abstract, blocking := false),
    accepted_values(column := type, is_in := ('article', 'book', 'book-chapter', 'dataset', 'dissertation', 'editorial',
                                              'erratum', 'grant', 'letter', 'libguides', 'other', 'paratext', 'peer-review',
                                              'preprint', 'reference-entry', 'report', 'retraction', 'review', 'standard',
                                              'supplementary-materials'), blocking := false),
    not_empty_string(column := container_title, blocking := false),
    not_empty_string(column := volume, blocking := false),
    not_empty_string(column := issue, blocking := false),
    not_empty_string(column := page, blocking := false),
    not_empty_string(column := publisher, blocking := false),
    not_empty_string(column := publisher_location, blocking := false)
  )
);

SELECT *
FROM read_parquet(@VAR('data_path') || '/openalex_works/' || @VAR('openalex_works_release_date') || '/transform/parquets/openalex_works_[0-9]*.parquet');