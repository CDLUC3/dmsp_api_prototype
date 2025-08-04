MODEL (
  name datacite.works_affiliations,
  dialect duckdb,
  kind VIEW,
  audits (
    number_of_rows(threshold := 1),
    unique_combination_of_columns(columns := (work_doi, affiliation_identifier, affiliation_identifier_scheme, name, scheme_uri, source), blocking := false)
  )
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

SELECT *
FROM read_parquet(@VAR('datacite_path') || '/datacite_works_affiliations_[0-9]*.parquet');