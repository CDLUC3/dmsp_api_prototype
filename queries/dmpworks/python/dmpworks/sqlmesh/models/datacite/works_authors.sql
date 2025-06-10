MODEL (
  name datacite.works_authors,
  dialect duckdb,
  kind VIEW,
  audits (
    number_of_rows(threshold := 1),
    unique_combination_of_columns(columns := (work_doi, given_name, family_name, name, orcid), blocking := false)
  )
);

SELECT *
FROM read_parquet(@VAR('data_path') || 'datacite/parquets/datacite_works_authors_[0-9]*.parquet');