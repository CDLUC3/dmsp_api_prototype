MODEL (
  name datacite.works_funders,
  dialect duckdb,
  kind VIEW,
  audits (
    number_of_rows(threshold := 1),
    unique_combination_of_columns(columns := (work_doi, funder_identifier, funder_identifier_type, funder_name, award_number, award_uri), blocking := false)
  )
);

PRAGMA threads=CAST(@VAR('default_threads') AS INT64);

SELECT *
FROM read_parquet(@VAR('datacite_path') || '/datacite_works_funders_[0-9]*.parquet');