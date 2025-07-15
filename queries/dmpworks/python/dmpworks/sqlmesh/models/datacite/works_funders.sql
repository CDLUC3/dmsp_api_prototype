MODEL (
  name datacite.works_funders,
  dialect duckdb,
  kind VIEW,
  audits (
    number_of_rows(threshold := 1),
    unique_combination_of_columns(columns := (work_doi, funder_identifier, funder_identifier_type, funder_name, award_number, award_uri), blocking := false)
  )
);

SELECT *
FROM read_parquet(@VAR('data_path') || '/datacite/' || @VAR('datacite_release_date') || '/transform/parquets/datacite_works_funders_[0-9]*.parquet');