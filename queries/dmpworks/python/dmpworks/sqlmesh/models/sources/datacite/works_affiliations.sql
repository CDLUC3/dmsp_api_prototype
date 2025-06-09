MODEL (
  name datacite.works_affiliations,
  dialect duckdb,
  kind VIEW,
  audits (
    number_of_rows(threshold := 1),
    unique_combination_of_columns(columns := (work_doi, affiliation_identifier, affiliation_identifier_scheme, name, scheme_uri, source))
  )
);

SELECT *
FROM read_parquet(@VAR('data_path') || 'datacite/parquets/datacite_works_affiliations_[0-9]*.parquet');