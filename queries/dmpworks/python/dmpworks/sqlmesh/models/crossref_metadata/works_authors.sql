MODEL (
  name crossref_metadata.works_authors,
  dialect duckdb,
  kind VIEW,
  audits (
    number_of_rows(threshold := 1),
  )
);

SELECT *
FROM read_parquet(@VAR('data_path') || 'crossref_metadata/parquets/crossref_works_authors_[0-9]*.parquet');