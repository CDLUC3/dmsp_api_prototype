MODEL (
  name crossref_metadata.works_authors,
  dialect duckdb,
  kind VIEW,
  audits (
    number_of_rows(threshold := 1),
  )
);

SELECT *
FROM read_parquet(@VAR('crossref_metadata_path') || '/crossref_works_authors_[0-9]*.parquet');