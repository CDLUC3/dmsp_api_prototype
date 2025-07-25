MODEL (
  name crossref_metadata.relations,
  dialect duckdb,
  kind VIEW,
  audits (
    number_of_rows(threshold := 1),
  )
);

SELECT *
FROM read_parquet(@VAR('crossref_metadata_path') || '/crossref_works_relations_[0-9]*.parquet');