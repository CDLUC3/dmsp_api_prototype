MODEL (
  name crossref.works_funders,
  dialect duckdb,
  kind VIEW,
  audits (
    number_of_rows(threshold := 1),
  )
);

SELECT *
FROM read_parquet(@VAR('data_path') || 'crossref/parquets/crossref_works_funders_[0-9]*.parquet');