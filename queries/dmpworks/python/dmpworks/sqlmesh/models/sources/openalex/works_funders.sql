MODEL (
  name openalex.works_funders,
  dialect duckdb,
  kind VIEW,
  audits (
    number_of_rows(threshold := 1),
  )
);

SELECT *
FROM read_parquet(@VAR('data_path') || 'openalex_works/parquets/openalex_works_funders_[0-9]*.parquet');