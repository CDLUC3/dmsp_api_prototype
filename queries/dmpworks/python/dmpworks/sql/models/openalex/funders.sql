MODEL (
  name openalex.funders,
  dialect duckdb,
  kind VIEW,
  audits (
    number_of_rows(threshold := 1),
  )
);

SELECT *
FROM read_parquet(@VAR('openalex_funders_path') || '/openalex_funders_[0-9]*.parquet');
