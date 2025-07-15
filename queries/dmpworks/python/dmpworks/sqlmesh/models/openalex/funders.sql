MODEL (
  name openalex.funders,
  dialect duckdb,
  kind VIEW,
  audits (
    number_of_rows(threshold := 1),
  )
);

SELECT *
FROM read_parquet(@VAR('data_path') || '/openalex_funders/' || @VAR('openalex_funders_release_date') || '/transform/parquets/openalex_funders_[0-9]*.parquet');
